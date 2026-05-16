import os
import json
import hashlib
import logging
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from queue import Empty, Queue
from typing import Any, Optional

import pandas as pd
import requests

from src.utils.config_loader import ConfigLoader
from src.utils.indicators import Indicators
from src.utils.data_provider import DataProvider
from src.utils.akshare_provider import AkshareProvider
from src.utils.tdx_provider import TdxProvider
from src.utils.tushare_provider import TushareProvider
from src.utils.mysql_provider import MysqlProvider
from src.utils.postgres_provider import PostgresProvider
from src.utils.duckdb_provider import DuckDbProvider

logger = logging.getLogger("HistorySyncService")


TABLE_INTERVAL_MAP = {
    "dat_1mins": "1min",
    "dat_5mins": "5min",
    "dat_10mins": "10min",
    "dat_15mins": "15min",
    "dat_30mins": "30min",
    "dat_60mins": "60min",
    "dat_days": "D",
    "dat_day": "D",
}

DEFAULT_SYNC_TABLES = [
    "dat_1mins",
    "dat_5mins",
    "dat_10mins",
    "dat_15mins",
    "dat_30mins",
    "dat_60mins",
    "dat_day",
]

SECRET_MASK = "********"
HISTORY_SYNC_EXISTING_KEYS_BATCH_SIZE = 200
HISTORY_SYNC_EXISTING_KEYS_BATCH_SIZE_DUCKDB = 20
HISTORY_SYNC_PROGRESS_LOG_EVERY = 50
HISTORY_SYNC_SLOW_CODE_WARN_SEC = 15.0


class HistorySyncCheckpointStore:
    # 按股票保存 DuckDB 增量同步的恢复进度，避免失败后整轮从头开始。
    def __init__(self, base_dir: str):
        self.base_dir = str(base_dir or os.path.join("reports", "history_sync"))
        os.makedirs(self.base_dir, exist_ok=True)

    def build_task_signature(self, payload: dict[str, Any]) -> str:
        codes = [str(item or "").strip().upper() for item in payload.get("codes", [])]
        normalized = {
            "provider_source": str(payload.get("provider_source", "") or "").strip().lower(),
            "write_mode": str(payload.get("write_mode", "") or "").strip().lower(),
            "direct_db_source": str(payload.get("direct_db_source", "") or "").strip().lower(),
            "start_time": str(payload.get("start_time", "") or "").strip(),
            "end_time": str(payload.get("end_time", "") or "").strip(),
            "tables": [str(item or "").strip().lower() for item in payload.get("tables", [])],
            "codes_digest": hashlib.sha1(",".join(codes).encode("utf-8")).hexdigest(),
            "session_only": bool(payload.get("session_only", True)),
        }
        text = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
        return hashlib.sha1(text.encode("utf-8")).hexdigest()

    def _file_path(self, task_signature: str) -> str:
        return os.path.join(self.base_dir, f"checkpoint_{task_signature}.json")

    def initialize(self, payload: dict[str, Any], total_codes: int) -> dict[str, Any]:
        signature = self.build_task_signature(payload)
        current = self.load(signature)
        if current:
            return current
        checkpoint = {
            "task_signature": signature,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "running",
            "completed_codes": [],
            "failed_code": "",
            "error": "",
            "summary": {
                "codes_total": int(total_codes or 0),
                "codes_completed": 0,
            },
        }
        self.save(signature, checkpoint)
        return checkpoint

    def load(self, task_signature: str) -> dict[str, Any]:
        file_path = self._file_path(task_signature)
        if not os.path.exists(file_path):
            return {}
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def save(self, task_signature: str, checkpoint: dict[str, Any]) -> None:
        checkpoint["updated_at"] = datetime.now().isoformat(timespec="seconds")
        with open(self._file_path(task_signature), "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)

    def mark_code_completed(self, task_signature: str, code: str) -> dict[str, Any]:
        checkpoint = self.load(task_signature)
        completed_codes = checkpoint.get("completed_codes", [])
        normalized_code = str(code or "").strip().upper()
        if normalized_code and normalized_code not in completed_codes:
            completed_codes.append(normalized_code)
            checkpoint["completed_codes"] = completed_codes
            checkpoint["summary"]["codes_completed"] = len(completed_codes)
            checkpoint["status"] = "running"
            self.save(task_signature, checkpoint)
        return checkpoint


@dataclass
class DuckDbWriteTask:
    # 表示某只股票某张表的一次 DuckDB 待写请求。
    code: str
    table: str
    interval: str
    df: pd.DataFrame
    source_rows: int
    existing_rows: int
    missing_rows: int
    result_future: Future = field(default_factory=Future)
    queued_at: float = field(default_factory=time.perf_counter)


class DuckDbSerialWriter:
    # 通过单独写线程串行刷 DuckDB，规避单文件并发写入冲突。
    def __init__(
        self,
        provider: DuckDbProvider,
        batch_size: int,
        max_batch_rows: int,
        max_batch_codes: int,
        max_wait_ms: int,
        queue_maxsize: int,
    ):
        self.provider = provider
        self.batch_size = max(1, int(batch_size or 1))
        self.max_batch_rows = max(1, int(max_batch_rows or 1))
        self.max_batch_codes = max(1, int(max_batch_codes or 1))
        self.max_wait_ms = max(1, int(max_wait_ms or 1))
        self.queue = Queue(maxsize=max(1, int(queue_maxsize or 1)))
        self.fatal_error: Optional[Exception] = None
        self.flush_batches = 0
        self.flushed_codes = 0
        self.queue_peak_size = 0
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._conn = None
        self._buckets: dict[tuple[str, str], list[DuckDbWriteTask]] = {}

    def start(self) -> None:
        self._conn = self.provider._connect(read_only=False)
        if self._conn is None:
            raise RuntimeError(self.provider.last_error or "DuckDB 写连接初始化失败")
        self._thread = threading.Thread(target=self._run, name="history-sync-duckdb-writer", daemon=True)
        self._thread.start()

    def submit(self, task: DuckDbWriteTask) -> None:
        if self.fatal_error is not None:
            raise RuntimeError(str(self.fatal_error))
        self.queue.put(task, block=True)
        # 记录队列峰值，便于观察生产端速度是否持续快于写线程刷盘速度。
        self.queue_peak_size = max(self.queue_peak_size, int(self.queue.qsize() or 0))

    def _bucket_key(self, task: DuckDbWriteTask) -> tuple[str, str]:
        return (str(task.table or "").strip().lower(), str(task.interval or "").strip())

    def _should_flush(self, tasks: list[DuckDbWriteTask]) -> bool:
        if not tasks:
            return False
        total_rows = sum(int(item.missing_rows or 0) for item in tasks)
        code_count = len({str(item.code or "").strip().upper() for item in tasks})
        oldest_queued_at = min(item.queued_at for item in tasks)
        wait_ms = (time.perf_counter() - oldest_queued_at) * 1000.0
        return total_rows >= self.max_batch_rows or code_count >= self.max_batch_codes or wait_ms >= self.max_wait_ms

    def _flush_bucket(self, key: tuple[str, str]) -> None:
        tasks = self._buckets.get(key, [])
        if not tasks:
            return
        _, interval = key
        frames = [item.df for item in tasks if item.df is not None and not item.df.empty]
        merged_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        written_rows = int(
            self.provider.upsert_kline_data_with_conn(
                self._conn,
                merged_df,
                interval=interval,
                batch_size=self.batch_size,
            )
            or 0
        )
        if written_rows <= 0 and str(getattr(self.provider, "last_error", "")).strip():
            raise RuntimeError(self.provider.last_error)
        self.flush_batches += 1
        self.flushed_codes += len(tasks)
        for item in tasks:
            if not item.result_future.done():
                item.result_future.set_result(
                    {
                        "code": item.code,
                        "table": item.table,
                        "written_rows": int(item.missing_rows or 0),
                    }
                )
        self._buckets[key] = []

    def _flush_due_buckets(self) -> None:
        for key, tasks in list(self._buckets.items()):
            if self._should_flush(tasks):
                self._flush_bucket(key)

    def _flush_all(self) -> None:
        for key in list(self._buckets.keys()):
            if self._buckets.get(key):
                self._flush_bucket(key)

    def _fail_pending(self, error: Exception) -> None:
        for tasks in self._buckets.values():
            for item in tasks:
                if not item.result_future.done():
                    item.result_future.set_exception(error)
        self._buckets = {}
        while True:
            try:
                item = self.queue.get_nowait()
            except Empty:
                break
            if not item.result_future.done():
                item.result_future.set_exception(error)

    def _run(self) -> None:
        try:
            while not self._stop_event.is_set() or not self.queue.empty():
                try:
                    task = self.queue.get(timeout=0.05)
                except Empty:
                    self._flush_due_buckets()
                    continue
                key = self._bucket_key(task)
                self._buckets.setdefault(key, []).append(task)
                if self._should_flush(self._buckets[key]):
                    self._flush_bucket(key)
            self._flush_all()
        except Exception as e:
            self.fatal_error = e
            self._fail_pending(e)

    def close_and_wait(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass


def normalize_history_sync_tables(tables: Any) -> list[str]:
    # 默认展示和默认保存统一使用 dat_day，但继续接受旧的 dat_days 作为兼容别名。
    raw_tables = tables if isinstance(tables, list) else list(DEFAULT_SYNC_TABLES)
    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw_tables:
        table = str(item or "").strip().lower()
        if not table:
            continue
        if table == "dat_days":
            table = "dat_day"
        if table not in TABLE_INTERVAL_MAP:
            continue
        if table in seen:
            continue
        seen.add(table)
        normalized.append(table)
    return normalized or list(DEFAULT_SYNC_TABLES)


def _chunk_list(items: list[Any], chunk_size: int) -> list[list[Any]]:
    # 按固定批次切分股票列表，避免 direct_db 去重查询退化成逐股逐表全量扫描。
    if not items:
        return []
    step = max(1, int(chunk_size or 1))
    return [items[idx:idx + step] for idx in range(0, len(items), step)]


def _deep_merge_dict(base: Any, override: Any) -> Any:
    # 运行时草稿配置只覆盖本次同步需要的字段，不修改磁盘配置。
    if not isinstance(base, dict):
        return override if override is not None else base
    if not isinstance(override, dict):
        return dict(base)
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dict(merged.get(key), value)
        else:
            merged[key] = value
    return merged


def _path_exists(payload: Any, path: str) -> bool:
    if not isinstance(payload, dict):
        return False
    cur = payload
    for key in str(path or "").split("."):
        if not isinstance(cur, dict) or key not in cur:
            return False
        cur = cur.get(key)
    return True


def _get_path_value(payload: Any, path: str, default: Any = None) -> Any:
    if not isinstance(payload, dict):
        return default
    cur = payload
    for key in str(path or "").split("."):
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur.get(key)
    return cur


def _delete_path_value(payload: Any, path: str) -> None:
    if not isinstance(payload, dict):
        return
    keys = str(path or "").split(".")
    chain = []
    cur = payload
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return
        chain.append((cur, key))
        cur = cur.get(key)
    parent, last_key = chain[-1]
    parent.pop(last_key, None)
    for parent, key in reversed(chain[:-1]):
        child = parent.get(key)
        if isinstance(child, dict) and not child:
            parent.pop(key, None)
        else:
            break


def _is_secret_mask_value(value: Any) -> bool:
    text = str(value or "").strip()
    return bool(text) and set(text) == {"*"} and len(text) >= len(SECRET_MASK)


def _cfg_get(cfg: Any, path: str, default: Any = None) -> Any:
    if isinstance(cfg, dict):
        return _get_path_value(cfg, path, default)
    if hasattr(cfg, "get"):
        return cfg.get(path, default)
    return default


def _build_runtime_sync_config(incoming_config: Optional[dict] = None) -> dict[str, Any]:
    # 未保存草稿只在本次同步内生效；掩码密钥继续回退到已生效私有配置。
    base_cfg = ConfigLoader.reload().to_dict()
    patch_cfg = json.loads(json.dumps(incoming_config if isinstance(incoming_config, dict) else {}, ensure_ascii=False))
    merged_candidate = _deep_merge_dict(base_cfg, patch_cfg)
    for path in ConfigLoader.resolve_private_override_paths(merged_candidate):
        if not _path_exists(patch_cfg, path):
            continue
        if _is_secret_mask_value(_get_path_value(patch_cfg, path, "")):
            _delete_path_value(patch_cfg, path)
    return _deep_merge_dict(base_cfg, patch_cfg)


def _bind_runtime_table_name_resolver(provider: Any, cfg: dict[str, Any], prefix: str) -> Any:
    # 数据库型 provider 需要使用草稿中的表名，避免未保存时仍然读写旧表。
    key_map = {
        "1min": f"data_provider.{prefix}_table_1min",
        "5min": f"data_provider.{prefix}_table_5min",
        "10min": f"data_provider.{prefix}_table_10min",
        "15min": f"data_provider.{prefix}_table_15min",
        "30min": f"data_provider.{prefix}_table_30min",
        "60min": f"data_provider.{prefix}_table_60min",
        "D": f"data_provider.{prefix}_table_day",
    }
    defaults = dict(getattr(provider, "_table_defaults", {}) or {})
    safe_name = getattr(provider, "_safe_table_name", None)

    def _resolve_table_name(interval: str) -> str:
        cfg_name = str(_cfg_get(cfg, key_map.get(interval, ""), "") or "").strip()
        if callable(safe_name):
            cfg_name = str(safe_name(cfg_name) or "").strip()
        if cfg_name:
            return cfg_name
        return str(defaults.get(interval, "") or "")

    provider._resolve_table_name = _resolve_table_name
    return provider


class HistoryDiffSyncService:
    def __init__(self):
        self._run_lock = threading.Lock()
        self._is_running = False
        self._stop_requested = False
        self._last_report: dict[str, Any] = {}
        self._current_report: dict[str, Any] = {}
        self._last_record: dict[str, Any] = {}
        self._records_dir = os.path.join("reports", "history_sync")
        self._worker_local = threading.local()
        self._duckdb_writer: Optional[DuckDbSerialWriter] = None
        self._duckdb_checkpoint_store: Optional[HistorySyncCheckpointStore] = None

    def _is_day_table(self, table: str) -> bool:
        return str(table or "").strip().lower() in {"dat_days", "dat_day"}

    def _set_current_report(self, report: dict[str, Any], status: str = "running") -> None:
        # 运行中快照独立保存，供状态接口实时返回，避免必须等整轮结束才看到统计结果。
        snapshot = json.loads(json.dumps(report or {}, ensure_ascii=False))
        snapshot["status"] = str(status or "running")
        self._current_report = snapshot

    def _clear_current_report(self) -> None:
        # 任务结束后清空运行中快照，避免前端把旧快照误判为当前执行态。
        self._current_report = {}

    def get_status(self) -> dict[str, Any]:
        return {
            "is_running": self._is_running,
            "stop_requested": self._stop_requested,
            "active_report": self._current_report,
            "last_report": self._last_report,
            "last_record": self._last_record,
        }

    def request_stop(self) -> dict[str, Any]:
        if not self._is_running:
            return {"status": "idle", "msg": "no running sync task"}
        self._stop_requested = True
        return {"status": "success", "msg": "stop requested"}

    def run_sync(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self._run_lock.acquire(blocking=False):
            return {"status": "busy", "msg": "sync task is already running", "report": self._last_report}
        self._is_running = True
        self._stop_requested = False
        self._clear_current_report()
        started_at = datetime.now()
        normalized_payload = json.loads(json.dumps(payload or {}, ensure_ascii=False))
        execution_meta = self._build_execution_meta(normalized_payload)
        try:
            report = self._run_sync_impl(normalized_payload)
            report["started_at"] = started_at.isoformat(timespec="seconds")
            report["finished_at"] = datetime.now().isoformat(timespec="seconds")
            self._last_report = report
            self._clear_current_report()
            result = {"status": "success", "report": report}
            record = self._persist_run_record(payload=self._build_record_payload(normalized_payload, report), result=result)
            result["record"] = record
            self._last_record = record
            return result
        except RuntimeError as e:
            msg = str(e)
            stopped = "sync stopped by user" in msg
            report = {
                "status": "stopped" if stopped else "failed",
                "error": msg,
                "started_at": started_at.isoformat(timespec="seconds"),
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "provider_source": execution_meta.get("provider_source", "default"),
                "write_mode": execution_meta.get("write_mode", "api"),
                "direct_db_source": execution_meta.get("direct_db_source", ""),
            }
            self._last_report = report
            self._clear_current_report()
            result = {"status": "stopped" if stopped else "error", "msg": msg, "report": report}
            record = self._persist_run_record(payload=self._build_record_payload(normalized_payload, report), result=result)
            result["record"] = record
            self._last_record = record
            return result
        except Exception as e:
            report = {
                "status": "failed",
                "error": str(e),
                "started_at": started_at.isoformat(timespec="seconds"),
                "finished_at": datetime.now().isoformat(timespec="seconds"),
                "provider_source": execution_meta.get("provider_source", "default"),
                "write_mode": execution_meta.get("write_mode", "api"),
                "direct_db_source": execution_meta.get("direct_db_source", ""),
            }
            self._last_report = report
            self._clear_current_report()
            result = {"status": "error", "msg": str(e), "report": report}
            record = self._persist_run_record(payload=self._build_record_payload(normalized_payload, report), result=result)
            result["record"] = record
            self._last_record = record
            return result
        finally:
            self._is_running = False
            self._stop_requested = False
            self._run_lock.release()

    def list_records(self, limit: int = 20, offset: int = 0) -> dict[str, Any]:
        self._ensure_records_dir()
        files = sorted(
            [f for f in os.listdir(self._records_dir) if f.startswith("record_") and f.endswith(".json")],
            reverse=True,
        )
        total = len(files)
        start = max(0, int(offset or 0))
        end = max(start, start + max(1, min(int(limit or 20), 200)))
        items: list[dict[str, Any]] = []
        for name in files[start:end]:
            file_path = os.path.join(self._records_dir, name)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    record = json.load(f)
                items.append(
                    {
                        "run_id": record.get("run_id"),
                        "status": record.get("status"),
                        "created_at": record.get("created_at"),
                        "summary": record.get("summary", {}),
                        "payload": record.get("payload", {}),
                        "detail_csv_path": record.get("detail_csv_path"),
                        "record_path": file_path,
                    }
                )
            except Exception:
                continue
        return {"total": total, "items": items}

    def get_record(self, run_id: str) -> Optional[dict[str, Any]]:
        rid = str(run_id or "").strip()
        if not rid:
            return None
        file_path = os.path.join(self._records_dir, f"record_{rid}.json")
        if not os.path.exists(file_path):
            return None
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                record = json.load(f)
            record["record_path"] = file_path
            return record
        except Exception:
            return None

    def _ensure_records_dir(self) -> None:
        os.makedirs(self._records_dir, exist_ok=True)

    def _build_detail_rows(self, report: dict[str, Any]) -> list[dict[str, Any]]:
        detail_rows: list[dict[str, Any]] = []
        code_reports = report.get("code_reports", []) if isinstance(report, dict) else []
        for code_row in code_reports:
            code = str((code_row or {}).get("code", "") or "")
            tables = (code_row or {}).get("tables", [])
            if not isinstance(tables, list):
                continue
            for table_row in tables:
                if not isinstance(table_row, dict):
                    continue
                detail_rows.append(
                    {
                        "code": code,
                        "table": table_row.get("table"),
                        "source_rows": int(table_row.get("source_rows", 0) or 0),
                        "existing_rows": int(table_row.get("existing_rows", 0) or 0),
                        "missing_rows": int(table_row.get("missing_rows", 0) or 0),
                        "written_rows": int(table_row.get("written_rows", 0) or 0),
                    }
                )
        return detail_rows

    def _persist_run_record(self, payload: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
        self._ensure_records_dir()
        report = result.get("report", {}) if isinstance(result, dict) else {}
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        detail_rows = self._build_detail_rows(report if isinstance(report, dict) else {})
        detail_csv_path = os.path.join(self._records_dir, f"detail_{run_id}.csv")
        detail_df = pd.DataFrame(detail_rows)
        detail_df.to_csv(detail_csv_path, index=False, encoding="utf-8-sig")
        summary = {
            "codes_total": int((report or {}).get("codes_total", 0) or 0),
            "tables": (report or {}).get("tables", []),
            "total_source_rows": int((report or {}).get("total_source_rows", 0) or 0),
            "total_existing_rows": int((report or {}).get("total_existing_rows", 0) or 0),
            "total_missing_rows": int((report or {}).get("total_missing_rows", 0) or 0),
            "total_written_rows": int((report or {}).get("total_written_rows", 0) or 0),
        }
        record = {
            "run_id": run_id,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "status": result.get("status"),
            "payload": payload,
            "msg": result.get("msg", ""),
            "summary": summary,
            "report": report,
            "detail_csv_path": detail_csv_path,
        }
        record_path = os.path.join(self._records_dir, f"record_{run_id}.json")
        with open(record_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        return {"run_id": run_id, "record_path": record_path, "detail_csv_path": detail_csv_path}

    def _build_record_payload(self, payload: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
        # 运行记录不落盘整份草稿配置，避免把临时敏感信息写入 reports。
        safe_payload = json.loads(json.dumps(payload or {}, ensure_ascii=False))
        if isinstance(safe_payload, dict) and "config" in safe_payload:
            safe_payload.pop("config", None)
            safe_payload["runtime_config_applied"] = True
        if isinstance(report, dict) and report.get("provider_source"):
            safe_payload["provider_source"] = str(report.get("provider_source") or "")
        return safe_payload

    def _build_execution_meta(self, payload: dict[str, Any]) -> dict[str, Any]:
        # 成功与失败分支统一复用同一份执行元信息，保证通知里的拉取源/写入目标不跑偏。
        cfg = _build_runtime_sync_config(payload.get("config"))
        write_mode = str(payload.get("write_mode", _cfg_get(cfg, "history_sync.write_mode", "api")) or "api").strip().lower()
        direct_db_source = ""
        if write_mode == "direct_db":
            direct_db_source = str(
                payload.get("direct_db_source", _cfg_get(cfg, "history_sync.direct_db_source", "mysql")) or "mysql"
            ).strip().lower()
        provider_source = str(
            _cfg_get(cfg, "data_provider.source", payload.get("provider_source", "default")) or "default"
        ).strip().lower()
        return {
            "provider_source": provider_source or "default",
            "write_mode": write_mode or "api",
            "direct_db_source": direct_db_source,
        }

    def _run_sync_impl(self, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _build_runtime_sync_config(payload.get("config"))
        write_mode = str(payload.get("write_mode", _cfg_get(cfg, "history_sync.write_mode", "api")) or "api").strip().lower()
        if write_mode not in {"api", "direct_db"}:
            raise RuntimeError("history_sync.write_mode must be one of: api, direct_db")
        direct_db_source = str(payload.get("direct_db_source", _cfg_get(cfg, "history_sync.direct_db_source", "mysql")) or "mysql").strip().lower()
        if direct_db_source not in {"mysql", "postgresql", "duckdb"}:
            raise RuntimeError("history_sync.direct_db_source must be one of: mysql, postgresql, duckdb")
        history_base_url = str(_cfg_get(cfg, "data_provider.default_api_url", "") or "").strip().rstrip("/")
        history_api_key = str(_cfg_get(cfg, "data_provider.default_api_key", "") or "").strip()
        if write_mode == "api":
            if not history_base_url:
                raise RuntimeError("missing data_provider.default_api_url")
            if not history_api_key:
                raise RuntimeError("missing data_provider.default_api_key")
        # 当前表单里未保存的新数据源应立即生效，因此优先使用运行时合并后的配置。
        provider_source = str(_cfg_get(cfg, "data_provider.source", payload.get("provider_source", "default")) or "default").strip().lower()

        lookback_days = int(payload.get("lookback_days", 10) or 10)
        max_codes = int(payload.get("max_codes", 10000) or 10000)
        batch_size = int(payload.get("batch_size", 500) or 500)
        requested_concurrency = int(payload.get("concurrency", _cfg_get(cfg, "history_sync.concurrency", 1)) or 1)
        effective_concurrency = self._resolve_effective_concurrency(
            requested_concurrency=requested_concurrency,
            write_mode=write_mode,
            direct_db_source=direct_db_source,
        )
        dry_run = self._as_bool(payload.get("dry_run", False), False)
        on_duplicate = str(payload.get("on_duplicate", "ignore") or "ignore")
        time_mode = str(payload.get("time_mode", _cfg_get(cfg, "history_sync.time_mode", "lookback")) or "lookback").strip().lower()
        if time_mode not in {"lookback", "custom"}:
            time_mode = "lookback"
        intraday_mode = self._as_bool(payload.get("intraday_mode", _cfg_get(cfg, "history_sync.intraday_mode", False)), False)
        session_only = self._as_bool(payload.get("session_only", _cfg_get(cfg, "history_sync.session_only", True)), True)
        start_time, end_time = self._resolve_time_range(
            payload=payload,
            cfg=cfg,
            lookback_days=lookback_days,
            time_mode=time_mode,
            intraday_mode=intraday_mode,
        )
        if start_time >= end_time:
            raise RuntimeError("start_time must be earlier than end_time")

        selected_tables = payload.get("tables")
        if not selected_tables:
            tables = list(DEFAULT_SYNC_TABLES)
        else:
            # 兼容旧请求里的 dat_days，同时把执行链路统一收敛到 dat_day。
            tables = normalize_history_sync_tables(selected_tables)
        if not tables:
            raise RuntimeError("no valid tables selected")

        codes = self._resolve_codes(payload.get("codes"), max_codes=max_codes, cfg=cfg)
        if not codes:
            raise RuntimeError("no stock codes available")

        target_db_provider = self._build_target_db_provider(write_mode=write_mode, direct_db_source=direct_db_source, cfg=cfg)
        self._ensure_target_db_ready(
            write_mode=write_mode,
            provider=target_db_provider,
            sample_code=codes[0],
        )
        use_serial_writer = self._is_duckdb_serial_writer_enabled(write_mode, direct_db_source, cfg)
        task_signature = ""
        checkpoint_skipped_codes = 0
        self._duckdb_checkpoint_store = None
        self._duckdb_writer = None
        if use_serial_writer:
            checkpoint_payload = {
                "provider_source": provider_source,
                "write_mode": write_mode,
                "direct_db_source": direct_db_source,
                "start_time": start_time.isoformat(timespec="seconds"),
                "end_time": end_time.isoformat(timespec="seconds"),
                "tables": tables,
                "codes": codes,
                "session_only": session_only,
            }
            self._duckdb_checkpoint_store = HistorySyncCheckpointStore(base_dir=self._records_dir)
            if self._as_bool(_cfg_get(cfg, "history_sync.resume_from_checkpoint", True), True):
                checkpoint = self._duckdb_checkpoint_store.initialize(checkpoint_payload, total_codes=len(codes))
                task_signature = str(checkpoint.get("task_signature", "") or "")
                completed_codes = {str(item or "").strip().upper() for item in checkpoint.get("completed_codes", [])}
                checkpoint_skipped_codes = len(completed_codes)
                if completed_codes:
                    codes = [code for code in codes if str(code or "").strip().upper() not in completed_codes]
            self._duckdb_writer = DuckDbSerialWriter(
                provider=target_db_provider,
                batch_size=batch_size,
                max_batch_rows=int(_cfg_get(cfg, "history_sync.duckdb_writer_batch_rows", 3000) or 3000),
                max_batch_codes=int(_cfg_get(cfg, "history_sync.duckdb_writer_batch_codes", 8) or 8),
                max_wait_ms=int(_cfg_get(cfg, "history_sync.duckdb_writer_wait_ms", 800) or 800),
                queue_maxsize=int(_cfg_get(cfg, "history_sync.duckdb_writer_queue_maxsize", 256) or 256),
            )
            self._duckdb_writer.start()

        summary = {
            "codes_total": len(codes),
            "tables": tables,
            "dry_run": dry_run,
            "provider_source": provider_source,
            "write_mode": write_mode,
            "direct_db_source": direct_db_source if write_mode == "direct_db" else "",
            "time_mode": time_mode,
            "session_only": session_only,
            "requested_concurrency": requested_concurrency,
            "effective_concurrency": effective_concurrency,
            "start_time": start_time.isoformat(timespec="seconds"),
            "end_time": end_time.isoformat(timespec="seconds"),
            "total_source_rows": 0,
            "total_existing_rows": 0,
            "total_missing_rows": 0,
            "total_written_rows": 0,
            "checkpoint_task_signature": task_signature,
            "checkpoint_completed_codes": 0,
            "checkpoint_skipped_codes": int(checkpoint_skipped_codes or 0),
            "writer_flush_batches": 0,
            "writer_flushed_codes": 0,
            "writer_queue_peak_size": 0,
            "code_reports": [],
        }
        logger.info(
            f"增量同步开始：拉取源={provider_source} 股票总数={len(codes)} 表={tables} "
            f"写入模式={write_mode} 并发={effective_concurrency} 预演={dry_run}"
        )
        self._set_current_report(summary, status="running")
        existing_keys_chunk_size = self._resolve_existing_keys_chunk_size(target_db_provider)
        code_chunks = _chunk_list(codes, existing_keys_chunk_size)
        total_codes = len(codes)
        processed_codes = 0
        runtime_token = f"history-sync-{time.time_ns()}"
        existing_keys_executor = (
            ThreadPoolExecutor(max_workers=1, thread_name_prefix="history-sync-existing-keys")
            if write_mode == "direct_db" and not isinstance(target_db_provider, DuckDbProvider)
            else None
        )
        current_existing_future: Optional[Future] = None
        next_existing_future: Optional[Future] = None
        try:
            if existing_keys_executor is not None and code_chunks:
                # 先把首批目标库去重查询放到后台，后续批次再做流水线预取。
                current_existing_future = existing_keys_executor.submit(
                    self._prefetch_existing_keys_for_chunk,
                    target_db_provider,
                    tables,
                    code_chunks[0],
                    start_time,
                    end_time,
                    1,
                    len(code_chunks),
                )
            for chunk_index, code_chunk in enumerate(code_chunks, start=1):
                self._check_stop_requested(context=f"before chunk {chunk_index}")
                if not code_chunk:
                    continue
                self._ensure_target_db_ready(
                    write_mode=write_mode,
                    provider=target_db_provider,
                    sample_code=code_chunk[0],
                )
                existing_keys_by_table: dict[str, dict[str, set[str]]] = {}
                if write_mode == "direct_db":
                    if current_existing_future is not None:
                        # 当前批次优先等待后台预取结果，避免同步主线程重复查询目标库。
                        existing_keys_by_table = current_existing_future.result()
                    else:
                        existing_keys_by_table = self._prefetch_existing_keys_for_chunk(
                            target_db_provider,
                            tables,
                            code_chunk,
                            start_time,
                            end_time,
                            chunk_index,
                            len(code_chunks),
                        )
                    if existing_keys_executor is not None and chunk_index < len(code_chunks):
                        next_code_chunk = code_chunks[chunk_index]
                        # 提前预取下一批 existing_keys，让目标库查询与当前批股票同步重叠执行。
                        next_existing_future = existing_keys_executor.submit(
                            self._prefetch_existing_keys_for_chunk,
                            target_db_provider,
                            tables,
                            next_code_chunk,
                            start_time,
                            end_time,
                            chunk_index + 1,
                            len(code_chunks),
                        )
                for code_result in self._iter_code_chunk_results(
                    code_chunk=code_chunk,
                    cfg=cfg,
                    provider_source=provider_source,
                    start_time=start_time,
                    end_time=end_time,
                    tables=tables,
                    session_only=session_only,
                    write_mode=write_mode,
                    direct_db_source=direct_db_source,
                    dry_run=dry_run,
                    batch_size=batch_size,
                    on_duplicate=on_duplicate,
                    history_base_url=history_base_url,
                    history_api_key=history_api_key,
                    existing_keys_by_table=existing_keys_by_table,
                    concurrency=effective_concurrency,
                    runtime_token=runtime_token,
                ):
                    processed_codes += 1
                    code = str(code_result.get("code", "") or "")
                    code_report = code_result.get("code_report", {})
                    self._append_code_report_to_summary(summary, code_report if isinstance(code_report, dict) else {})
                    code_elapsed = float(code_result.get("code_elapsed", 0.0) or 0.0)
                    summary["code_reports"].append(code_report)
                    if use_serial_writer and task_signature and self._duckdb_checkpoint_store is not None:
                        checkpoint = self._duckdb_checkpoint_store.mark_code_completed(task_signature, code)
                        summary["checkpoint_completed_codes"] = int(
                            ((checkpoint or {}).get("summary", {}) or {}).get("codes_completed", 0) or 0
                        )
                    if self._duckdb_writer is not None:
                        summary["writer_flush_batches"] = int(getattr(self._duckdb_writer, "flush_batches", 0) or 0)
                        summary["writer_flushed_codes"] = int(getattr(self._duckdb_writer, "flushed_codes", 0) or 0)
                        summary["writer_queue_peak_size"] = int(getattr(self._duckdb_writer, "queue_peak_size", 0) or 0)
                    self._set_current_report(summary, status="running")
                    # 每完成一只股票后输出一次完成进度，便于长任务时持续观察实际推进情况。
                    chunk_done = processed_codes - ((chunk_index - 1) * existing_keys_chunk_size)
                    percent = (processed_codes / total_codes * 100.0) if total_codes > 0 else 0.0
                    logger.info(
                        f"增量同步进度：已完成股票={processed_codes}/{total_codes} ({percent:.2f}%) "
                        f"当前批次={chunk_index}/{len(code_chunks)} 批次内完成={chunk_done}/{len(code_chunk)} "
                        f"当前股票={code} 股票耗时={code_elapsed:.2f}s "
                        f"本股票源数据行数={sum(int(item.get('source_rows', 0) or 0) for item in code_report['tables'])} "
                        f"本股票写入行数={sum(int(item.get('written_rows', 0) or 0) for item in code_report['tables'])}"
                    )
                    if code_elapsed >= HISTORY_SYNC_SLOW_CODE_WARN_SEC:
                        logger.warning(
                            f"增量同步慢股票告警：股票={code} 耗时={code_elapsed:.2f}s "
                            f"源数据行数={sum(int(item.get('source_rows', 0) or 0) for item in code_report['tables'])} "
                            f"缺失行数={sum(int(item.get('missing_rows', 0) or 0) for item in code_report['tables'])}"
                        )
                current_existing_future = next_existing_future
                next_existing_future = None
        finally:
            if next_existing_future is not None:
                next_existing_future.cancel()
            if existing_keys_executor is not None:
                existing_keys_executor.shutdown(wait=False, cancel_futures=True)
            if self._duckdb_writer is not None:
                self._duckdb_writer.close_and_wait()
                summary["writer_flush_batches"] = int(getattr(self._duckdb_writer, "flush_batches", 0) or 0)
                summary["writer_flushed_codes"] = int(getattr(self._duckdb_writer, "flushed_codes", 0) or 0)
                summary["writer_queue_peak_size"] = int(getattr(self._duckdb_writer, "queue_peak_size", 0) or 0)
                if self._duckdb_writer.fatal_error is not None:
                    raise RuntimeError(f"duckdb serial writer failed: {self._duckdb_writer.fatal_error}")
                self._duckdb_writer = None
        logger.info(
            f"增量同步完成：源数据总行数={summary['total_source_rows']} "
            f"已存在总行数={summary['total_existing_rows']} 缺失总行数={summary['total_missing_rows']} "
            f"写入总行数={summary['total_written_rows']} 有效并发={summary['effective_concurrency']} "
            f"writer批次={summary['writer_flush_batches']} writer股票任务={summary['writer_flushed_codes']} "
            f"writer队列峰值={summary['writer_queue_peak_size']} checkpoint跳过股票={summary['checkpoint_skipped_codes']}"
        )
        return summary

    def _resolve_existing_keys_chunk_size(self, provider: Any) -> int:
        # DuckDB 对超大 IN 查询更敏感，这里主动缩小判重批次，降低 metadata/internal error 概率。
        if isinstance(provider, DuckDbProvider):
            return HISTORY_SYNC_EXISTING_KEYS_BATCH_SIZE_DUCKDB
        return HISTORY_SYNC_EXISTING_KEYS_BATCH_SIZE

    def _is_duckdb_serial_writer_enabled(self, write_mode: str, direct_db_source: str, cfg: dict[str, Any]) -> bool:
        # 仅 DuckDB 直写模式启用串行写线程，其他写入链路保持原样。
        return (
            str(write_mode or "").strip().lower() == "direct_db"
            and str(direct_db_source or "").strip().lower() == "duckdb"
            and self._as_bool(_cfg_get(cfg, "history_sync.duckdb_writer_enabled", True), True)
        )

    def _submit_duckdb_write_task(
        self,
        code: str,
        table: str,
        df: pd.DataFrame,
        source_rows: int,
        existing_rows: int,
        missing_rows: int,
    ) -> dict[str, Any]:
        # 工作线程只负责提交缺失数据并等待写线程确认结果。
        if self._duckdb_writer is None:
            raise RuntimeError("duckdb serial writer not initialized")
        task = DuckDbWriteTask(
            code=code,
            table=table,
            interval=TABLE_INTERVAL_MAP.get(table, "1min"),
            df=df,
            source_rows=source_rows,
            existing_rows=existing_rows,
            missing_rows=missing_rows,
        )
        self._duckdb_writer.submit(task)
        return task.result_future.result(timeout=300)

    def _resolve_effective_concurrency(self, requested_concurrency: Any, write_mode: str, direct_db_source: str) -> int:
        # 并发上限做保护，避免前台误填过大值直接把本机/数据库压垮。
        try:
            normalized = max(1, int(requested_concurrency or 1))
        except Exception:
            normalized = 1
        return min(normalized, 16)

    def _build_worker_runtime(
        self,
        cfg: dict[str, Any],
        provider_source: str,
        write_mode: str,
        direct_db_source: str,
        history_api_key: str,
        runtime_token: str,
    ) -> dict[str, Any]:
        # 同一线程内复用 provider/session，避免并发模式下每只股票都重复建连。
        runtime_key = (runtime_token, provider_source, write_mode, direct_db_source)
        cached_key = getattr(self._worker_local, "history_sync_runtime_key", None)
        cached_runtime = getattr(self._worker_local, "history_sync_runtime", None)
        if cached_runtime is not None and cached_key == runtime_key:
            return cached_runtime
        old_session = cached_runtime.get("session") if isinstance(cached_runtime, dict) else None
        if old_session is not None:
            try:
                old_session.close()
            except Exception:
                pass
        runtime = {
            "source_provider": self._build_source_provider(provider_source=provider_source, cfg=cfg),
            "target_db_provider": self._build_target_db_provider(
                write_mode=write_mode,
                direct_db_source=direct_db_source,
                cfg=cfg,
            ),
            "session": requests.Session() if write_mode == "api" else None,
            "headers": {"x-api-key": history_api_key, "Content-Type": "application/json"} if write_mode == "api" else {},
        }
        self._worker_local.history_sync_runtime_key = runtime_key
        self._worker_local.history_sync_runtime = runtime
        return runtime

    def _process_code_sync(
        self,
        code: str,
        cfg: dict[str, Any],
        provider_source: str,
        start_time: datetime,
        end_time: datetime,
        tables: list[str],
        session_only: bool,
        write_mode: str,
        direct_db_source: str,
        dry_run: bool,
        batch_size: int,
        on_duplicate: str,
        history_base_url: str,
        history_api_key: str,
        existing_keys_by_table: dict[str, dict[str, set[str]]],
        runtime_token: str,
    ) -> dict[str, Any]:
        self._check_stop_requested(context=f"before code {code}")
        code_started = time.perf_counter()
        runtime = self._build_worker_runtime(
            cfg=cfg,
            provider_source=provider_source,
            write_mode=write_mode,
            direct_db_source=direct_db_source,
            history_api_key=history_api_key,
            runtime_token=runtime_token,
        )
        provider = runtime.get("source_provider")
        session = runtime.get("session")
        headers = runtime.get("headers", {})
        target_db_provider = runtime.get("target_db_provider")
        serial_duckdb = self._is_duckdb_serial_writer_enabled(write_mode, direct_db_source, cfg)
        if write_mode == "direct_db":
            self._ensure_target_db_ready(
                write_mode=write_mode,
                provider=target_db_provider,
                sample_code=code,
            )
        source_frames = self._build_source_frames(provider, code, start_time, end_time, tables, session_only=session_only)
        code_report = {"code": code, "tables": []}
        for table in tables:
            self._check_stop_requested(context=f"before table {table} code {code}")
            source_df = source_frames.get(table)
            if source_df is None or source_df.empty:
                code_report["tables"].append(
                    {
                        "table": table,
                        "source_rows": 0,
                        "existing_rows": 0,
                        "missing_rows": 0,
                        "written_rows": 0,
                    }
                )
                continue
            key_col = "trade_time" if not self._is_day_table(table) else "date"
            if write_mode == "api":
                existing_keys = self._fetch_existing_keys(
                    session=session,
                    base_url=history_base_url,
                    headers=headers,
                    table=table,
                    code=code,
                    start_time=start_time,
                    end_time=end_time,
                )
            else:
                existing_keys = existing_keys_by_table.get(table, {}).get(code, set())
            source_keys = source_df[key_col].map(lambda x: self._normalize_time_key(x, is_day=self._is_day_table(table)))
            missing_mask = ~source_keys.isin(existing_keys)
            missing_df = source_df.loc[missing_mask].copy()
            written_rows = 0
            if not dry_run and not missing_df.empty:
                if write_mode == "api":
                    rows = missing_df.to_dict("records")
                    written_rows = self._push_rows(
                        session=session,
                        base_url=history_base_url,
                        headers=headers,
                        table=table,
                        rows=rows,
                        batch_size=batch_size,
                        on_duplicate=on_duplicate,
                    )
                else:
                    upsert_df = self._build_direct_db_upsert_df(table=table, df=missing_df)
                    if serial_duckdb:
                        write_result = self._submit_duckdb_write_task(
                            code=code,
                            table=table,
                            df=upsert_df,
                            source_rows=int(len(source_df)),
                            existing_rows=int(len(existing_keys)),
                            missing_rows=int(len(missing_df)),
                        )
                        written_rows = int(write_result.get("written_rows", 0) or 0)
                    else:
                        interval = TABLE_INTERVAL_MAP.get(table, "1min")
                        written_rows = int(target_db_provider.upsert_kline_data(upsert_df, interval=interval, batch_size=batch_size) or 0)
                        if written_rows <= 0 and str(getattr(target_db_provider, "last_error", "")).strip():
                            raise RuntimeError(f"direct_db upsert failed table={table} code={code}: {target_db_provider.last_error}")
            code_report["tables"].append(
                {
                    "table": table,
                    "source_rows": int(len(source_df)),
                    "existing_rows": int(len(existing_keys)),
                    "missing_rows": int(len(missing_df)),
                    "written_rows": int(written_rows),
                }
            )
        return {
            "code": code,
            "code_report": code_report,
            "code_elapsed": time.perf_counter() - code_started,
        }

    def _iter_code_chunk_results(
        self,
        code_chunk: list[str],
        cfg: dict[str, Any],
        provider_source: str,
        start_time: datetime,
        end_time: datetime,
        tables: list[str],
        session_only: bool,
        write_mode: str,
        direct_db_source: str,
        dry_run: bool,
        batch_size: int,
        on_duplicate: str,
        history_base_url: str,
        history_api_key: str,
        existing_keys_by_table: dict[str, dict[str, set[str]]],
        concurrency: int,
        runtime_token: str,
    ):
        if concurrency <= 1 or len(code_chunk) <= 1:
            for code in code_chunk:
                yield self._process_code_sync(
                    code=code,
                    cfg=cfg,
                    provider_source=provider_source,
                    start_time=start_time,
                    end_time=end_time,
                    tables=tables,
                    session_only=session_only,
                    write_mode=write_mode,
                    direct_db_source=direct_db_source,
                    dry_run=dry_run,
                    batch_size=batch_size,
                    on_duplicate=on_duplicate,
                    history_base_url=history_base_url,
                    history_api_key=history_api_key,
                    existing_keys_by_table=existing_keys_by_table,
                    runtime_token=runtime_token,
                )
            return
        executor = ThreadPoolExecutor(
            max_workers=min(max(1, int(concurrency or 1)), len(code_chunk)),
            thread_name_prefix="history-sync-code",
        )
        pending: set[Future] = set()
        try:
            for code in code_chunk:
                pending.add(
                    executor.submit(
                        self._process_code_sync,
                        code,
                        cfg,
                        provider_source,
                        start_time,
                        end_time,
                        tables,
                        session_only,
                        write_mode,
                        direct_db_source,
                        dry_run,
                        batch_size,
                        on_duplicate,
                        history_base_url,
                        history_api_key,
                        existing_keys_by_table,
                        runtime_token,
                    )
                )
            while pending:
                self._check_stop_requested(context="waiting code workers")
                done, pending = wait(pending, timeout=0.5, return_when=FIRST_COMPLETED)
                if not done:
                    continue
                for future in done:
                    yield future.result()
        finally:
            for future in pending:
                future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)

    def _append_code_report_to_summary(self, summary: dict[str, Any], code_report: dict[str, Any]) -> None:
        # 汇总逻辑单独收敛，保证串行/并发两条执行路径统计口径完全一致。
        tables = code_report.get("tables", []) if isinstance(code_report, dict) else []
        for table_report in tables:
            if not isinstance(table_report, dict):
                continue
            summary["total_source_rows"] += int(table_report.get("source_rows", 0) or 0)
            summary["total_existing_rows"] += int(table_report.get("existing_rows", 0) or 0)
            summary["total_missing_rows"] += int(table_report.get("missing_rows", 0) or 0)
            summary["total_written_rows"] += int(table_report.get("written_rows", 0) or 0)

    def _prefetch_existing_keys_for_chunk(
        self,
        provider: Any,
        tables: list[str],
        code_chunk: list[str],
        start_time: datetime,
        end_time: datetime,
        chunk_index: int,
        total_chunks: int,
    ) -> dict[str, dict[str, set[str]]]:
        existing_keys_by_table: dict[str, dict[str, set[str]]] = {}
        batch_existing_started = time.perf_counter()
        logger.info(
            f"增量同步目标库去重查询开始：批次={chunk_index}/{total_chunks} "
            f"批次数量={len(code_chunk)} 表数={len(tables)}"
        )
        for table in tables:
            # 后台预取阶段不走停止检查，避免 worker 线程误读主线程停止态导致状态混乱。
            existing_keys_by_table[table] = self._fetch_existing_keys_from_db_batch(
                provider=provider,
                table=table,
                codes=code_chunk,
                start_time=start_time,
                end_time=end_time,
            )
        logger.info(
            f"增量同步目标库去重查询完成：批次={chunk_index}/{total_chunks} "
            f"耗时={time.perf_counter() - batch_existing_started:.2f}s"
        )
        return existing_keys_by_table

    def _build_source_provider(self, provider_source: str, cfg: dict[str, Any]):
        # 增量同步必须跟随当前配置的数据源，禁止再写死为某一个 provider。
        src = str(provider_source or "default").strip().lower() or "default"
        if src == "tushare":
            token = str(_cfg_get(cfg, "data_provider.tushare_token", "") or "").strip()
            if not token:
                raise RuntimeError("missing data_provider.tushare_token")
            provider = TushareProvider(token=token)
            provider._tushare_http_url = str(_cfg_get(cfg, "data_provider.tushare_api_url", "http://tushare.xyz") or "http://tushare.xyz").strip()
            provider.set_token(token)
            return provider
        if src == "akshare":
            return AkshareProvider()
        if src == "mysql":
            provider = MysqlProvider(
                host=_cfg_get(cfg, "data_provider.mysql_host", "127.0.0.1"),
                port=_cfg_get(cfg, "data_provider.mysql_port", 3306),
                user=_cfg_get(cfg, "data_provider.mysql_user", ""),
                password=_cfg_get(cfg, "data_provider.mysql_password", ""),
                database=_cfg_get(cfg, "data_provider.mysql_database", ""),
                charset=_cfg_get(cfg, "data_provider.mysql_charset", "utf8mb4"),
            )
            provider.page_size = max(1000, int(_cfg_get(cfg, "data_provider.mysql_query_page_size", getattr(provider, "page_size", 20000)) or getattr(provider, "page_size", 20000)))
            return _bind_runtime_table_name_resolver(provider, cfg, "mysql")
        if src == "postgresql":
            provider = PostgresProvider(
                host=_cfg_get(cfg, "data_provider.postgres_host", "127.0.0.1"),
                port=_cfg_get(cfg, "data_provider.postgres_port", 5432),
                user=_cfg_get(cfg, "data_provider.postgres_user", ""),
                password=_cfg_get(cfg, "data_provider.postgres_password", ""),
                database=_cfg_get(cfg, "data_provider.postgres_database", ""),
                schema=_cfg_get(cfg, "data_provider.postgres_schema", "public"),
            )
            provider.page_size = max(1000, int(_cfg_get(cfg, "data_provider.postgres_query_page_size", getattr(provider, "page_size", 20000)) or getattr(provider, "page_size", 20000)))
            return _bind_runtime_table_name_resolver(provider, cfg, "postgres")
        if src == "duckdb":
            provider = DuckDbProvider(db_path=_cfg_get(cfg, "data_provider.duckdb_path", ""))
            provider.page_size = max(1000, int(_cfg_get(cfg, "data_provider.duckdb_query_page_size", getattr(provider, "page_size", 20000)) or getattr(provider, "page_size", 20000)))
            return _bind_runtime_table_name_resolver(provider, cfg, "duckdb")
        if src == "tdx":
            provider = TdxProvider(
                host=_cfg_get(cfg, "data_provider.tdx_host", None),
                port=_cfg_get(cfg, "data_provider.tdx_port", None),
                tdxdir=_cfg_get(cfg, "data_provider.tdxdir", "") or _cfg_get(cfg, "data_provider.tdx_dir", ""),
            )
            provider.mootdx_market = str(_cfg_get(cfg, "data_provider.tdx_market", getattr(provider, "mootdx_market", "std")) or getattr(provider, "mootdx_market", "std")).strip() or getattr(provider, "mootdx_market", "std")
            configured_timeout = int(_cfg_get(cfg, "data_provider.tdx_timeout_sec", getattr(provider, "quote_timeout_sec", 6)) or getattr(provider, "quote_timeout_sec", 6))
            provider.quote_timeout_sec = max(1, configured_timeout)
            return provider
        return DataProvider(
            api_key=_cfg_get(cfg, "data_provider.default_api_key", ""),
            base_url=_cfg_get(cfg, "data_provider.default_api_url", ""),
        )

    def _check_stop_requested(self, context: str = "") -> None:
        if not self._stop_requested:
            return
        text = str(context or "").strip()
        raise RuntimeError(f"sync stopped by user{(' at ' + text) if text else ''}")

    def _build_target_db_provider(self, write_mode: str, direct_db_source: str, cfg: dict[str, Any]):
        if write_mode != "direct_db":
            return None
        if direct_db_source == "mysql":
            provider = MysqlProvider(
                host=_cfg_get(cfg, "data_provider.mysql_host", "127.0.0.1"),
                port=_cfg_get(cfg, "data_provider.mysql_port", 3306),
                user=_cfg_get(cfg, "data_provider.mysql_user", ""),
                password=_cfg_get(cfg, "data_provider.mysql_password", ""),
                database=_cfg_get(cfg, "data_provider.mysql_database", ""),
                charset=_cfg_get(cfg, "data_provider.mysql_charset", "utf8mb4"),
            )
            provider.page_size = max(1000, int(_cfg_get(cfg, "data_provider.mysql_query_page_size", getattr(provider, "page_size", 20000)) or getattr(provider, "page_size", 20000)))
            return _bind_runtime_table_name_resolver(provider, cfg, "mysql")
        if direct_db_source == "postgresql":
            provider = PostgresProvider(
                host=_cfg_get(cfg, "data_provider.postgres_host", "127.0.0.1"),
                port=_cfg_get(cfg, "data_provider.postgres_port", 5432),
                user=_cfg_get(cfg, "data_provider.postgres_user", ""),
                password=_cfg_get(cfg, "data_provider.postgres_password", ""),
                database=_cfg_get(cfg, "data_provider.postgres_database", ""),
                schema=_cfg_get(cfg, "data_provider.postgres_schema", "public"),
            )
            provider.page_size = max(1000, int(_cfg_get(cfg, "data_provider.postgres_query_page_size", getattr(provider, "page_size", 20000)) or getattr(provider, "page_size", 20000)))
            return _bind_runtime_table_name_resolver(provider, cfg, "postgres")
        if direct_db_source == "duckdb":
            provider = DuckDbProvider(db_path=_cfg_get(cfg, "data_provider.duckdb_path", ""))
            provider.page_size = max(1000, int(_cfg_get(cfg, "data_provider.duckdb_query_page_size", getattr(provider, "page_size", 20000)) or getattr(provider, "page_size", 20000)))
            return _bind_runtime_table_name_resolver(provider, cfg, "duckdb")
        raise RuntimeError("unsupported direct_db_source")

    def _ensure_target_db_ready(self, write_mode: str, provider: Any, sample_code: str) -> None:
        if write_mode != "direct_db":
            return
        if provider is None:
            raise RuntimeError("direct_db provider not initialized")
        if not hasattr(provider, "check_connectivity"):
            return
        ok, msg = provider.check_connectivity(sample_code)
        if not ok:
            raise RuntimeError(f"direct_db precheck failed: {msg}")

    def _extract_time_keys_from_df(self, df: pd.DataFrame, is_day: bool) -> set[str]:
        if df is None or df.empty:
            return set()
        out: set[str] = set()
        if "dt" in df.columns:
            series = pd.to_datetime(df["dt"], errors="coerce")
            for x in series.dropna().tolist():
                key = self._normalize_time_key(x, is_day=is_day)
                if key:
                    out.add(key)
            return out
        if is_day and "date" in df.columns:
            for x in df["date"].tolist():
                key = self._normalize_time_key(x, is_day=True)
                if key:
                    out.add(key)
            return out
        if (not is_day) and "trade_time" in df.columns:
            for x in df["trade_time"].tolist():
                key = self._normalize_time_key(x, is_day=False)
                if key:
                    out.add(key)
        return out

    def _resolve_table_time_range(self, table: str, start_time: datetime, end_time: datetime) -> tuple[datetime, datetime]:
        if not self._is_day_table(table):
            return start_time, end_time
        start_day = pd.to_datetime(start_time, errors="coerce")
        end_day = pd.to_datetime(end_time, errors="coerce")
        if pd.isna(start_day) or pd.isna(end_day):
            return start_time, end_time
        start_dt = start_day.to_pydatetime().replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = end_day.to_pydatetime().replace(hour=23, minute=59, second=59, microsecond=0)
        return start_dt, end_dt

    def _fetch_existing_keys_from_db(
        self,
        provider: Any,
        table: str,
        code: str,
        start_time: datetime,
        end_time: datetime,
    ) -> set[str]:
        if provider is None:
            return set()
        interval = TABLE_INTERVAL_MAP.get(table, "1min")
        query_start, query_end = self._resolve_table_time_range(table, start_time, end_time)
        try:
            if hasattr(provider, "fetch_kline_data_strict"):
                df = provider.fetch_kline_data_strict(code, query_start, query_end, interval=interval)
            else:
                df = provider.fetch_kline_data(code, query_start, query_end, interval=interval)
        except Exception as e:
            raise RuntimeError(f"query direct_db existing rows failed table={table} code={code}: {e}")
        provider_err = str(getattr(provider, "last_error", "") or "").strip()
        if provider_err:
            raise RuntimeError(f"query direct_db existing rows failed table={table} code={code}: {provider_err}")
        return self._extract_time_keys_from_df(df, is_day=self._is_day_table(table))

    def _build_provider_code_variant_map(self, provider: Any, codes: list[str]) -> tuple[list[str], dict[str, str]]:
        # 目标库中的 code 可能存在带后缀/不带后缀混用，批量查询时统一展开别名并回映射到源 code。
        query_codes: list[str] = []
        reverse_map: dict[str, str] = {}
        seen: set[str] = set()
        variant_builder = getattr(provider, "_code_variants", None)
        for code in codes:
            raw_variants = variant_builder(code) if callable(variant_builder) else [code]
            for item in raw_variants:
                variant = str(item or "").strip().upper()
                if not variant:
                    continue
                reverse_map[variant] = code
                if variant in seen:
                    continue
                seen.add(variant)
                query_codes.append(variant)
        return query_codes, reverse_map

    def _resolve_provider_table_name(self, provider: Any, table: str) -> str:
        # direct_db provider 可能在运行时绑定了草稿表名，这里统一走 provider 自己的解析口径。
        interval = TABLE_INTERVAL_MAP.get(table, "1min")
        resolver = getattr(provider, "_resolve_table_name", None)
        if callable(resolver):
            resolved = str(resolver(interval) or "").strip()
            if resolved:
                return resolved
        return str(table or "").strip()

    def _map_existing_key_rows(
        self,
        rows: list[Any],
        reverse_map: dict[str, str],
        is_day: bool,
        codes: list[str],
    ) -> dict[str, set[str]]:
        # 批量 SQL 返回的原始行统一折叠成 {标准code -> 已有时间key集合}，供缺失判定直接复用。
        result: dict[str, set[str]] = {code: set() for code in codes}
        for row in rows or []:
            row_code = ""
            row_time = None
            if isinstance(row, dict):
                row_code = str(row.get("code", "") or "").strip().upper()
                row_time = row.get("trade_time")
            elif isinstance(row, (list, tuple)) and len(row) >= 2:
                row_code = str(row[0] or "").strip().upper()
                row_time = row[1]
            else:
                continue
            owner = reverse_map.get(row_code)
            if not owner:
                continue
            key = self._normalize_time_key(row_time, is_day=is_day)
            if key:
                result.setdefault(owner, set()).add(key)
        return result

    def _fetch_existing_keys_from_duckdb_batch(
        self,
        provider: DuckDbProvider,
        table: str,
        codes: list[str],
        start_time: datetime,
        end_time: datetime,
    ) -> dict[str, set[str]]:
        result = {code: set() for code in codes}
        query_codes, reverse_map = self._build_provider_code_variant_map(provider, codes)
        if not query_codes:
            return result
        conn = provider._connect(read_only=True)
        if conn is None:
            raise RuntimeError(provider.last_error or "DuckDB 连接失败")
        try:
            parse_expr = provider._trade_time_parse_expr()
            placeholders = ", ".join(["?"] * len(query_codes))
            sql = (
                f"SELECT code, trade_time "
                f"FROM {provider._quoted_table(table)} "
                f"WHERE code IN ({placeholders}) "
                f"AND {parse_expr} >= CAST(? AS DATE) "
                f"AND {parse_expr} <= CAST(? AS DATE)"
            )
            params = list(query_codes) + [
                provider._query_date_text(start_time),
                provider._query_date_text(end_time),
            ]
            rows = conn.execute(sql, params).fetchall()
            return self._map_existing_key_rows(rows, reverse_map, self._is_day_table(table), codes)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _fetch_existing_keys_from_duckdb_batch_safe(
        self,
        provider: DuckDbProvider,
        table: str,
        codes: list[str],
        start_time: datetime,
        end_time: datetime,
    ) -> dict[str, set[str]]:
        # DuckDB 在超大批量 IN 查询下偶发 internal error，这里自动二分拆小批次继续执行。
        try:
            return self._fetch_existing_keys_from_duckdb_batch(provider, table, codes, start_time, end_time)
        except Exception as e:
            if len(codes) <= 1:
                # 单股批量 SQL 仍失败时，最后回退到旧的逐股查询链路，尽量让整轮同步继续跑完。
                fallback_result: dict[str, set[str]] = {}
                for code in codes:
                    logger.warning(
                        f"DuckDB 判重最终回退到逐股查询：table={table} code={code} reason={e}"
                    )
                    fallback_result[code] = self._fetch_existing_keys_from_db(
                        provider=provider,
                        table=table,
                        code=code,
                        start_time=start_time,
                        end_time=end_time,
                    )
                return fallback_result
            split_at = max(1, len(codes) // 2)
            left_codes = codes[:split_at]
            right_codes = codes[split_at:]
            logger.warning(
                f"DuckDB 批量判重降级重试：table={table} codes={len(codes)} "
                f"left={len(left_codes)} right={len(right_codes)} reason={e}"
            )
            merged: dict[str, set[str]] = {}
            merged.update(self._fetch_existing_keys_from_duckdb_batch_safe(provider, table, left_codes, start_time, end_time))
            merged.update(self._fetch_existing_keys_from_duckdb_batch_safe(provider, table, right_codes, start_time, end_time))
            return merged

    def _fetch_existing_keys_from_mysql_batch(
        self,
        provider: MysqlProvider,
        table: str,
        codes: list[str],
        start_time: datetime,
        end_time: datetime,
    ) -> dict[str, set[str]]:
        result = {code: set() for code in codes}
        query_codes, reverse_map = self._build_provider_code_variant_map(provider, codes)
        if not query_codes:
            return result
        conn = provider._acquire_connection()
        if conn is None:
            raise RuntimeError(provider.last_error or "MySQL 连接失败")
        broken = False
        try:
            placeholders = ", ".join(["%s"] * len(query_codes))
            sql = (
                f"SELECT code, trade_time "
                f"FROM `{table}` "
                f"WHERE code IN ({placeholders}) "
                f"AND trade_time >= %s AND trade_time <= %s"
            )
            with conn.cursor() as cursor:
                cursor.execute(sql, tuple(query_codes) + (start_time, end_time))
                rows = cursor.fetchall() or []
            return self._map_existing_key_rows(rows, reverse_map, self._is_day_table(table), codes)
        except Exception:
            broken = True
            raise
        finally:
            provider._release_connection(conn, broken=broken)

    def _fetch_existing_keys_from_postgres_batch(
        self,
        provider: PostgresProvider,
        table: str,
        codes: list[str],
        start_time: datetime,
        end_time: datetime,
    ) -> dict[str, set[str]]:
        result = {code: set() for code in codes}
        query_codes, reverse_map = self._build_provider_code_variant_map(provider, codes)
        if not query_codes:
            return result
        conn = provider._acquire_connection()
        if conn is None:
            raise RuntimeError(provider.last_error or "PostgreSQL 连接失败")
        broken = False
        try:
            placeholders = ", ".join(["%s"] * len(query_codes))
            sql = (
                f"SELECT code, trade_time "
                f"FROM {provider._qualified_table(table)} "
                f"WHERE code IN ({placeholders}) "
                f"AND trade_time >= %s AND trade_time <= %s"
            )
            with conn.cursor() as cursor:
                cursor.execute(sql, tuple(query_codes) + (start_time, end_time))
                rows = cursor.fetchall() or []
            return self._map_existing_key_rows(rows, reverse_map, self._is_day_table(table), codes)
        except Exception:
            broken = True
            raise
        finally:
            provider._release_connection(conn, broken=broken)

    def _fetch_existing_keys_from_db_batch(
        self,
        provider: Any,
        table: str,
        codes: list[str],
        start_time: datetime,
        end_time: datetime,
    ) -> dict[str, set[str]]:
        result = {code: set() for code in codes}
        if provider is None or not codes:
            return result
        query_start, query_end = self._resolve_table_time_range(table, start_time, end_time)
        table_name = self._resolve_provider_table_name(provider, table)
        try:
            if isinstance(provider, DuckDbProvider):
                return self._fetch_existing_keys_from_duckdb_batch_safe(provider, table_name, codes, query_start, query_end)
            if isinstance(provider, MysqlProvider):
                return self._fetch_existing_keys_from_mysql_batch(provider, table_name, codes, query_start, query_end)
            if isinstance(provider, PostgresProvider):
                return self._fetch_existing_keys_from_postgres_batch(provider, table_name, codes, query_start, query_end)
        except Exception as e:
            raise RuntimeError(f"query direct_db existing rows failed table={table} codes={len(codes)}: {e}")
        # 兜底回退到旧的逐股查询逻辑，保证未知 provider 仍可继续执行。
        for code in codes:
            result[code] = self._fetch_existing_keys_from_db(provider, table, code, start_time, end_time)
        return result

    def _build_direct_db_upsert_df(self, table: str, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        out = df.copy()
        if self._is_day_table(table):
            if "trade_time" not in out.columns and "date" in out.columns:
                out["trade_time"] = pd.to_datetime(out["date"], errors="coerce")
        return out

    def _build_write_preview(self, table: str, df: pd.DataFrame) -> dict[str, Any]:
        if df is None or df.empty:
            return {"rows": 0}
        key_col = "date" if self._is_day_table(table) else "trade_time"
        if key_col not in df.columns:
            return {"rows": int(len(df))}
        work = df.copy()
        if self._is_day_table(table):
            keys = pd.to_datetime(work[key_col], errors="coerce")
            keys = keys.dropna().dt.strftime("%Y-%m-%d")
        else:
            keys = pd.to_datetime(work[key_col], errors="coerce")
            keys = keys.dropna().dt.strftime("%Y-%m-%d %H:%M:%S")
        if keys.empty:
            return {"rows": int(len(df))}
        return {
            "rows": int(len(df)),
            "from": str(keys.iloc[0]),
            "to": str(keys.iloc[-1]),
        }

    def _parse_datetime(self, value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        text = str(value).strip()
        if not text:
            return None
        text = text.replace("Z", "")
        try:
            return datetime.fromisoformat(text)
        except Exception:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt)
            except Exception:
                continue
        raise RuntimeError(f"invalid datetime: {value}")

    def _as_bool(self, value: Any, default: bool) -> bool:
        if value is None:
            return bool(default)
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
        return bool(default)

    def _resolve_time_range(
        self,
        payload: dict[str, Any],
        cfg: Any,
        lookback_days: int,
        time_mode: str,
        intraday_mode: bool,
    ) -> tuple[datetime, datetime]:
        start_time = self._parse_datetime(payload.get("start_time"))
        end_time = self._parse_datetime(payload.get("end_time"))
        if start_time is not None or end_time is not None:
            end_time = end_time or datetime.now()
            start_time = start_time or (end_time - timedelta(days=lookback_days))
            return start_time, end_time
        if time_mode == "custom":
            custom_start = payload.get("custom_start_time", _cfg_get(cfg, "history_sync.custom_start_time", None))
            custom_end = payload.get("custom_end_time", _cfg_get(cfg, "history_sync.custom_end_time", None))
            start_time = self._parse_datetime(custom_start)
            end_time = self._parse_datetime(custom_end)
            if start_time is None or end_time is None:
                raise RuntimeError("history_sync custom mode requires custom_start_time and custom_end_time")
            return start_time, end_time
        if intraday_mode:
            return self._default_intraday_window()
        end_time = datetime.now()
        start_time = end_time - timedelta(days=lookback_days)
        return start_time, end_time

    def _default_intraday_window(self) -> tuple[datetime, datetime]:
        now = datetime.now()
        start_today = now.replace(hour=9, minute=30, second=0, microsecond=0)
        end_today = now.replace(hour=15, minute=0, second=0, microsecond=0)
        if now < start_today:
            prev = now - timedelta(days=1)
            start_prev = prev.replace(hour=9, minute=30, second=0, microsecond=0)
            end_prev = prev.replace(hour=15, minute=0, second=0, microsecond=0)
            return start_prev, end_prev
        if now >= end_today:
            return start_today, end_today
        return start_today, now

    def _normalize_code(self, code: str) -> str:
        c = str(code or "").strip().upper()
        if not c:
            return c
        if c.isdigit() and len(c) < 6:
            c = c.zfill(6)
        if c.startswith("SH") and len(c) == 8 and c[2:].isdigit():
            return f"{c[2:]}.SH"
        if c.startswith("SZ") and len(c) == 8 and c[2:].isdigit():
            return f"{c[2:]}.SZ"
        if "." in c:
            return c
        if len(c) == 6 and c.isdigit():
            return f"{c}.SH" if c.startswith("6") else f"{c}.SZ"
        return c

    def _normalize_time_key(self, value: Any, is_day: bool) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d" if is_day else "%Y-%m-%d %H:%M:%S")
        text = str(value).strip()
        if not text:
            return None
        if is_day:
            try:
                return datetime.fromisoformat(text.replace("Z", "").replace("T", " ")).strftime("%Y-%m-%d")
            except Exception:
                return text[:10]
        try:
            return datetime.fromisoformat(text.replace("Z", "").replace("T", " ")).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            if len(text) >= 19:
                return text[:19].replace("T", " ")
            return text.replace("T", " ")

    def _to_float(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        try:
            out = float(value)
        except Exception:
            return None
        if pd.isna(out):
            return None
        if out == float("inf") or out == float("-inf"):
            return None
        return out

    def _sanitize_rows_for_post(self, table: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        is_day = self._is_day_table(table)
        time_key = "date" if is_day else "trade_time"
        required = ["code", time_key, "open", "high", "low", "close", "vol", "amount"]
        sanitized: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            code = str(row.get("code", "")).strip().upper()
            if not code:
                continue
            cleaned: dict[str, Any] = {"code": code}
            normalized_time = self._normalize_time_key(row.get(time_key), is_day=is_day)
            if not normalized_time:
                continue
            cleaned[time_key] = normalized_time
            for col in ("open", "high", "low", "close", "vol", "amount"):
                if col in row:
                    cleaned[col] = self._to_float(row.get(col))
            cleaned["vol"] = 0.0 if cleaned.get("vol") is None else cleaned.get("vol")
            cleaned["amount"] = 0.0 if cleaned.get("amount") is None else cleaned.get("amount")
            if any(cleaned.get(k) is None for k in required):
                continue
            sanitized.append(cleaned)
        return sanitized

    def _resolve_api_table_candidates(self, table: str) -> list[str]:
        if self._is_day_table(table):
            return ["dat_day"]
        return [table]

    def _build_daily_rows_for_api_table(self, api_table: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if api_table == "dat_days":
            return rows
        out: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            date_text = self._normalize_time_key(row.get("date"), is_day=True)
            if not date_text:
                continue
            trade_time = self._normalize_time_key(f"{date_text} 00:00:00", is_day=False)
            if not trade_time:
                continue
            mapped = {
                "code": row.get("code"),
                "trade_time": trade_time,
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "vol": row.get("vol"),
                "amount": row.get("amount"),
            }
            out.append(mapped)
        return out

    def _resolve_codes(self, payload_codes: Any, max_codes: int, cfg: Optional[dict[str, Any]] = None) -> list[str]:
        out: list[str] = []
        if isinstance(payload_codes, list):
            out.extend([self._normalize_code(x) for x in payload_codes if str(x).strip()])
        if not out:
            file_path = os.path.join("data", "stock_list.csv")
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
                    if "code" in df.columns:
                        out.extend([self._normalize_code(x) for x in df["code"].tolist()])
                    elif len(df.columns) > 0:
                        out.extend([self._normalize_code(x) for x in df.iloc[:, 0].tolist()])
                except Exception:
                    pass
        if not out:
            targets = _cfg_get(cfg or ConfigLoader.reload().to_dict(), "targets", [])
            if isinstance(targets, list):
                out.extend([self._normalize_code(x) for x in targets if str(x).strip()])
        dedup = []
        seen = set()
        for c in out:
            if not c or c in seen:
                continue
            seen.add(c)
            dedup.append(c)
            if len(dedup) >= max_codes:
                break
        return dedup

    def _fetch_daily_frame(self, provider: Any, code: str, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        # 不同数据源的日线接口名称不完全一致，这里做一层兼容适配。
        if hasattr(provider, "fetch_daily_data"):
            return provider.fetch_daily_data(code, start_time, end_time)
        if hasattr(provider, "fetch_kline_data"):
            return provider.fetch_kline_data(code, start_time, end_time, interval="D")
        raise RuntimeError(f"source provider does not support daily fetch: {provider.__class__.__name__}")

    def _build_daily_frame_from_minute(self, minute_df: pd.DataFrame, code: str) -> pd.DataFrame:
        # 当本轮已经拉到 1 分钟数据时，优先直接聚合日线，避免额外再打一轮源端日线请求。
        if minute_df is None or minute_df.empty:
            return pd.DataFrame()
        work = minute_df.copy()
        if "dt" not in work.columns:
            return pd.DataFrame()
        work["dt"] = pd.to_datetime(work["dt"], errors="coerce")
        work = work.dropna(subset=["dt"]).sort_values("dt").drop_duplicates(subset=["dt"]).reset_index(drop=True)
        if work.empty:
            return pd.DataFrame()
        daily_df = Indicators.resample(work.copy(), "D")
        if daily_df is None or daily_df.empty:
            return pd.DataFrame()
        daily_df["code"] = code
        return daily_df.reset_index(drop=True)

    def _build_source_frames(
        self,
        provider: Any,
        code: str,
        start_time: datetime,
        end_time: datetime,
        tables: list[str],
        session_only: bool = True,
    ) -> dict[str, pd.DataFrame]:
        frames: dict[str, pd.DataFrame] = {}
        minute_tables = [t for t in tables if not self._is_day_table(t)]
        source_by_interval: dict[str, pd.DataFrame] = {}
        self._check_stop_requested(context=f"build source start code {code}")
        if minute_tables:
            base_df = provider.fetch_minute_data(code, start_time, end_time)
            self._check_stop_requested(context=f"after minute fetch code {code}")
            if base_df is not None and not base_df.empty:
                df = base_df.copy()
                if "dt" not in df.columns and "trade_time" in df.columns:
                    df = df.rename(columns={"trade_time": "dt"})
                required = ["dt", "open", "high", "low", "close", "vol", "amount"]
                if not any(col not in df.columns for col in required):
                    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
                    df = df.dropna(subset=["dt"]).sort_values("dt").drop_duplicates(subset=["dt"])
                    df["open"] = pd.to_numeric(df["open"], errors="coerce")
                    df["high"] = pd.to_numeric(df["high"], errors="coerce")
                    df["low"] = pd.to_numeric(df["low"], errors="coerce")
                    df["close"] = pd.to_numeric(df["close"], errors="coerce")
                    df["vol"] = pd.to_numeric(df["vol"], errors="coerce")
                    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
                    df = df.dropna(subset=["open", "high", "low", "close"])
                    if session_only:
                        df = self._filter_session_minutes(df)
                    df["code"] = code
                    if not df.empty:
                        source_by_interval["1min"] = df
                        needed_intervals = {TABLE_INTERVAL_MAP[t] for t in minute_tables}
                        for interval in needed_intervals:
                            if interval == "1min":
                                continue
                            source_by_interval[interval] = Indicators.resample(df.copy(), interval)
                            source_by_interval[interval]["code"] = code
                        if any(self._is_day_table(table_name) for table_name in tables):
                            # 默认分钟+日线一起同步时，直接复用本轮分钟数据聚合日线，减少一次源端抓取。
                            source_by_interval["D"] = self._build_daily_frame_from_minute(df, code)
        for table in tables:
            self._check_stop_requested(context=f"build source table {table} code {code}")
            table_start, table_end = self._resolve_table_time_range(table, start_time, end_time)
            if self._is_day_table(table):
                day_df = source_by_interval.get("D")
                if day_df is None or day_df.empty:
                    day_df = self._fetch_daily_frame(provider, code, table_start, table_end)
                    self._check_stop_requested(context=f"after daily fetch code {code}")
                if day_df is None or day_df.empty:
                    frames[table] = pd.DataFrame()
                    continue
                day_df["dt"] = pd.to_datetime(day_df["dt"], errors="coerce")
                day_df = day_df.dropna(subset=["dt"])
                day_df = day_df[(day_df["dt"] >= table_start) & (day_df["dt"] <= table_end)]
                if day_df.empty:
                    frames[table] = pd.DataFrame()
                    continue
                day_df = day_df.sort_values("dt").drop_duplicates(subset=["dt"]).reset_index(drop=True)
                day_df["date"] = day_df["dt"].dt.strftime("%Y-%m-%d")
                day_df["pre_close"] = day_df["close"].shift(1).fillna(day_df["close"])
                day_df["change"] = (day_df["close"] - day_df["pre_close"]).fillna(0.0)
                day_df["pct_chg"] = (day_df["change"] / day_df["pre_close"] * 100.0).replace([float("inf"), float("-inf")], 0.0).fillna(0.0)
                use_cols = [
                    "code",
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "vol",
                    "amount",
                    "pre_close",
                    "change",
                    "pct_chg",
                ]
                frames[table] = day_df[use_cols].copy().reset_index(drop=True)
                continue
            interval = TABLE_INTERVAL_MAP[table]
            interval_df = source_by_interval.get(interval)
            if interval_df is None or interval_df.empty:
                frames[table] = pd.DataFrame()
                continue
            table_df = interval_df.copy()
            if "dt" not in table_df.columns:
                frames[table] = pd.DataFrame()
                continue
            table_df["dt"] = pd.to_datetime(table_df["dt"], errors="coerce")
            table_df = table_df.dropna(subset=["dt"])
            table_df = table_df[(table_df["dt"] >= start_time) & (table_df["dt"] <= end_time)]
            if table_df.empty:
                frames[table] = pd.DataFrame()
                continue
            table_df = table_df.sort_values("dt").drop_duplicates(subset=["dt"]).reset_index(drop=True)
            table_df["date"] = table_df["dt"].dt.strftime("%Y-%m-%d")
            table_df["pre_close"] = table_df["close"].shift(1)
            table_df["change"] = table_df["close"] - table_df["pre_close"]
            table_df["pct_chg"] = table_df["change"] / table_df["pre_close"] * 100.0
            table_df["pre_close"] = table_df["pre_close"].fillna(table_df["close"])
            table_df["change"] = table_df["change"].fillna(0.0)
            table_df["pct_chg"] = table_df["pct_chg"].replace([pd.NA, pd.NaT], 0.0).fillna(0.0)
            for col in ("open", "high", "low", "close", "vol", "amount", "pre_close", "change", "pct_chg"):
                table_df[col] = pd.to_numeric(table_df[col], errors="coerce")
            table_df = table_df.dropna(subset=["open", "high", "low", "close"])
            table_df["vol"] = table_df["vol"].fillna(0.0)
            table_df["amount"] = table_df["amount"].fillna(0.0)
            table_df["pre_close"] = table_df["pre_close"].fillna(table_df["close"])
            table_df["change"] = table_df["change"].fillna(0.0)
            table_df["pct_chg"] = table_df["pct_chg"].replace([float("inf"), float("-inf")], 0.0).fillna(0.0)
            if self._is_day_table(table):
                use_cols = [
                    "code",
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "vol",
                    "amount",
                    "pre_close",
                    "change",
                    "pct_chg",
                ]
                out_df = table_df[use_cols].copy()
            else:
                table_df["trade_time"] = table_df["dt"].dt.strftime("%Y-%m-%d %H:%M:%S")
                use_cols = [
                    "code",
                    "trade_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "vol",
                    "amount",
                    "date",
                    "pre_close",
                    "change",
                    "pct_chg",
                ]
                out_df = table_df[use_cols].copy()
            frames[table] = out_df.reset_index(drop=True)
        return frames

    def _filter_session_minutes(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty or "dt" not in df.columns:
            return pd.DataFrame() if df is None else df
        work = df.copy()
        work["dt"] = pd.to_datetime(work["dt"], errors="coerce")
        work = work.dropna(subset=["dt"])
        minutes = work["dt"].dt.hour * 60 + work["dt"].dt.minute
        mask = (minutes >= 9 * 60 + 30) & (minutes <= 15 * 60)
        work = work.loc[mask].copy()
        return work.reset_index(drop=True)

    def _fetch_existing_keys(
        self,
        session: requests.Session,
        base_url: str,
        headers: dict[str, str],
        table: str,
        code: str,
        start_time: datetime,
        end_time: datetime,
    ) -> set[str]:
        offset = 0
        limit = 10000
        result: set[str] = set()
        last_error = ""
        api_tables = self._resolve_api_table_candidates(table)
        for api_table in api_tables:
            path = f"{base_url}/tables/{api_table}/rows"
            if self._is_day_table(table):
                if api_table == "dat_days":
                    query_plans = [
                        (
                            "date",
                            "date",
                            [
                                f"code:eq:{code}",
                                f"date:gte:{start_time.strftime('%Y-%m-%d')}",
                                f"date:lte:{end_time.strftime('%Y-%m-%d')}",
                            ],
                        ),
                        (
                            "trade_time",
                            "trade_time",
                            [
                                f"code:eq:{code}",
                                f"trade_time:gte:{start_time.strftime('%Y-%m-%d 00:00:00')}",
                                f"trade_time:lte:{end_time.strftime('%Y-%m-%d 23:59:59')}",
                            ],
                        ),
                    ]
                else:
                    query_plans = [
                        (
                            "trade_time",
                            "trade_time",
                            [
                                f"code:eq:{code}",
                                f"trade_time:gte:{start_time.strftime('%Y-%m-%d 00:00:00')}",
                                f"trade_time:lte:{end_time.strftime('%Y-%m-%d 23:59:59')}",
                            ],
                        ),
                    ]
            else:
                query_plans = [
                    (
                        "trade_time",
                        "trade_time",
                        [
                            f"code:eq:{code}",
                            f"trade_time:gte:{start_time.strftime('%Y-%m-%d %H:%M:%S')}",
                            f"trade_time:lte:{end_time.strftime('%Y-%m-%d %H:%M:%S')}",
                        ],
                    ),
                ]
            for key_col, order_by, filters in query_plans:
                result.clear()
                offset = 0
                ok = True
                while True:
                    self._check_stop_requested(context=f"query existing table {table} code {code}")
                    params = {
                        "limit": limit,
                        "offset": offset,
                        "order_by": order_by,
                        "order_dir": "asc",
                        "filter": filters,
                    }
                    resp = session.get(path, headers=headers, params=params, timeout=45)
                    if resp.status_code != 200:
                        ok = False
                        last_error = f"table={api_table} status={resp.status_code} detail={resp.text[:200]}"
                        break
                    payload = resp.json()
                    rows = payload.get("rows") if isinstance(payload, dict) else payload
                    if not isinstance(rows, list) or len(rows) == 0:
                        break
                    for row in rows:
                        if isinstance(row, dict) and row.get(key_col) is not None:
                            normalized_key = self._normalize_time_key(row.get(key_col), is_day=self._is_day_table(table))
                            if normalized_key:
                                result.add(normalized_key)
                    if len(rows) < limit:
                        break
                    offset += limit
                if ok:
                    return result
        raise RuntimeError(f"query existing rows failed table={table} code={code} {last_error}")

    def _push_rows(
        self,
        session: requests.Session,
        base_url: str,
        headers: dict[str, str],
        table: str,
        rows: list[dict[str, Any]],
        batch_size: int,
        on_duplicate: str,
    ) -> int:
        if not rows:
            return 0
        written = 0
        api_tables = self._resolve_api_table_candidates(table)
        for i in range(0, len(rows), batch_size):
            self._check_stop_requested(context=f"push rows table {table}")
            batch = self._sanitize_rows_for_post(table=table, rows=rows[i:i + batch_size])
            if not batch:
                continue
            inserted = False
            last_error = ""
            for api_table in api_tables:
                path = f"{base_url}/tables/{api_table}/rows"
                post_rows = self._build_daily_rows_for_api_table(api_table, batch) if self._is_day_table(table) else batch
                if not post_rows:
                    continue
                payload = {"on_duplicate": on_duplicate, "rows": post_rows}
                resp = session.post(path, headers=headers, json=payload, timeout=90)
                if resp.status_code != 200:
                    last_error = f"table={api_table} status={resp.status_code} detail={resp.text[:200]}"
                    continue
                data = resp.json()
                rowcount = data.get("rowcount") if isinstance(data, dict) else None
                if isinstance(rowcount, int):
                    written += rowcount
                else:
                    written += len(post_rows)
                inserted = True
                break
            if not inserted:
                raise RuntimeError(f"insert rows failed table={table} {last_error}")
        return written
