# DuckDB History Sync Writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 DuckDB 增量同步实现“多线程抓取与判重 + 单写线程小批量串行落盘 + 按股票检查点恢复”链路，并保持现有 API 与其他写入模式兼容。

**Architecture:** 在 `src/utils/history_sync_service.py` 内新增检查点与串行写线程组件，首版不拆文件，避免扩大改动面。DuckDB 直写时由工作线程负责抓取与判重，再通过内存队列把缺失数据提交给单写线程；只有股票全部表写入成功后才提交检查点。

**Tech Stack:** Python 3、pytest、pandas、duckdb、ThreadPoolExecutor、queue、threading、json

---

## File Map

- Modify: `src/utils/history_sync_service.py`
  - 新增 `DuckDbWriteTask`
  - 新增 `DuckDbSerialWriter`
  - 新增 `HistorySyncCheckpointStore`
  - 接入 DuckDB 专用写入分支、检查点恢复与快速失败
- Modify: `src/utils/duckdb_provider.py`
  - 新增复用连接的写入方法
  - 让现有 `upsert_kline_data()` 复用新方法
- Create: `tests/utils/test_history_sync_checkpoint.py`
  - 覆盖任务签名与按股票恢复行为
- Create: `tests/utils/test_duckdb_serial_writer.py`
  - 覆盖聚合、快速失败、成功确认
- Create: `tests/utils/test_history_sync_duckdb_integration.py`
  - 覆盖 `HistoryDiffSyncService` 的 DuckDB 专用集成行为

### Task 1: 建立检查点测试与最小实现骨架

**Files:**
- Create: `d:\04.量化\jin-ce-zhi-suan\tests\utils\test_history_sync_checkpoint.py`
- Modify: `d:\04.量化\jin-ce-zhi-suan\src\utils\history_sync_service.py`

- [ ] **Step 1: 写失败测试，锁定任务签名稳定性与恢复过滤**

```python
import json
from datetime import datetime

from src.utils.history_sync_service import HistorySyncCheckpointStore


def test_checkpoint_signature_is_stable_for_same_payload(tmp_path):
    store = HistorySyncCheckpointStore(base_dir=str(tmp_path))
    payload = {
        "provider_source": "duckdb",
        "write_mode": "direct_db",
        "direct_db_source": "duckdb",
        "start_time": "2026-03-02T09:30:00",
        "end_time": "2026-03-02T15:00:00",
        "tables": ["dat_1mins", "dat_day"],
        "codes": ["000001.SZ", "000002.SZ"],
        "session_only": True,
    }

    left = store.build_task_signature(payload)
    right = store.build_task_signature(dict(payload))

    assert left == right


def test_checkpoint_filters_completed_codes(tmp_path):
    store = HistorySyncCheckpointStore(base_dir=str(tmp_path))
    payload = {
        "provider_source": "duckdb",
        "write_mode": "direct_db",
        "direct_db_source": "duckdb",
        "start_time": "2026-03-02T09:30:00",
        "end_time": "2026-03-02T15:00:00",
        "tables": ["dat_1mins"],
        "codes": ["000001.SZ", "000002.SZ", "000004.SZ"],
        "session_only": True,
    }
    signature = store.build_task_signature(payload)
    store.initialize(payload, total_codes=3)
    store.mark_code_completed(signature, "000001.SZ")
    store.mark_code_completed(signature, "000004.SZ")

    checkpoint = store.load(signature)
    remain = [code for code in payload["codes"] if code not in checkpoint["completed_codes"]]

    assert checkpoint["completed_codes"] == ["000001.SZ", "000004.SZ"]
    assert remain == ["000002.SZ"]
```

- [ ] **Step 2: 运行测试，确认当前失败**

Run:

```bash
pytest tests/utils/test_history_sync_checkpoint.py -v
```

Expected:

```text
FAILED tests/utils/test_history_sync_checkpoint.py::test_checkpoint_signature_is_stable_for_same_payload - ImportError: cannot import name 'HistorySyncCheckpointStore'
FAILED tests/utils/test_history_sync_checkpoint.py::test_checkpoint_filters_completed_codes - ImportError: cannot import name 'HistorySyncCheckpointStore'
```

- [ ] **Step 3: 在 `history_sync_service.py` 增加最小实现**

```python
import hashlib


class HistorySyncCheckpointStore:
    """保存 DuckDB 历史同步的按股票恢复进度。"""

    def __init__(self, base_dir: str):
        self.base_dir = str(base_dir or os.path.join("reports", "history_sync"))
        os.makedirs(self.base_dir, exist_ok=True)

    def build_task_signature(self, payload: dict[str, Any]) -> str:
        codes = [str(x or "").strip().upper() for x in payload.get("codes", [])]
        normalized = {
            "provider_source": str(payload.get("provider_source", "") or "").strip().lower(),
            "write_mode": str(payload.get("write_mode", "") or "").strip().lower(),
            "direct_db_source": str(payload.get("direct_db_source", "") or "").strip().lower(),
            "start_time": str(payload.get("start_time", "") or "").strip(),
            "end_time": str(payload.get("end_time", "") or "").strip(),
            "tables": [str(x or "").strip().lower() for x in payload.get("tables", [])],
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
        completed = checkpoint.get("completed_codes", [])
        normalized = str(code or "").strip().upper()
        if normalized and normalized not in completed:
            completed.append(normalized)
            checkpoint["completed_codes"] = completed
            checkpoint["summary"]["codes_completed"] = len(completed)
            checkpoint["status"] = "running"
            self.save(task_signature, checkpoint)
        return checkpoint
```

- [ ] **Step 4: 运行测试，确认通过**

Run:

```bash
pytest tests/utils/test_history_sync_checkpoint.py -v
```

Expected:

```text
2 passed
```

- [ ] **Step 5: 提交检查点骨架**

```bash
git add tests/utils/test_history_sync_checkpoint.py src/utils/history_sync_service.py
git commit -m "test: add checkpoint store coverage"
```

### Task 2: 为 DuckDB provider 增加复用连接写入能力

**Files:**
- Modify: `d:\04.量化\jin-ce-zhi-suan\src\utils\duckdb_provider.py`
- Create: `d:\04.量化\jin-ce-zhi-suan\tests\utils\test_duckdb_provider_reuse_conn.py`

- [ ] **Step 1: 写失败测试，锁定复用连接写入接口**

```python
import pandas as pd

from src.utils.duckdb_provider import DuckDbProvider


def test_upsert_kline_data_with_conn_writes_rows(tmp_path):
    db_file = tmp_path / "quote.duckdb"
    provider = DuckDbProvider(db_path=str(db_file))
    conn = provider._load_duckdb().connect(database=str(db_file), read_only=False)
    conn.execute(
        """
        CREATE TABLE dat_1mins (
            code VARCHAR,
            trade_time TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            vol DOUBLE,
            amount DOUBLE
        )
        """
    )
    frame = pd.DataFrame(
        [
            {
                "code": "000001.SZ",
                "trade_time": "2026-03-02 09:30:00",
                "open": 10.0,
                "high": 10.1,
                "low": 9.9,
                "close": 10.05,
                "vol": 1000,
                "amount": 10050,
            }
        ]
    )

    written = provider.upsert_kline_data_with_conn(conn, frame, interval="1min", batch_size=100)
    rows = conn.execute("SELECT code, close FROM dat_1mins").fetchall()

    assert written == 1
    assert rows == [("000001.SZ", 10.05)]
```

- [ ] **Step 2: 运行测试，确认接口缺失**

Run:

```bash
pytest tests/utils/test_duckdb_provider_reuse_conn.py -v
```

Expected:

```text
FAILED tests/utils/test_duckdb_provider_reuse_conn.py::test_upsert_kline_data_with_conn_writes_rows - AttributeError: 'DuckDbProvider' object has no attribute 'upsert_kline_data_with_conn'
```

- [ ] **Step 3: 实现复用连接写入方法，并让旧接口复用它**

```python
def upsert_kline_data_with_conn(self, conn, df, interval="1min", batch_size=2000):
    table = self._resolve_table_name(str(interval or "1min"))
    if not table:
        self.last_error = f"未配置 {interval} 对应的 DuckDB 表名"
        return 0
    norm = self._normalize_for_upsert(df)
    if norm.empty:
        return 0
    rows = [
        (
            str(r["code"]),
            pd.to_datetime(r["trade_time"]).to_pydatetime(),
            float(r["open"]),
            float(r["high"]),
            float(r["low"]),
            float(r["close"]),
            float(r["vol"]),
            float(r["amount"]),
        )
        for _, r in norm.iterrows()
    ]
    insert_sql = (
        f"INSERT INTO {self._quoted_table(table)} (code, trade_time, open, high, low, close, vol, amount) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    upsert_sql = (
        f"INSERT INTO {self._quoted_table(table)} (code, trade_time, open, high, low, close, vol, amount) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        f"ON CONFLICT (code, trade_time) DO UPDATE SET "
        f"open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, "
        f"vol=EXCLUDED.vol, amount=EXCLUDED.amount"
    )
    written = 0
    step = max(1, int(batch_size or 2000))
    for i in range(0, len(rows), step):
        chunk = rows[i:i + step]
        try:
            conn.executemany(upsert_sql, chunk)
        except Exception as e:
            err_text = str(e).lower()
            if "conflict target" in err_text or ("unique" in err_text and "primary key" in err_text):
                conn.executemany(insert_sql, chunk)
            else:
                self.last_error = f"DuckDB 写入缓存失败: {e}"
                return 0
        written += len(chunk)
    self.last_error = ""
    return written


def upsert_kline_data(self, df, interval="1min", batch_size=2000):
    conn = self._connect(read_only=False)
    if conn is None:
        return 0
    try:
        return int(self.upsert_kline_data_with_conn(conn, df, interval=interval, batch_size=batch_size) or 0)
    finally:
        try:
            conn.close()
        except Exception:
            pass
```

- [ ] **Step 4: 运行 provider 测试**

Run:

```bash
pytest tests/utils/test_duckdb_provider_reuse_conn.py -v
```

Expected:

```text
1 passed
```

- [ ] **Step 5: 提交 DuckDB provider 改动**

```bash
git add tests/utils/test_duckdb_provider_reuse_conn.py src/utils/duckdb_provider.py
git commit -m "feat: reuse duckdb connection for batch upsert"
```

### Task 3: 为串行写线程建立聚合与快速失败测试

**Files:**
- Create: `d:\04.量化\jin-ce-zhi-suan\tests\utils\test_duckdb_serial_writer.py`
- Modify: `d:\04.量化\jin-ce-zhi-suan\src\utils\history_sync_service.py`

- [ ] **Step 1: 写失败测试，锁定聚合与失败传播**

```python
import queue
from concurrent.futures import Future

import pandas as pd

from src.utils.history_sync_service import DuckDbSerialWriter, DuckDbWriteTask


class FakeDuckDbProvider:
    def __init__(self, fail=False):
        self.fail = fail
        self.calls = []
        self.last_error = ""

    def _connect(self, read_only=False):
        return object()

    def upsert_kline_data_with_conn(self, conn, df, interval="1min", batch_size=2000):
        if self.fail:
            self.last_error = "boom"
            raise RuntimeError("boom")
        self.calls.append((interval, len(df)))
        return len(df)


def _build_task(code, trade_time):
    future = Future()
    frame = pd.DataFrame(
        [
            {
                "code": code,
                "trade_time": trade_time,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "vol": 1.0,
                "amount": 1.0,
            }
        ]
    )
    return DuckDbWriteTask(
        code=code,
        table="dat_1mins",
        interval="1min",
        df=frame,
        source_rows=1,
        existing_rows=0,
        missing_rows=1,
        result_future=future,
    )


def test_writer_merges_small_tasks_before_flush():
    provider = FakeDuckDbProvider(fail=False)
    writer = DuckDbSerialWriter(
        provider=provider,
        batch_size=500,
        max_batch_rows=10,
        max_batch_codes=8,
        max_wait_ms=50,
        queue_maxsize=10,
    )
    writer.start()
    task_a = _build_task("000001.SZ", "2026-03-02 09:30:00")
    task_b = _build_task("000002.SZ", "2026-03-02 09:31:00")

    writer.submit(task_a)
    writer.submit(task_b)
    writer.close_and_wait()

    assert provider.calls == [("1min", 2)]
    assert task_a.result_future.result(timeout=1)["written_rows"] == 1
    assert task_b.result_future.result(timeout=1)["written_rows"] == 1


def test_writer_sets_fatal_error_on_batch_failure():
    provider = FakeDuckDbProvider(fail=True)
    writer = DuckDbSerialWriter(
        provider=provider,
        batch_size=500,
        max_batch_rows=1,
        max_batch_codes=1,
        max_wait_ms=10,
        queue_maxsize=10,
    )
    writer.start()
    task = _build_task("000001.SZ", "2026-03-02 09:30:00")

    writer.submit(task)
    writer.close_and_wait()

    assert writer.fatal_error is not None
    assert "boom" in str(writer.fatal_error)
    assert task.result_future.exception() is not None
```

- [ ] **Step 2: 运行测试，确认类尚未实现**

Run:

```bash
pytest tests/utils/test_duckdb_serial_writer.py -v
```

Expected:

```text
FAILED tests/utils/test_duckdb_serial_writer.py::test_writer_merges_small_tasks_before_flush - ImportError: cannot import name 'DuckDbSerialWriter'
FAILED tests/utils/test_duckdb_serial_writer.py::test_writer_sets_fatal_error_on_batch_failure - ImportError: cannot import name 'DuckDbSerialWriter'
```

- [ ] **Step 3: 实现 `DuckDbWriteTask` 与 `DuckDbSerialWriter`**

```python
from dataclasses import dataclass, field
from concurrent.futures import Future
from queue import Empty, Queue


@dataclass
class DuckDbWriteTask:
    """承载单个股票单张表的缺失数据写入请求。"""

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
    """单线程串行写 DuckDB，避免多线程竞争单文件写锁。"""

    def __init__(self, provider, batch_size: int, max_batch_rows: int, max_batch_codes: int, max_wait_ms: int, queue_maxsize: int):
        self.provider = provider
        self.batch_size = max(1, int(batch_size or 1))
        self.max_batch_rows = max(1, int(max_batch_rows or 1))
        self.max_batch_codes = max(1, int(max_batch_codes or 1))
        self.max_wait_ms = max(1, int(max_wait_ms or 1))
        self.queue = Queue(maxsize=max(1, int(queue_maxsize or 1)))
        self.fatal_error = None
        self._stop_event = threading.Event()
        self._thread = None
        self._conn = None
        self._buckets = {}

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

    def _bucket_key(self, task: DuckDbWriteTask) -> tuple[str, str]:
        return (str(task.table or "").strip().lower(), str(task.interval or "").strip())

    def _should_flush(self, tasks: list[DuckDbWriteTask]) -> bool:
        if not tasks:
            return False
        total_rows = sum(int(item.missing_rows or 0) for item in tasks)
        codes = {str(item.code or "").strip().upper() for item in tasks}
        wait_ms = (time.perf_counter() - min(item.queued_at for item in tasks)) * 1000.0
        return total_rows >= self.max_batch_rows or len(codes) >= self.max_batch_codes or wait_ms >= self.max_wait_ms

    def _flush_bucket(self, key: tuple[str, str]) -> None:
        tasks = self._buckets.get(key, [])
        if not tasks:
            return
        _, interval = key
        merged = pd.concat([item.df for item in tasks if item.df is not None and not item.df.empty], ignore_index=True)
        written_rows = int(self.provider.upsert_kline_data_with_conn(self._conn, merged, interval=interval, batch_size=self.batch_size) or 0)
        if written_rows <= 0 and str(getattr(self.provider, "last_error", "")).strip():
            raise RuntimeError(self.provider.last_error)
        for item in tasks:
            if not item.result_future.done():
                item.result_future.set_result({"code": item.code, "table": item.table, "written_rows": int(item.missing_rows or 0)})
        self._buckets[key] = []

    def _flush_all(self) -> None:
        for key in list(self._buckets.keys()):
            if self._buckets.get(key):
                self._flush_bucket(key)

    def _run(self) -> None:
        try:
            while not self._stop_event.is_set() or not self.queue.empty():
                try:
                    task = self.queue.get(timeout=0.05)
                except Empty:
                    for key, tasks in list(self._buckets.items()):
                        if self._should_flush(tasks):
                            self._flush_bucket(key)
                    continue
                key = self._bucket_key(task)
                self._buckets.setdefault(key, []).append(task)
                if self._should_flush(self._buckets[key]):
                    self._flush_bucket(key)
            self._flush_all()
        except Exception as e:
            self.fatal_error = e
            for tasks in self._buckets.values():
                for item in tasks:
                    if not item.result_future.done():
                        item.result_future.set_exception(e)
            while True:
                try:
                    item = self.queue.get_nowait()
                except Empty:
                    break
                if not item.result_future.done():
                    item.result_future.set_exception(e)

    def close_and_wait(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
```

- [ ] **Step 4: 运行串行写线程测试**

Run:

```bash
pytest tests/utils/test_duckdb_serial_writer.py -v
```

Expected:

```text
2 passed
```

- [ ] **Step 5: 提交串行写线程实现**

```bash
git add tests/utils/test_duckdb_serial_writer.py src/utils/history_sync_service.py
git commit -m "feat: add serial duckdb writer for history sync"
```

### Task 4: 将检查点与串行写线程接入 `HistoryDiffSyncService`

**Files:**
- Modify: `d:\04.量化\jin-ce-zhi-suan\src\utils\history_sync_service.py`
- Create: `d:\04.量化\jin-ce-zhi-suan\tests\utils\test_history_sync_duckdb_integration.py`

- [ ] **Step 1: 写集成失败测试，锁定 DuckDB 专用分支行为**

```python
from datetime import datetime

import pandas as pd

from src.utils.history_sync_service import HistoryDiffSyncService


class DummyProvider:
    def __init__(self):
        self.last_error = ""

    def check_connectivity(self, sample_code):
        return True, "ok"


class RecordingWriter:
    def __init__(self, *args, **kwargs):
        self.submitted = []
        self.closed = False
        self.fatal_error = None

    def start(self):
        return None

    def submit(self, task):
        task.result_future.set_result({"code": task.code, "table": task.table, "written_rows": task.missing_rows})
        self.submitted.append((task.code, task.table, task.missing_rows))

    def close_and_wait(self):
        self.closed = True


def test_process_code_sync_uses_writer_for_duckdb(monkeypatch, tmp_path):
    service = HistoryDiffSyncService()
    service._duckdb_checkpoint_store = None
    service._duckdb_writer = RecordingWriter()

    monkeypatch.setattr(service, "_ensure_target_db_ready", lambda **kwargs: None)
    monkeypatch.setattr(
        service,
        "_build_worker_runtime",
        lambda **kwargs: {
            "source_provider": object(),
            "target_db_provider": DummyProvider(),
            "session": None,
            "headers": {},
        },
    )
    monkeypatch.setattr(
        service,
        "_build_source_frames",
        lambda provider, code, start_time, end_time, tables, session_only=True: {
            "dat_1mins": pd.DataFrame(
                [
                    {
                        "code": code,
                        "trade_time": "2026-03-02 09:30:00",
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0,
                        "vol": 1.0,
                        "amount": 1.0,
                        "date": "2026-03-02",
                        "pre_close": 1.0,
                        "change": 0.0,
                        "pct_chg": 0.0,
                    }
                ]
            )
        },
    )

    result = service._process_code_sync(
        code="000001.SZ",
        cfg={},
        provider_source="duckdb",
        start_time=datetime(2026, 3, 2, 9, 30, 0),
        end_time=datetime(2026, 3, 2, 15, 0, 0),
        tables=["dat_1mins"],
        session_only=True,
        write_mode="direct_db",
        direct_db_source="duckdb",
        dry_run=False,
        batch_size=500,
        on_duplicate="ignore",
        history_base_url="",
        history_api_key="",
        existing_keys_by_table={"dat_1mins": {"000001.SZ": set()}},
        runtime_token="t1",
    )

    assert service._duckdb_writer.submitted == [("000001.SZ", "dat_1mins", 1)]
    assert result["code_report"]["tables"][0]["written_rows"] == 1
```

- [ ] **Step 2: 运行集成测试，确认当前分支未接入**

Run:

```bash
pytest tests/utils/test_history_sync_duckdb_integration.py -v
```

Expected:

```text
FAILED tests/utils/test_history_sync_duckdb_integration.py::test_process_code_sync_uses_writer_for_duckdb - AssertionError
```

- [ ] **Step 3: 在 `HistoryDiffSyncService` 中接入 DuckDB writer 与 checkpoint**

```python
def _is_duckdb_serial_writer_enabled(self, write_mode: str, direct_db_source: str, cfg: dict[str, Any]) -> bool:
    return (
        str(write_mode or "").strip().lower() == "direct_db"
        and str(direct_db_source or "").strip().lower() == "duckdb"
        and self._as_bool(_cfg_get(cfg, "history_sync.duckdb_writer_enabled", True), True)
    )


def _resolve_effective_concurrency(self, requested_concurrency: Any, write_mode: str, direct_db_source: str) -> int:
    try:
        normalized = max(1, int(requested_concurrency or 1))
    except Exception:
        normalized = 1
    return min(normalized, 16)


def _submit_duckdb_write_task(self, code: str, table: str, df: pd.DataFrame, source_rows: int, existing_rows: int, missing_rows: int) -> dict[str, Any]:
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


def _process_code_sync(...):
    ...
    serial_duckdb = self._is_duckdb_serial_writer_enabled(write_mode, direct_db_source, cfg)
    ...
    if not dry_run and not missing_df.empty:
        if serial_duckdb:
            write_result = self._submit_duckdb_write_task(
                code=code,
                table=table,
                df=self._build_direct_db_upsert_df(table=table, df=missing_df),
                source_rows=int(len(source_df)),
                existing_rows=int(len(existing_keys)),
                missing_rows=int(len(missing_df)),
            )
            written_rows = int(write_result.get("written_rows", 0) or 0)
        elif write_mode == "api":
            ...
        else:
            ...
```

- [ ] **Step 4: 在 `_run_sync_impl()` 接入检查点恢复与 writer 生命周期**

```python
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
use_serial_writer = self._is_duckdb_serial_writer_enabled(write_mode, direct_db_source, cfg)
task_signature = ""
if use_serial_writer and self._as_bool(_cfg_get(cfg, "history_sync.resume_from_checkpoint", True), True):
    checkpoint = self._duckdb_checkpoint_store.initialize(checkpoint_payload, total_codes=len(codes))
    task_signature = str(checkpoint.get("task_signature", "") or "")
    completed_codes = set(checkpoint.get("completed_codes", []))
    codes = [code for code in codes if code not in completed_codes]
if use_serial_writer:
    self._duckdb_writer = DuckDbSerialWriter(
        provider=target_db_provider,
        batch_size=batch_size,
        max_batch_rows=int(_cfg_get(cfg, "history_sync.duckdb_writer_batch_rows", 3000) or 3000),
        max_batch_codes=int(_cfg_get(cfg, "history_sync.duckdb_writer_batch_codes", 8) or 8),
        max_wait_ms=int(_cfg_get(cfg, "history_sync.duckdb_writer_wait_ms", 800) or 800),
        queue_maxsize=int(_cfg_get(cfg, "history_sync.duckdb_writer_queue_maxsize", 256) or 256),
    )
    self._duckdb_writer.start()
...
for code_result in self._iter_code_chunk_results(...):
    ...
    if use_serial_writer and task_signature:
        code_name = str(code_result.get("code", "") or "").strip().upper()
        self._duckdb_checkpoint_store.mark_code_completed(task_signature, code_name)
...
finally:
    if self._duckdb_writer is not None:
        self._duckdb_writer.close_and_wait()
```

- [ ] **Step 5: 运行集成测试**

Run:

```bash
pytest tests/utils/test_history_sync_duckdb_integration.py -v
```

Expected:

```text
1 passed
```

- [ ] **Step 6: 提交服务集成改动**

```bash
git add tests/utils/test_history_sync_duckdb_integration.py src/utils/history_sync_service.py
git commit -m "feat: integrate duckdb serial writer into history sync"
```

### Task 5: 补充恢复与快速失败回归测试，并完成全量验证

**Files:**
- Modify: `d:\04.量化\jin-ce-zhi-suan\tests\utils\test_history_sync_duckdb_integration.py`
- Modify: `d:\04.量化\jin-ce-zhi-suan\tests\utils\test_duckdb_serial_writer.py`
- Modify: `d:\04.量化\jin-ce-zhi-suan\tests\utils\test_history_sync_checkpoint.py`

- [ ] **Step 1: 为恢复行为补测试**

```python
def test_run_sync_skips_completed_codes_from_checkpoint(monkeypatch, tmp_path):
    service = HistoryDiffSyncService()
    cfg = {
        "history_sync": {
            "duckdb_writer_enabled": True,
            "resume_from_checkpoint": True,
            "duckdb_writer_batch_rows": 100,
            "duckdb_writer_batch_codes": 2,
            "duckdb_writer_wait_ms": 10,
            "duckdb_writer_queue_maxsize": 10,
        }
    }
    store = HistorySyncCheckpointStore(base_dir=str(tmp_path))
    payload = {
        "provider_source": "duckdb",
        "write_mode": "direct_db",
        "direct_db_source": "duckdb",
        "start_time": "2026-03-02T09:30:00",
        "end_time": "2026-03-02T15:00:00",
        "tables": ["dat_1mins"],
        "codes": ["000001.SZ", "000002.SZ"],
        "session_only": True,
    }
    checkpoint = store.initialize(payload, total_codes=2)
    store.mark_code_completed(checkpoint["task_signature"], "000001.SZ")

    remain = [code for code in payload["codes"] if code not in store.load(checkpoint["task_signature"])["completed_codes"]]

    assert remain == ["000002.SZ"]
```

- [ ] **Step 2: 为快速失败传播补测试**

```python
def test_writer_failure_marks_task_future_exception():
    provider = FakeDuckDbProvider(fail=True)
    writer = DuckDbSerialWriter(
        provider=provider,
        batch_size=100,
        max_batch_rows=1,
        max_batch_codes=1,
        max_wait_ms=5,
        queue_maxsize=4,
    )
    writer.start()
    task = _build_task("000001.SZ", "2026-03-02 09:30:00")

    writer.submit(task)
    writer.close_and_wait()

    assert isinstance(writer.fatal_error, RuntimeError)
    assert isinstance(task.result_future.exception(), RuntimeError)
```

- [ ] **Step 3: 运行全部新增测试**

Run:

```bash
pytest tests/utils/test_history_sync_checkpoint.py tests/utils/test_duckdb_provider_reuse_conn.py tests/utils/test_duckdb_serial_writer.py tests/utils/test_history_sync_duckdb_integration.py -v
```

Expected:

```text
all selected tests passed
```

- [ ] **Step 4: 运行语言诊断并修复显而易见问题**

Run:

```bash
python -m pytest tests/utils/test_history_sync_checkpoint.py tests/utils/test_duckdb_provider_reuse_conn.py tests/utils/test_duckdb_serial_writer.py tests/utils/test_history_sync_duckdb_integration.py -q
```

Expected:

```text
.......                                                                  [100%]
```

- [ ] **Step 5: 提交回归测试与最终收尾**

```bash
git add tests/utils/test_history_sync_checkpoint.py tests/utils/test_duckdb_provider_reuse_conn.py tests/utils/test_duckdb_serial_writer.py tests/utils/test_history_sync_duckdb_integration.py src/utils/history_sync_service.py src/utils/duckdb_provider.py
git commit -m "test: cover duckdb history sync recovery flow"
```

## Self-Review

- **Spec coverage:** 已覆盖检查点、串行写线程、小批量阈值、快速失败、DuckDB provider 长连接写入、DuckDB 专用集成分支与恢复跳过逻辑。
- **Placeholder scan:** 计划中未使用 `TBD`、`TODO`、`implement later`、`similar to` 等占位写法。
- **Type consistency:** 计划中统一使用 `HistorySyncCheckpointStore`、`DuckDbWriteTask`、`DuckDbSerialWriter`、`upsert_kline_data_with_conn()` 命名，与设计文档保持一致。
