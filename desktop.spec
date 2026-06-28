# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller 打包配置 — 金策智算 桌面端
============================================
打包方式（Windows）:
    build_desktop.bat   (双击运行)

打包方式（macOS / Linux）:
    bash scripts/build_desktop.sh

生成产物:
    Windows: dist/金策智算/金策智算.exe
    macOS:   dist/金策智算.app/
    Linux:   dist/金策智算/
"""

import os
import sys
import site

block_cipher = None
project_root = os.path.dirname(os.path.abspath(SPEC))
is_macos = sys.platform == "darwin"
is_windows = sys.platform == "win32"

# ---------------------------------------------------------------
# 收集 site-packages（支持 Windows Store Python 等非常规路径）
# ---------------------------------------------------------------
VENV_SITE = None
for sp in site.getsitepackages() + [site.getusersitepackages()]:
    if os.path.isdir(sp) and os.path.isfile(os.path.join(sp, "pandas", "__init__.py")):
        VENV_SITE = sp
        break
# fallback: 从已安装包的 import 路径反推
if not VENV_SITE:
    import importlib.util
    spec = importlib.util.find_spec("pandas")
    if spec and spec.origin:
        VENV_SITE = os.path.dirname(os.path.dirname(spec.origin))
        if not os.path.isdir(VENV_SITE):
            VENV_SITE = None
if not VENV_SITE:
    VENV_SITE = os.path.join(os.path.dirname(sys.executable), "Lib", "site-packages")
    if not os.path.isdir(VENV_SITE):
        VENV_SITE = None

# ---------------------------------------------------------------
# datas: 非 Python 运行时文件
# ---------------------------------------------------------------
datas = []
for fname in ["dashboard.html", "backtest_report.html", "logo.png", "server.py", "config.json"]:
    fpath = os.path.join(project_root, fname)
    if os.path.exists(fpath):
        datas.append((fpath, "."))

src_path = os.path.join(project_root, "src")
if os.path.isdir(src_path):
    datas.append((src_path, "src"))

static_path = os.path.join(project_root, "static")
if os.path.isdir(static_path):
    datas.append((static_path, "static"))

# databases/ 目录：内置数据库数据
databases_path = os.path.join(project_root, "databases")
if os.path.isdir(databases_path):
    datas.append((databases_path, "databases"))

# data/ 目录：用户数据，macOS 上运行时复制到 ~/Library/Application Support/
data_path = os.path.join(project_root, "data")
if os.path.isdir(data_path):
    datas.append((data_path, "data"))

# 第三方包的数据文件（akshare 需要 file_fold 下的 calendar.json 等）
if VENV_SITE and os.path.isdir(VENV_SITE):
    ak_path = os.path.join(VENV_SITE, "akshare", "file_fold")
    if os.path.isdir(ak_path):
        datas.append((ak_path, "akshare/file_fold"))
    # matplotlib 需要 mpl-data
    mpl_data = os.path.join(VENV_SITE, "matplotlib", "mpl-data")
    if os.path.isdir(mpl_data):
        datas.append((mpl_data, "matplotlib/mpl-data"))

# ---------------------------------------------------------------
# excludes
# ---------------------------------------------------------------
excludes = [
    "pytest", "tests", "docs", "scripts",
    ".git", ".venv", "venv", "__pycache__",
    "node_modules", ".vscode", ".idea",
    "*.md", "*.txt", "*.log", "*.sql",
    "server-start.log", "server-start-error.log",
]

# ---------------------------------------------------------------
# hiddenimports
# ---------------------------------------------------------------
hiddenimports = [
    "runpy", "webbrowser",
    "pystray", "PIL",
    # macOS 菜单栏托盘依赖（pystray 的 darwin backend 依赖 pyobjc）
    "objc", "Foundation", "AppKit", "PyObjCTools",
    "uvicorn", "uvicorn.logging", "uvicorn.protocols", "uvicorn.protocols.http",
    "uvicorn.protocols.http.auto", "uvicorn.protocols.websockets", "uvicorn.lifespan",
    "uvicorn.loops", "uvicorn.loops.auto", "uvicorn.server",
    "starlette", "starlette.middleware", "starlette.middleware.cors",
    "fastapi", "fastapi.middleware", "fastapi.middleware.cors", "fastapi.staticfiles",
    "fastapi.responses", "fastapi.exceptions",
    "starlette.exceptions",
    "sse_starlette", "sse_starlette.sse",
    "pydantic",
    "pandas", "numpy", "matplotlib", "mplfinance",
    "tushare", "akshare", "pymysql", "psycopg2", "duckdb",
    "mootdx", "pytdx",
    "src.evolution.core.orchestrator",
    "src.evolution.core.runtime_manager",
    "src.evolution.core.event_bus",
    "src.evolution.core.evolution_profile",
    "src.evolution.agents.researcher",
    "src.evolution.agents.critic",
    "src.evolution.agents.trader",
    "src.evolution.agents.library_committer",
    "src.evolution.memory.strategy_memory",
    "src.evolution.memory.gene_run_store",
    "src.evolution.memory.profile_update_store",
    "src.evolution.memory.analysis_store",
    "src.evolution.adapters.gene_strategy_adapter",
    "src.evolution.adapters.fundamental_adapter",
    "src.evolution.platform.platform_hub",
    "src.consistency.storage.live_snapshot_store",
    "src.consistency.replay.replay_builder",
    "src.consistency.replay.replay_store",
    "src.consistency.reporting.report_builder",
    "src.consistency.reporting.report_store",
    "src.consistency.adapters.backtest_report_adapter",
    "src.core.live_cabinet",
    "src.core.backtest_cabinet",
    "src.core.crown_prince",
    "src.core.zhongshu_sheng",
    "src.core.menxia_sheng",
    "src.core.shangshu_sheng",
    "src.utils.config_loader",
    "src.utils.data_provider",
    "src.utils.tushare_provider",
    "src.utils.akshare_provider",
    "src.utils.mysql_provider",
    "src.utils.postgres_provider",
    "src.utils.duckdb_provider",
    "src.utils.tdx_provider",
    "src.utils.history_sync_service",
    "src.utils.backtest_baseline",
    "src.utils.webhook_notifier",
    "src.utils.stock_manager",
    "src.utils.blk_loader",
    "src.tdx.formula_compiler",
    "src.tdx.terminal_bridge",
    "src.strategies.strategy_factory",
    "src.strategies.strategy_manager_repo",
    "src.strategies.implemented_strategies",
    "src.strategy_intent.intent_engine",
]

# ---------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------
# Windows-only params are safe on macOS — PyInstaller ignores them
a = Analysis(
    [os.path.join(project_root, "desktop_launcher.py")],
    pathex=[project_root, VENV_SITE] if VENV_SITE else [project_root],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[VENV_SITE] if VENV_SITE else [],
    runtime_hooks=[],
    excludes=excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# macOS 菜单栏托盘应用默认不显示控制台（通过落盘日志排查）
console_option = False if is_macos else True

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="金策智算",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=console_option,
    icon=os.path.join(project_root, "logo.png"),
)

if is_macos:
    app = BUNDLE(
        exe,
        a.binaries,
        a.zipfiles,
        a.datas,
        name="金策智算.app",
        icon=os.path.join(project_root, "logo.png"),
        bundle_identifier="com.jincenzhisuan.app",
        version="1.0.0",
        info_plist={
            "NSPrincipalClass": "NSApplication",
            "NSHighResolutionCapable": "True",
            "LSMinimumSystemVersion": "10.15",
            # 菜单栏托盘应用：隐藏 Dock 图标（仅菜单栏显示）
            "LSUIElement": "1",
        },
    )
else:
    # Windows/Linux: COLLECT creates the output folder
    coll_spec = COLLECT(
        exe,
        a.binaries,
        a.zipfiles,
        a.datas,
        strip=False,
        upx=False,
        upx_exclude=[],
        name="金策智算",
    )
