# -*- coding: utf-8 -*-
"""pytest 全局配置 — 测试数据库隔离到 apps/dsa-web/data/。

通过设置 DATABASE_PATH 环境变量，所有测试默认使用 Web 前端同目录的
SQLite 数据库，避免污染生产数据库（data/stock_analysis.db）。

如需使用临时数据库，在测试中显式 override 即可：
    os.environ["DATABASE_PATH"] = temp_db_path
    Config.reset_instance()
    DatabaseManager.reset_instance()
"""

import os
from pathlib import Path


def pytest_configure(config):
    """pytest 启动时注入测试数据库路径。"""
    repo_root = Path(__file__).resolve().parent.parent
    test_db = repo_root / "apps" / "dsa-web" / "data" / "stock_analysis.db"

    # 确保父目录存在
    test_db.parent.mkdir(parents=True, exist_ok=True)

    test_db_path = str(test_db)
    os.environ["DATABASE_PATH"] = test_db_path

    # 重置已缓存单例，确保后续 import 走新路径
    _reset_singletons()


def _reset_singletons():
    """重置 Config / DatabaseManager 单例。"""
    try:
        from src.config import Config
        Config.reset_instance()
    except Exception:
        pass
    try:
        from src.storage import DatabaseManager
        DatabaseManager.reset_instance()
    except Exception:
        pass


def pytest_sessionstart(session):
    """每次测试会话开始时清理测试数据库，避免累积数据导致 UNIQUE 约束冲突。"""
    repo_root = Path(__file__).resolve().parent.parent
    test_db = repo_root / "apps" / "dsa-web" / "data" / "stock_analysis.db"
    if test_db.exists():
        test_db.unlink()
