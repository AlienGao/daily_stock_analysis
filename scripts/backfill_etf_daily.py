#!/usr/bin/env python3
"""回填 ETF 日线数据到 etf_daily 表。

数据来源：Tushare fund_daily API → 标准化 → etf_daily 表 UPSERT。
先通过 fund_basic 获取 ETF 列表（含名称），再逐只回填 fund_daily。

用法:
    python scripts/backfill_etf_daily.py --test       # 测试 ~90 天
    python scripts/backfill_etf_daily.py               # 全量 2026-01-01 至今
    python scripts/backfill_etf_daily.py --codes 510050,510300  # 指定 ETF
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("backfill_etf")


# ── 主要 ETF 列表（ts_code 格式）─────────────────────────────────────────────
# 覆盖宽基、行业、跨境、商品等主流品种
MAJOR_ETF_TS_CODES = [
    # 宽基
    "510050.SH",  # 上证50ETF
    "510300.SH",  # 沪深300ETF
    "510500.SH",  # 中证500ETF
    "512100.SH",  # 中证1000ETF
    "159919.SZ",  # 沪深300ETF易方达
    "159845.SZ",  # 中证1000ETF
    "510880.SH",  # 红利ETF
    "515180.SH",  # 红利低波ETF
    "159915.SZ",  # 创业板ETF
    "159949.SZ",  # 创业板50ETF
    "588000.SH",  # 科创50ETF
    "588050.SH",  # 科创ETF
    # 行业
    "512880.SH",  # 证券ETF
    "512660.SH",  # 军工ETF
    "512760.SH",  # 芯片ETF
    "159865.SZ",  # 养殖ETF
    "159928.SZ",  # 消费ETF
    "159929.SZ",  # 医药ETF
    "515050.SH",  # 5GETF
    "515790.SH",  # 光伏ETF
    "515030.SH",  # 新能源ETF
    "516160.SH",  # 新能源车ETF
    "515700.SH",  # 电池ETF
    "159766.SZ",  # 旅游ETF
    # 跨境
    "513050.SH",  # 中概互联ETF
    "513100.SH",  # 纳指ETF
    "513500.SH",  # 标普500ETF
    "159941.SZ",  # 纳指ETF
    # 商品
    "518880.SH",  # 黄金ETF
    "159985.SZ",  # 豆粕ETF
]

# ── Tushare fund_basic 中 type='E' 为 ETF ──────────────────────────────────
MARKET_ETF_MAP = {
    "510": ".SH", "512": ".SH", "513": ".SH", "515": ".SH",
    "516": ".SH", "517": ".SH", "518": ".SH", "520": ".SH",
    "521": ".SH", "560": ".SH", "561": ".SH", "562": ".SH",
    "563": ".SH", "564": ".SH", "565": ".SH", "566": ".SH",
    "567": ".SH", "568": ".SH", "569": ".SH", "588": ".SH",
    "15": ".SZ", "16": ".SZ", "18": ".SZ",
}


def code_to_ts_code(bare: str) -> str:
    """6 位纯数字代码 → ts_code (如 510050 → 510050.SH)。"""
    bare = str(bare).strip().zfill(6)
    for prefix, suffix in MARKET_ETF_MAP.items():
        if bare.startswith(prefix):
            return f"{bare}{suffix}"
    return bare


def list_etfs_from_api(fetcher) -> list[tuple[str, str]]:
    """通过 Tushare fund_basic 获取所有 ETF 列表。
    Returns: [(ts_code, name), ...]
    """
    try:
        fetcher._check_rate_limit()
        df = fetcher._api.fund_basic(market="E")
        if df is None or df.empty:
            logger.warning("fund_basic 未返回数据")
            return []
        results = []
        for _, row in df.iterrows():
            ts_code = str(row.get("ts_code", "")).strip()
            name = str(row.get("name", "")).strip()
            if ts_code:
                results.append((ts_code, name))
        logger.info("fund_basic 返回 %d 只 ETF", len(results))
        return results
    except Exception as exc:
        logger.warning("fund_basic 调用失败: %s", exc)
        return []


def fetch_etf_daily(fetcher, ts_code: str, start_date: str, end_date: str):
    """通过 Tushare fund_daily 拉取单只 ETF 日线。"""
    ts_start = start_date.replace("-", "")
    ts_end = end_date.replace("-", "")

    fetcher._check_rate_limit()
    df = fetcher._api.fund_daily(ts_code=ts_code, start_date=ts_start, end_date=ts_end)
    if df is None or df.empty:
        return None

    import pandas as pd

    result = pd.DataFrame()
    result["date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    result["open"] = pd.to_numeric(df["open"], errors="coerce")
    result["high"] = pd.to_numeric(df["high"], errors="coerce")
    result["low"] = pd.to_numeric(df["low"], errors="coerce")
    result["close"] = pd.to_numeric(df["close"], errors="coerce")
    result["volume"] = pd.to_numeric(df["vol"], errors="coerce")
    result["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    result["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce")
    result = result.sort_values("date").reset_index(drop=True)
    return result


def fetch_fund_adj(fetcher, ts_code: str, start_date: str, end_date: str):
    """通过 Tushare fund_adj 拉取 ETF 复权因子。"""
    ts_start = start_date.replace("-", "")
    ts_end = end_date.replace("-", "")

    fetcher._check_rate_limit()
    df = fetcher._api.fund_adj(ts_code=ts_code, start_date=ts_start, end_date=ts_end)
    if df is None or df.empty:
        return None

    import pandas as pd

    result = pd.DataFrame()
    result["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    result["adj_factor"] = pd.to_numeric(df["adj_factor"], errors="coerce")
    result = result.sort_values("trade_date").reset_index(drop=True)
    return result


def upsert_fund_adj(db, etf_code: str, df) -> int:
    """UPSERT 到 fund_adj_factor 表。"""
    from src.storage import FundAdjFactor
    from sqlalchemy import and_

    if df is None or df.empty:
        return 0

    saved = 0
    with db.get_session() as session:
        for _, row in df.iterrows():
            row_date = row["trade_date"]
            if hasattr(row_date, "to_pydatetime"):
                row_date = row_date.to_pydatetime().date()
            elif hasattr(row_date, "date"):
                row_date = row_date.date()

            adj = _v(row.get("adj_factor"))
            if adj is None or adj <= 0:
                continue

            existing = session.query(FundAdjFactor).filter(
                and_(FundAdjFactor.code == etf_code, FundAdjFactor.trade_date == row_date)
            ).first()

            if existing:
                existing.adj_factor = adj
            else:
                session.add(FundAdjFactor(
                    code=etf_code,
                    trade_date=row_date,
                    adj_factor=adj,
                ))
            saved += 1

        session.commit()
        return saved


def upsert_etf_daily(db, etf_code: str, etf_name: str, df) -> int:
    """UPSERT 到 etf_daily 表。"""
    from src.storage import EtfDaily
    from sqlalchemy import and_

    if df is None or df.empty:
        return 0

    with db.get_session() as session:
        saved = 0
        for _, row in df.iterrows():
            row_date = row["date"]
            if hasattr(row_date, "to_pydatetime"):
                row_date = row_date.to_pydatetime().date()
            elif hasattr(row_date, "date"):
                row_date = row_date.date()

            existing = session.query(EtfDaily).filter(
                and_(EtfDaily.code == etf_code, EtfDaily.date == row_date)
            ).first()

            if existing:
                existing.open = _v(row.get("open"))
                existing.high = _v(row.get("high"))
                existing.low = _v(row.get("low"))
                existing.close = _v(row.get("close"))
                existing.volume = _v(row.get("volume"))
                existing.amount = _v(row.get("amount"))
                existing.pct_chg = _v(row.get("pct_chg"))
                existing.name = etf_name
                existing.updated_at = datetime.now()
            else:
                session.add(EtfDaily(
                    code=etf_code,
                    date=row_date,
                    name=etf_name,
                    open=_v(row.get("open")),
                    high=_v(row.get("high")),
                    low=_v(row.get("low")),
                    close=_v(row.get("close")),
                    volume=_v(row.get("volume")),
                    amount=_v(row.get("amount")),
                    pct_chg=_v(row.get("pct_chg")),
                ))
            saved += 1

        session.commit()
        return saved


def _v(val):
    """Coerce to float or None."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if (f != f or f in (float("inf"), float("-inf"))) else f
    except (TypeError, ValueError):
        return None


def main():
    parser = argparse.ArgumentParser(description="回填 ETF 日线数据")
    parser.add_argument("--start", default=None, help="起始日期 YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="结束日期 YYYY-MM-DD（默认: 今天）")
    parser.add_argument("--test", action="store_true", help="测试模式：仅回填最近 ~90 天")
    parser.add_argument("--dry-run", action="store_true", help="仅预览不写入")
    parser.add_argument("--codes", default=None, help="逗号分隔的 ts_code 列表，如 510050.SH,510300.SH")
    parser.add_argument("--use-major", action="store_true", help="仅回填预定义 MAJOR_ETF_TS_CODES（默认全量）")
    parser.add_argument("--use-all", action="store_true", help="通过 fund_basic 获取全量 ETF 列表（默认行为）")
    args = parser.parse_args()

    from src.storage import DatabaseManager

    end_date = args.end or date.today().strftime("%Y-%m-%d")
    if args.test:
        start_date = (date.today() - timedelta(days=90)).strftime("%Y-%m-%d")
        logger.info("测试模式: %s ~ %s", start_date, end_date)
    elif args.start:
        start_date = args.start
    else:
        start_date = "2026-01-01"

    logger.info("范围: %s ~ %s", start_date, end_date)

    # ── 确定 ETF 列表 ──
    etf_list: list[tuple[str, str]] = []

    if args.codes:
        for raw in args.codes.split(","):
            raw = raw.strip()
            if raw:
                bare = raw.split(".")[0].strip().zfill(6)
                ts = code_to_ts_code(bare)
                etf_list.append((ts, ""))
        logger.info("指定 %d 只 ETF", len(etf_list))
    elif args.use_all:
        from data_provider.tushare_fetcher import TushareFetcher

        fetcher = TushareFetcher.get_instance()
        if fetcher._api is None:
            logger.error("Tushare API 未初始化，请检查 TUSHARE_TOKEN")
            return 1
        etf_list = list_etfs_from_api(fetcher)
        if not etf_list:
            logger.error("未获取到 ETF 列表")
            return 1
        logger.info("全量 %d 只 ETF", len(etf_list))
    else:
        # 默认通过 fund_basic 获取全量 ETF
        from data_provider.tushare_fetcher import TushareFetcher as _F
        _fetcher_tmp = _F.get_instance()
        if _fetcher_tmp._api is not None:
            all_etfs_tmp = list_etfs_from_api(_fetcher_tmp)
            if all_etfs_tmp:
                etf_list = list(all_etfs_tmp)
        if not etf_list:
            for ts in MAJOR_ETF_TS_CODES:
                etf_list.append((ts, ""))
        logger.info("共 %d 只 ETF", len(etf_list))

    # ── 同类型 ETF 去重（同名/同类型只保留一只） ──
    # 策略：按名称核心词分组，每组保留沪市（.SH）的，无沪市则保留任意一只
    if etf_list and not args.codes:
        from collections import OrderedDict
        # 已知的 ETF 核心词分类（按优先级排序，匹配到更具体的分类优先）
        KEYWORDS = [
            # 宽基
            "上证50", "沪深300", "中证500", "中证1000", "中证2000",
            "上证指数",
            # 科创板（先匹配具体再匹配通用）
            "科创50", "科创100", "科创AI", "科创芯片", "科创生物医药",
            "科创新能源", "科创人工智能", "科创材料", "科创信息",
            "科创成长", "科创板综合",
            # 创业板
            "创业板50", "创业板300", "创业板200", "创业板综合",
            "创业板增强", "创业板成长", "创业板动量", "创业板低波",
            "创业板",
            # 红利
            "红利低波", "红利质量", "红利价值",
            "红利",
            # 金融
            "证券", "证券保险", "券商", "银行", "保险", "金融",
            # 军工
            "军工",
            # 科技/芯片/半导体
            "芯片", "半导体", "人工智能", "AI", "软件", "信息技术",
            "数字经济", "互联网", "计算机",
            # 医药/医疗
            "医疗", "医药", "生物医药", "医疗器械", "创新药", "医美",
            "疫苗", "中药",
            # 消费
            "消费50", "消费龙头", "可选消费", "食品饮料", "酒", "白酒",
            "消费",
            # 新能源/车/电池/光伏
            "新能源车", "新能源汽车",
            "电池", "光伏",
            "新能源",
            # 周期/制造
            "有色金属", "钢铁", "煤炭", "化工", "稀土", "稀有金属",
            "矿业", "油气", "石油天然气", "石化",
            "工业互联网", "工程机械", "机器人", "高端制造", "电力",
            # 农业
            "养殖", "农业", "畜牧",
            # 地产/基建/建材
            "地产", "基建", "建材", "央企",
            # 旅游/传媒
            "旅游", "传媒", "游戏",
            # 跨境
            "中概互联", "恒生科技", "恒生互联网", "恒生医疗", "恒生消费",
            "恒生生物科技", "恒生中国企业", "港股通50", "港股通科技",
            "香港银行", "沪港深",
            "纳指", "标普500", "日经225", "日经",
            # 商品
            "黄金", "豆粕", "能源化工",
        ]
        selected = {}
        for ts_code, name in etf_list:
            ts_code = str(ts_code).strip()
            name = str(name or "").strip()
            # 仅保留纯 ETF 代码（排除 LOF、联接基金、REIT、定开混合等）
            if not ts_code.endswith((".SH", ".SZ")):
                continue
            bare_code = ts_code.split(".")[0].strip()
            if not (bare_code.isdigit() and len(bare_code) == 6):
                continue
            skip_words = ["联接", "LOF", "REIT", "定开", "混合", "ETF联接", "指数增强", "ETF(LOF)",
                           "ETF联接", "ETF-LOF", "联接-LOF", "FOF", "ETF-FOF"]
            name_lower = name.lower()
            if any(w.lower() in name_lower for w in skip_words):
                continue
            # 找最长的匹配关键词（更具体的优先）
            matched_keyword = None
            best_len = 0
            for kw in KEYWORDS:
                if kw in name:
                    if len(kw) > best_len:
                        matched_keyword = kw
                        best_len = len(kw)
            if not matched_keyword:
                continue
            market_sh = ".SH" in ts_code
            existing = selected.get(matched_keyword)
            if existing is None:
                selected[matched_keyword] = (ts_code, name)
            elif market_sh and ".SH" not in existing[0]:
                selected[matched_keyword] = (ts_code, name)
            elif market_sh and ".SH" in existing[0]:
                # 沪市 vs 沪市，保留名字更短的（更纯正的ETF）
                if len(name) < len(existing[1]):
                    selected[matched_keyword] = (ts_code, name)
        etf_list = list(selected.values())
        logger.info("去重后 %d 只 ETF（按 %d 个核心词分类）", len(etf_list), len(selected))

    # ── 分批获取名称（如果名称为空） ──
    from data_provider.tushare_fetcher import TushareFetcher

    fetcher = TushareFetcher.get_instance()
    if fetcher._api is None:
        logger.error("Tushare API 未初始化，请检查 TUSHARE_TOKEN")
        return 1

    # 如果名称全为空，尝试通过 fund_basic 获取
    if all(not name for _, name in etf_list):
        logger.info("通过 fund_basic 获取 ETF 名称…")
        all_etfs = list_etfs_from_api(fetcher)
        name_map = {ts: name for ts, name in all_etfs}
        etf_list = [(ts, name_map.get(ts, "")) for ts, _ in etf_list]

    # ── 回填 ──
    db = DatabaseManager()
    total_saved = 0
    total_failed = 0
    total_adj_saved = 0

    for idx, (ts_code, etf_name) in enumerate(etf_list):
        bare = ts_code.split(".")[0].strip().zfill(6)
        label = f"[{idx + 1}/{len(etf_list)}] {ts_code} {etf_name or ''}".strip()

        logger.info("%s 拉取 fund_daily…", label)
        df = fetch_etf_daily(fetcher, ts_code, start_date, end_date)

        if df is None or df.empty:
            logger.warning("%s 无数据，跳过", label)
            total_failed += 1
            continue

        if args.dry_run:
            logger.info("  dry-run: 获取 %d 行 (%s ~ %s)", len(df),
                        df["date"].min().strftime("%Y-%m-%d"),
                        df["date"].max().strftime("%Y-%m-%d"))
            total_saved += len(df)
            continue

        name = etf_name or ts_code
        saved = upsert_etf_daily(db, bare, name, df)
        logger.info("  → 入库 %d 行", saved)
        total_saved += saved

        # ── 回填复权因子 ──
        logger.info("%s 拉取 fund_adj…", label)
        adj_df = fetch_fund_adj(fetcher, ts_code, start_date, end_date)
        if adj_df is not None and not adj_df.empty:
            adj_saved = upsert_fund_adj(db, bare, adj_df)
            logger.info("  → 复权因子入库 %d 行", adj_saved)
            total_adj_saved += adj_saved
        else:
            logger.warning("%s 无复权因子数据", label)

    logger.info("===== 完成 =====")
    logger.info("处理 %d / %d 只 ETF, 入库 %d 行, 复权因子 %d 行, 失败 %d 只",
                len(etf_list) - total_failed, len(etf_list), total_saved, total_adj_saved, total_failed)

    # ── 验证 ──
    if not args.dry_run:
        with db.get_session() as sess:
            from src.storage import EtfDaily, FundAdjFactor
            from sqlalchemy import func

            cnt = sess.query(func.count()).select_from(EtfDaily).scalar()
            code_cnt = sess.query(func.count(EtfDaily.code.distinct())).scalar()
            mind = sess.query(func.min(EtfDaily.date)).scalar()
            maxd = sess.query(func.max(EtfDaily.date)).scalar()
            logger.info("etf_daily 表: %d 行, %d 只 ETF (%s ~ %s)", cnt, code_cnt, mind, maxd)

            adj_cnt = sess.query(func.count()).select_from(FundAdjFactor).scalar()
            adj_code_cnt = sess.query(func.count(FundAdjFactor.code.distinct())).scalar()
            logger.info("fund_adj_factor 表: %d 行, %d 只 ETF", adj_cnt, adj_code_cnt)

    return 0


if __name__ == "__main__":
    sys.exit(main())
