"""用今日盘中 Top 5 真实数据测试 StockScorer 评分。"""
import json
import numpy as np
from pathlib import Path
from datetime import date, timedelta

# 读取今日盘中 Top N
report_path = Path(__file__).parent.parent / "discovery_reports/intraday_20260513_topn.json"
topn = json.loads(report_path.read_text())

from src.storage import DatabaseManager
from src.services.stock_scorer import StockScorer

db = DatabaseManager()
scorer = StockScorer()

# 预加载板块涨跌幅
spot = db.get_realtime_spot()
ths_map = db.get_ths_industry_map()
sector_name = ""
if spot is not None and not spot.empty and ths_map:
    spot_c = spot.copy()
    spot_c["sector_name"] = spot_c.index.map(ths_map)
    sector_pct = spot_c.groupby("sector_name")["pct_chg"].mean().dropna()
    scorer.preload_sector_pct(sector_pct.to_dict())
    print(f"板块数据: {len(sector_pct)} 个行业\n")

# 预加载大盘 OHLCV
try:
    index_rows = db.get_data_range("000001", (date.today() - timedelta(days=90)), date.today())
    if index_rows:
        ohlcv_arr = np.array([[r.open, r.high, r.low, r.close] for r in index_rows])
        scorer.preload_index_ohlcv(ohlcv_arr)
        print(f"大盘数据: {len(ohlcv_arr)} 天\n")
except Exception as e:
    print(f"大盘数据加载失败: {e}\n")

print(f"{'代码':<8} {'名称':<8} {'原始分':<6} {'Tech':<6} {'RR':<6} {'大盘':<6} {'板块':<6} {'量能':<6} {'位置':<6} {'形态':<6}")
print("-" * 80)

for stock in topn:
    code = stock["stock_code"]
    name = stock["stock_name"]
    price = stock["price_at_discovery"]
    tp1 = stock["take_profit_1"]
    tp2 = stock["take_profit_2"]
    stop = stock["stop_loss"]
    reasons = stock.get("reasons", [])
    sector = stock.get("sector", "")
    sector_name = sector  # for final weight print

    ohlcv_rows = db.get_data_range(code, date.today() - timedelta(days=180), date.today())
    if not ohlcv_rows:
        print(f"{code:<8} {name:<8} 无OHLCV数据")
        continue

    highs = np.array([r.high for r in ohlcv_rows], dtype=float)
    lows = np.array([r.low for r in ohlcv_rows], dtype=float)
    closes = np.array([r.close for r in ohlcv_rows], dtype=float)
    pre_close = float(closes[-2]) if len(closes) > 1 else float(closes[-1])

    vol_ratio = 1.0
    if hasattr(ohlcv_rows[-1], 'vol') and len(ohlcv_rows) >= 6:
        vols = np.array([r.vol for r in ohlcv_rows[-6:]], dtype=float)
        mean_vol = np.mean(vols[:-1])
        if mean_vol > 0:
            vol_ratio = float(vols[-1] / mean_vol)

    tech = scorer.score(
        stock_code=code, sector=sector, price=price, pre_close=pre_close,
        tp1=tp1, tp2=tp2, stop_loss=stop,
        reasons=reasons, ohlcv=(highs, lows, closes), volume_ratio=vol_ratio,
    )

    print(
        f"{code:<8} {name:<8} {stock['score']:<6.1f} "
        f"{tech.composite:<6.1f} {tech.rr_score:<6.1f} {tech.market_score:<6.1f} "
        f"{tech.sector_score:<6.1f} {tech.volume_score:<6.1f} {tech.position_score:<6.1f} "
        f"{tech.formation_score:<6.1f}"
    )

print()
print("动态权重:", {k: round(v, 2) for k, v in scorer._get_dynamic_weights(sector_name, 50).items()})
