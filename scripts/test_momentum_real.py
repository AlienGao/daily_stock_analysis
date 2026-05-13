# -*- coding: utf-8 -*-
"""Real-data test for MomentumFactor — outflow+volume and momentum building."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date
import pandas as pd

from src.discovery.factors.momentum_factor import MomentumFactor

factor = MomentumFactor()
trade_date = date.today().strftime("%Y%m%d")
print(f"Trade date: {trade_date}")

df = factor.fetch_data(trade_date)
if df is None or df.empty:
    print("No data, exiting")
    sys.exit(1)

print(f"Stocks: {len(df)}, columns: {list(df.columns)}")

scores = factor.score(df, trade_date=trade_date)

ir = df.get("inflow_rate", pd.Series(0, index=df.index))
vr = df.get("volume_ratio", pd.Series(1, index=df.index))
pct = df.get("pct_chg", pd.Series(0, index=df.index))
tr = df.get("turnover_rate", pd.Series(0, index=df.index))

inflow_pos = (ir > 0).sum()
inflow_neg = (ir < 0).sum()
print(f"inflow_rate: >0={inflow_pos}, <0={inflow_neg}")

# --- Outflow + high volume (distribution scenario) ---
outflow_highvol = df[(ir < 0) & (vr >= 1.5)]
print(f"\n=== Net outflow + vol>=1.5x: {len(outflow_highvol)} stocks ===")
if len(outflow_highvol) > 0:
    ohv_scores = scores.loc[outflow_highvol.index].sort_values(ascending=False)
    print("Top 10:")
    for i, (c, s) in enumerate(ohv_scores.head(10).items()):
        nm = str(df.loc[c, "name"]) if "name" in df.columns else ""
        print(f"  {i+1}. {c} {nm:8s} ir={ir[c]:.3f} vr={vr[c]:.1f}x pct={pct[c]:.1f}% tr={tr[c]:.1f}% score={s:.1f}")

# --- Inflow + high volume (contrast) ---
inflow_highvol = df[(ir > 0.03) & (vr >= 1.5)]
print(f"\n=== Inflow>3% + vol>=1.5x: {len(inflow_highvol)} stocks ===")
if len(inflow_highvol) > 0:
    ihv = scores.loc[inflow_highvol.index].sort_values(ascending=False)
    for i, (c, s) in enumerate(ihv.head(10).items()):
        nm = str(df.loc[c, "name"]) if "name" in df.columns else ""
        print(f"  {i+1}. {c} {nm:8s} ir={ir[c]:.3f} vr={vr[c]:.1f}x pct={pct[c]:.1f}% score={s:.1f}")

# --- Top 20 ---
print(f"\n=== Top 20 ===")
for i, (c, s) in enumerate(scores.nlargest(20).items()):
    nm = str(df.loc[c, "name"]) if "name" in df.columns else ""
    print(f"  {i+1:2d}. {c} {nm:8s} ir={ir[c]:.3f} vr={vr[c]:.1f}x pct={pct[c]:.1f}% score={s:.1f}")

# --- Score distribution ---
print(f"\n=== Distribution ===")
print(f"  All: avg={scores.mean():.1f} med={scores.median():.1f} max={scores.max():.1f}")
print(f"  Inflow>0: avg={scores[ir>0].mean():.1f} med={scores[ir>0].median():.1f}")
print(f"  Inflow<0: avg={scores[ir<0].mean():.1f} med={scores[ir<0].median():.1f}")

# --- Momentum building stats ---
mb = factor._cached_mbuilding
if mb:
    mb_scores = pd.Series({k: v["score"] for k, v in mb.items()})
    print(f"\n=== Momentum building ===")
    print(f"  Count: {len(mb_scores)}, avg={mb_scores.mean():.2f}, max={mb_scores.max():.1f}")
    print(f"  Score>=5: {(mb_scores>=5).sum()}, >=10: {(mb_scores>=10).sum()}")
    print(f"  Score distribution: 0={(mb_scores==0).sum()} (0,5)={((mb_scores>0)&(mb_scores<5)).sum()} [5,10)={((mb_scores>=5)&(mb_scores<10)).sum()} [10,20]={(mb_scores>=10).sum()}")
    if (mb_scores > 0).sum() > 0:
        top_mb = mb_scores.nlargest(5)
        for c, s in top_mb.items():
            d = mb.get(c, {})
            print(f"  {c}: score={s} ir_delta={d.get('inflow_delta','?')} vol_delta={d.get('vol_delta','?')} pct_delta={d.get('pct_delta','?')}")
