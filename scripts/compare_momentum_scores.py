# -*- coding: utf-8 -*-
"""Compare old vs new MomentumFactor scoring with real data."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date
import pandas as pd
import numpy as np

from src.discovery.factors.momentum_factor import MomentumFactor, _linear_map

factor = MomentumFactor()
trade_date = date.today().strftime("%Y%m%d")
print(f"Trade date: {trade_date}")

df = factor.fetch_data(trade_date)
if df is None or df.empty:
    print("No data, exiting")
    sys.exit(1)

print(f"Stocks: {len(df)}, columns: {list(df.columns)}")
print(f"data_source: {df['data_source'].unique()}")

# ── NEW scoring (current code) ──
new_scores = factor.score(df, trade_date=trade_date)

# ── OLD scoring (simulate pre-refactor logic) ──
idx = df.index
zeros = pd.Series(0.0, index=idx)

inflow_rate = df.get("inflow_rate", zeros)
volume_ratio = df.get("volume_ratio", pd.Series(1.0, index=idx))
turnover_rate = df.get("turnover_rate", zeros)
pct_chg = df.get("pct_chg", zeros)

# OLD: inflow 0-35 (narrower), vol_ratio 0-25 (independent), turnover 0-15, pct_chg 0-25
s_inflow_old = zeros.copy()
s_inflow_old = s_inflow_old.mask(inflow_rate > 0.10, 35.0)
s_inflow_old = s_inflow_old.mask((inflow_rate >= 0.03) & (inflow_rate <= 0.10),
                                 _linear_map(inflow_rate, 0.03, 18, 0.10, 35))
s_inflow_old = s_inflow_old.mask((inflow_rate > 0) & (inflow_rate < 0.03),
                                 _linear_map(inflow_rate, 0, 0, 0.03, 18))

s_vol_old = zeros.copy()
vr = volume_ratio.fillna(1.0)
s_vol_old = s_vol_old.mask(vr >= 3.0, 25.0)
s_vol_old = s_vol_old.mask((vr >= 2.0) & (vr < 3.0), _linear_map(vr, 2.0, 18, 3.0, 25))
s_vol_old = s_vol_old.mask((vr >= 1.5) & (vr < 2.0), _linear_map(vr, 1.5, 10, 2.0, 18))
s_vol_old = s_vol_old.mask((vr >= 1.0) & (vr < 1.5), _linear_map(vr, 1.0, 2, 1.5, 10))

s_tr_old = zeros.copy()
s_tr_old = s_tr_old.mask((turnover_rate >= 3) & (turnover_rate <= 10), 15.0)
s_tr_old = s_tr_old.mask((turnover_rate > 10) & (turnover_rate <= 15),
                         _linear_map(turnover_rate, 10, 15, 15, 5))
s_tr_old = s_tr_old.mask((turnover_rate >= 1) & (turnover_rate < 3),
                         _linear_map(turnover_rate, 1, 2, 3, 15))

s_pct_old = zeros.copy()
s_pct_old = s_pct_old.mask((pct_chg >= 2) & (pct_chg <= 5), 25.0)
s_pct_old = s_pct_old.mask((pct_chg >= 0) & (pct_chg < 2),
                           _linear_map(pct_chg, 0, 6, 2, 25))
s_pct_old = s_pct_old.mask((pct_chg > 5) & (pct_chg <= 7),
                           _linear_map(pct_chg, 5, 25, 7, 10))
s_pct_old = s_pct_old.mask((pct_chg > 7) & (pct_chg <= 9),
                           _linear_map(pct_chg, 7, 10, 9, 3))

old_total = s_inflow_old + s_vol_old + s_tr_old + s_pct_old
# OLD: net outflow penalty (-10) and volume not amplifier-gated
old_total.loc[inflow_rate < 0] = (old_total - 10).clip(0, 100)
old_total.loc[turnover_rate < 1] = 0.0
old_total.loc[pct_chg > 9] = 0.0
old_total = old_total.clip(0, 100)

# ── Per-component comparison ──
print(f"\n{'='*80}")
print("COMPONENT-LEVEL COMPARISON (avg per stock)")
print(f"{'='*80}")
print(f"{'Component':<20s} {'Old Avg':>8s} {'New Avg':>8s} {'Delta':>8s}")
print(f"{'-'*44}")

new_signals = factor._compute_signals(df)
new_inflow = new_signals.get("inflow", zeros)
new_tr = new_signals.get("turnover", zeros)
new_pct = new_signals.get("pct_chg", zeros)

print(f"{'inflow':<20s} {s_inflow_old.mean():>8.1f} {new_inflow.mean():>8.1f} {new_inflow.mean()-s_inflow_old.mean():>+8.1f}")
print(f"{'vol_ratio (old only)':<20s} {s_vol_old.mean():>8.1f} {'N/A':>8s} {'--':>8s}")
print(f"{'turnover':<20s} {s_tr_old.mean():>8.1f} {new_tr.mean():>8.1f} {new_tr.mean()-s_tr_old.mean():>+8.1f}")
print(f"{'pct_chg':<20s} {s_pct_old.mean():>8.1f} {new_pct.mean():>8.1f} {new_pct.mean()-s_pct_old.mean():>+8.1f}")
print(f"{'TOTAL (pre-clip)':<20s} {old_total.mean():>8.1f} {new_scores.mean():>8.1f} {new_scores.mean()-old_total.mean():>+8.1f}")

# ── Score distribution ──
print(f"\n{'='*80}")
print("SCORE DISTRIBUTION")
print(f"{'='*80}")
print(f"{'':<20s} {'Old':>8s} {'New':>8s}")
print(f"{'All avg':<20s} {old_total.mean():>8.1f} {new_scores.mean():>8.1f}")
print(f"{'All median':<20s} {old_total.median():>8.1f} {new_scores.median():>8.1f}")
print(f"{'All max':<20s} {old_total.max():>8.1f} {new_scores.max():>8.1f}")
print(f"{'Inflow>0 avg':<20s} {old_total[inflow_rate>0].mean():>8.1f} {new_scores[inflow_rate>0].mean():>8.1f}")
print(f"{'Inflow<0 avg':<20s} {old_total[inflow_rate<0].mean():>8.1f} {new_scores[inflow_rate<0].mean():>8.1f}")

# ── Outflow + high volume ──
outflow_highvol = df[(inflow_rate < 0) & (volume_ratio >= 1.5)]
print(f"\n{'='*80}")
print(f"OUTFLOW + HIGH VOLUME (>=1.5x): {len(outflow_highvol)} stocks")
print(f"{'='*80}")

if len(outflow_highvol) > 0:
    ohv_compare = pd.DataFrame({
        "old": old_total.loc[outflow_highvol.index],
        "new": new_scores.loc[outflow_highvol.index],
        "ir": inflow_rate.loc[outflow_highvol.index],
        "vr": volume_ratio.loc[outflow_highvol.index],
        "pct": pct_chg.loc[outflow_highvol.index],
        "name": df.get("name", pd.Series("", index=idx)).loc[outflow_highvol.index],
    }).sort_values("old", ascending=False)

    print(f"  Old avg: {ohv_compare['old'].mean():.1f}, New avg: {ohv_compare['new'].mean():.1f}, Delta: {ohv_compare['new'].mean() - ohv_compare['old'].mean():+.1f}")
    print(f"  Top 10 (by old score):")
    for i, (c, r) in enumerate(ohv_compare.head(10).iterrows()):
        print(f"  {i+1:2d}. {c} {r['name']:8s} ir={r['ir']:.3f} vr={r['vr']:.1f}x pct={r['pct']:.1f}% old={r['old']:.1f} new={r['new']:.1f} Δ={r['new']-r['old']:+.1f}")

# ── Top 20 changes ──
print(f"\n{'='*80}")
print("TOP 20 — OLD vs NEW")
print(f"{'='*80}")

old_top20 = old_total.nlargest(20)
new_top20 = new_scores.nlargest(20)

print(f"\n{'Rank':<5s} {'Old Code':<14s} {'Old':>5s} {'New Code':<14s} {'New':>5s}")
for i in range(20):
    oc = old_top20.index[i] if i < len(old_top20) else ""
    os_ = old_top20.iloc[i] if i < len(old_top20) else 0
    nc = new_top20.index[i] if i < len(new_top20) else ""
    ns = new_top20.iloc[i] if i < len(new_top20) else 0
    print(f"{i+1:<5d} {str(oc):<14s} {os_:>5.0f} {str(nc):<14s} {ns:>5.0f}")

# ── Biggest changes ──
print(f"\n{'='*80}")
print("BIGGEST SCORE CHANGES (New - Old)")
print(f"{'='*80}")

deltas = (new_scores - old_total).sort_values()
print(f"\n  Top 10 drops:")
for c in deltas.head(10).index:
    nm = str(df.loc[c, "name"]) if "name" in df.columns else ""
    ir = inflow_rate.get(c, 0)
    vr = volume_ratio.get(c, 1)
    print(f"  {c} {nm:8s} ir={ir:.3f} vr={vr:.1f}x old={old_total[c]:.1f} new={new_scores[c]:.1f} Δ={deltas[c]:+.1f}")

print(f"\n  Top 10 gains:")
for c in deltas.nlargest(10).index:
    nm = str(df.loc[c, "name"]) if "name" in df.columns else ""
    ir = inflow_rate.get(c, 0)
    vr = volume_ratio.get(c, 1)
    print(f"  {c} {nm:8s} ir={ir:.3f} vr={vr:.1f}x old={old_total[c]:.1f} new={new_scores[c]:.1f} Δ={deltas[c]:+.1f}")
