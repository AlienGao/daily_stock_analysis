# -*- coding: utf-8 -*-
"""Compare old vs new RankingMomentumFactor scoring with real data."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date
import pandas as pd
import numpy as np

from src.discovery.factors.ranking_momentum_factor import RankingMomentumFactor

factor = RankingMomentumFactor()
trade_date = date.today().strftime("%Y%m%d")
print(f"Trade date: {trade_date}")

df = factor.fetch_data(trade_date)
if df is None or df.empty:
    print("No data, exiting")
    sys.exit(1)

day_cols = sorted(
    [c for c in df.columns if c.startswith("d") and c[1:].isdigit()],
    key=lambda x: int(x[1:]),
)
print(f"Stocks: {len(df)}, day columns: {day_cols}")

# ── NEW scoring (current factor) ──
new_scores = factor.score(df)

# ── OLD scoring (pre-refactor: 3-day polyfit, strict >0, calendar days) ──
idx = df.index
old_scores = pd.Series(0.0, index=idx)

# Compute cross-sectional rank percentiles for each day
rank_pcts: dict = {}
for col in day_cols:
    raw = df[col].dropna()
    if len(raw) < 100:
        rank_pcts[col] = pd.Series(50.0, index=idx)
        continue
    rank_pcts[col] = raw.rank(pct=True) * 100.0
    rank_pcts[col] = rank_pcts[col].reindex(idx)

for ts_code in idx:
    ranks = []
    for col in day_cols:
        v = rank_pcts[col].get(ts_code, np.nan)
        if pd.notna(v):
            ranks.append(v)

    if len(ranks) < 3:  # OLD: hard filter at 3
        continue

    current_pct = ranks[0] if ranks else 50.0

    # OLD slope: 3-day linear polyfit
    n_slope = min(3, len(ranks))
    if n_slope >= 2:
        y = np.array(ranks[:n_slope])
        days = np.arange(n_slope, 0, -1)  # d0→d2 → days 3,2,1
        try:
            slope, _ = np.polyfit(days, y, 1)
        except np.linalg.LinAlgError:
            slope = 0.0
    else:
        slope = 0.0

    if slope > 8:
        slope_score = 40.0
    elif slope > 2:
        slope_score = 10.0 + (slope - 2.0) / 6.0 * 30.0
    elif slope > 0:
        slope_score = slope / 2.0 * 10.0
    else:
        slope_score = 0.0

    # OLD consecutive: strict >0
    consecutive = 0
    for j in range(len(ranks) - 1):
        if ranks[j] > ranks[j + 1]:
            consecutive += 1
        else:
            break

    if consecutive >= 4:
        consec_score = 30.0
    elif consecutive == 3:
        consec_score = 20.0
    elif consecutive == 2:
        consec_score = 10.0
    else:
        consec_score = 0.0

    # OLD position score
    pos_score = min(30.0, current_pct * 0.3)

    total = slope_score + consec_score + pos_score

    d0_pct = df.loc[ts_code, day_cols[0]] if day_cols else 0
    if pd.notna(d0_pct) and d0_pct >= 9.8:
        total = max(0, total - 40)

    total = max(0.0, min(100.0, total))
    old_scores.loc[ts_code] = total

# ── Also compute per-stock slope/consecutive metrics for both ──
old_slopes = {}
old_consecs = {}
new_slopes = {}
new_consecs = {}

for ts_code in idx:
    ranks_old = []
    for col in day_cols:
        v = rank_pcts[col].get(ts_code, np.nan)
        if pd.notna(v):
            ranks_old.append(v)

    # Old slope
    if len(ranks_old) >= 2:
        n_slope = min(3, len(ranks_old))
        y = np.array(ranks_old[:n_slope])
        days_arr = np.arange(n_slope, 0, -1)
        try:
            sl, _ = np.polyfit(days_arr, y, 1)
        except np.linalg.LinAlgError:
            sl = 0.0
    else:
        sl = 0.0
    old_slopes[ts_code] = sl

    # Old consecutive
    cons = 0
    for j in range(len(ranks_old) - 1):
        if ranks_old[j] > ranks_old[j + 1]:
            cons += 1
        else:
            break
    old_consecs[ts_code] = cons

    # New slope & consecutive from cache
    bare = str(ts_code).split(".")[0] if "." in str(ts_code) else str(ts_code)
    info = factor._rank_trend_cache.get(bare, {})
    new_slopes[ts_code] = info.get("slope", 0.0)
    new_consecs[ts_code] = info.get("consecutive", 0)

# ── Score distribution ──
print(f"\n{'='*80}")
print("SCORE DISTRIBUTION")
print(f"{'='*80}")
print(f"{'':<25s} {'Old':>10s} {'New':>10s} {'Delta':>10s}")
print(f"{'All avg':<25s} {old_scores.mean():>10.2f} {new_scores.mean():>10.2f} {new_scores.mean()-old_scores.mean():>+10.2f}")
print(f"{'All median':<25s} {old_scores.median():>10.2f} {new_scores.median():>10.2f} {new_scores.median()-old_scores.median():>+10.2f}")
print(f"{'All max':<25s} {old_scores.max():>10.2f} {new_scores.max():>10.2f} {new_scores.max()-old_scores.max():>+10.2f}")
print(f"{'All std':<25s} {old_scores.std():>10.2f} {new_scores.std():>10.2f} {new_scores.std()-old_scores.std():>+10.2f}")
print(f"{'Count >0':<25s} {(old_scores>0).sum():>10d} {(new_scores>0).sum():>10d} {(new_scores>0).sum()-(old_scores>0).sum():>+10d}")
print(f"{'Count >=50':<25s} {(old_scores>=50).sum():>10d} {(new_scores>=50).sum():>10d} {(new_scores>=50).sum()-(old_scores>=50).sum():>+10d}")

# ── Slope distribution ──
old_sl = pd.Series(old_slopes)
new_sl = pd.Series(new_slopes)
print(f"\n{'='*80}")
print("SLOPE DISTRIBUTION (rank pp per day)")
print(f"{'='*80}")
print(f"{'':<25s} {'Old':>10s} {'New':>10s}")
print(f"{'Slope mean':<25s} {old_sl.mean():>10.2f} {new_sl.mean():>10.2f}")
print(f"{'Slope std':<25s} {old_sl.std():>10.2f} {new_sl.std():>10.2f}")
print(f"{'Slope >2 count':<25s} {(old_sl>2).sum():>10d} {(new_sl>2).sum():>10d}")
print(f"{'Slope >8 count':<25s} {(old_sl>8).sum():>10d} {(new_sl>8).sum():>10d}")
print(f"{'Slope <0 count':<25s} {(old_sl<0).sum():>10d} {(new_sl<0).sum():>10d}")

# ── Consecutive distribution ──
old_cs = pd.Series(old_consecs)
new_cs = pd.Series(new_consecs)
print(f"\n{'='*80}")
print("CONSECUTIVE UP-DAYS DISTRIBUTION")
print(f"{'='*80}")
for v in [0, 1, 2, 3, 4, 5]:
    oc = (old_cs == v).sum()
    nc = (new_cs == v).sum()
    print(f"  {v:>1d} day(s): old={oc:>5d}  new={nc:>5d}  delta={nc-oc:>+5d}")
oc5p = (old_cs >= 5).sum()
nc5p = (new_cs >= 5).sum()
print(f"  >=5:     old={oc5p:>5d}  new={nc5p:>5d}  delta={nc5p-oc5p:>+5d}")

# ── Top 20 ──
print(f"\n{'='*80}")
print("TOP 20 — OLD vs NEW")
print(f"{'='*80}")
old_top20 = old_scores.nlargest(20)
new_top20 = new_scores.nlargest(20)
print(f"\n{'Rank':<5s} {'Old Code':<14s} {'Old':>6s} {'New Code':<14s} {'New':>6s}")
for i in range(20):
    oc = str(old_top20.index[i]) if i < len(old_top20) else ""
    os_ = old_top20.iloc[i] if i < len(old_top20) else 0
    nc = str(new_top20.index[i]) if i < len(new_top20) else ""
    ns = new_top20.iloc[i] if i < len(new_top20) else 0
    print(f"{i+1:<5d} {oc:<14s} {os_:>6.1f} {nc:<14s} {ns:>6.1f}")

# ── Biggest changes ──
print(f"\n{'='*80}")
print("BIGGEST SCORE CHANGES (New - Old)")
print(f"{'='*80}")
deltas = (new_scores - old_scores).sort_values()

print(f"\n  Top 10 drops:")
for c in deltas.head(10).index:
    sl_o = old_sl.get(c, 0)
    sl_n = new_sl.get(c, 0)
    cs_o = old_cs.get(c, 0)
    cs_n = new_cs.get(c, 0)
    print(f"  {c} old={old_scores[c]:.1f} new={new_scores[c]:.1f} Δ={deltas[c]:+.1f} "
          f"slope({sl_o:.1f}→{sl_n:.1f}) consec({cs_o}→{cs_n})")

print(f"\n  Top 10 gains:")
for c in deltas.nlargest(10).index:
    sl_o = old_sl.get(c, 0)
    sl_n = new_sl.get(c, 0)
    cs_o = old_cs.get(c, 0)
    cs_n = new_cs.get(c, 0)
    print(f"  {c} old={old_scores[c]:.1f} new={new_scores[c]:.1f} Δ={deltas[c]:+.1f} "
          f"slope({sl_o:.1f}→{sl_n:.1f}) consec({cs_o}→{cs_n})")

# ── Overlap in top 50 ──
old_top50 = set(old_scores.nlargest(50).index)
new_top50 = set(new_scores.nlargest(50).index)
overlap = old_top50 & new_top50
print(f"\n{'='*80}")
print(f"Top 50 overlap: {len(overlap)}/50 ({len(overlap)/50*100:.0f}%)")
print(f"Only old: {len(old_top50 - new_top50)}")
print(f"Only new: {len(new_top50 - old_top50)}")
