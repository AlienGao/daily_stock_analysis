# -*- coding: utf-8 -*-
"""Compare old vs new SectorFactor scoring using real DB data."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from datetime import date

from src.storage import DatabaseManager

db = DatabaseManager()
trade_date = date.today().strftime("%Y%m%d")
print(f"Trade date: {trade_date}")

lp = db.get_limit_pool(trade_date=trade_date, min_pct_chg=9.5)
print(f"limit_pool: {len(lp) if lp is not None else 0} stocks")

spot = db.get_realtime_spot()
print(f"realtime_spot: {len(spot) if spot is not None else 0} stocks")

if lp is None or lp.empty:
    print("No limit_pool data, exiting")
    sys.exit(1)

from src.discovery.factors.sector_factor import SectorFactor as SF

factor = SF()

# ── Momentum (shared) ──
momentum = factor._compute_intraday_momentum(trade_date)
print(f"momentum: {len(momentum)} stocks")

# ── NEW (current code: expanded fetch_data, new weights) ──
print("\n=== NEW ===")
new_df = factor.fetch_data(trade_date)
if new_df is None or new_df.empty:
    print("fetch_data returned None/empty")
    sys.exit(1)
print(f"fetch_data: {len(new_df)} stocks")
new_scores = factor.score(new_df, trade_date=trade_date)
new_signals = factor._compute_signals(new_df, momentum_series=momentum)

# ── OLD (limit_pool only, old weights) ──
print("\n=== OLD ===")
old_df = lp.reset_index().copy()
old_df = SF._with_ts_code_index(old_df.set_index("code"))
print(f"fetch_data (old): {len(old_df)} stocks")

# Build old-style signals manually
idx = old_df.index
sector_col = "sector" if "sector" in old_df.columns else None
lt_col = "limit_times" if "limit_times" in old_df.columns else None
pct_col = "pct_chg" if "pct_chg" in old_df.columns else None
seal_col = "first_seal_time" if "first_seal_time" in old_df.columns else None
last_seal_col = "last_seal_time" if "last_seal_time" in old_df.columns else None
break_col = "break_count" if "break_count" in old_df.columns else None
sa_col = "seal_amount" if "seal_amount" in old_df.columns else None
cap_col = "float_market_cap" if "float_market_cap" in old_df.columns else None

# chain: old clip 0-60
s_chain_old = pd.Series(0.0, index=idx)
if lt_col:
    lt = old_df[lt_col].fillna(0).clip(0, 5)
    s_chain_old = lt.map({0: 0, 1: 10, 2: 20, 3: 27, 4: 32, 5: 35}).clip(0, 60)
elif pct_col:
    s_chain_old = (old_df[pct_col].fillna(0).clip(0, 10) * 3.5).clip(0, 60)

# sector_heat: old clip 0-20 with *20 not *30
if sector_col and factor._sector_history:
    sec = old_df[sector_col].fillna("").astype(str).mask(lambda x: x.str.strip() == "")
    today_cnts = sec.groupby(sec).transform("count")
    mean_map = pd.Series({k: v[0] for k, v in factor._sector_history.items()})
    std_map = pd.Series({k: v[1] for k, v in factor._sector_history.items()})
    sector_mean = sec.map(mean_map)
    sector_std = sec.map(std_map)
    z = (today_cnts - sector_mean) / sector_std.where(sector_std > 0, 1.0)
    z = z.mask(sector_std <= 0,
               pd.Series(0.0, index=z.index)
               .mask(today_cnts > sector_mean, 2.0)
               .mask(today_cnts < sector_mean, -1.0))
    s_sector_old = ((z + 1) / 3 * 20).clip(0, 20)
    s_sector_old = s_sector_old.where(sec.isin(mean_map.index), 10.0)
else:
    s_sector_old = pd.Series(10.0, index=idx)

# seal_time: same as new
s_seal_old = pd.Series(0.0, index=idx)
if seal_col:
    def _seal_score(raw):
        try:
            s = str(raw).strip()
            if ":" in s:
                parts = s.split(":")
            elif len(s) >= 4:
                parts = [s[:2], s[2:4]]
            elif s.isdigit():
                s = s.zfill(4)
                parts = [s[:2], s[2:4]]
            else:
                return 0
            if len(parts) < 2:
                return 0
            mins = int(parts[0]) * 60 + int(parts[1]) - 570
            if mins < 0:
                return 15
            if mins > 240:
                return 0
            return max(0, 15 - mins / 16)
        except (ValueError, TypeError):
            return 0
    s_seal_old = old_df[seal_col].apply(_seal_score).clip(0, 15)

# seal_quality: same as new
s_qual_old = pd.Series(0.0, index=idx)
for i in idx:
    sc = 0.0
    if break_col:
        bc = int(old_df[break_col].get(i, 0) or 0)
        sc += {0: 5, 1: 3, 2: 1}.get(bc, 0)
    if seal_col and last_seal_col:
        gap = SF._seal_gap_minutes(
            str(old_df[seal_col].get(i, "")),
            str(old_df[last_seal_col].get(i, "")),
        )
        if gap > 15:
            sc -= 3
        elif gap > 5:
            sc -= 2
        elif gap > 0:
            sc -= 1
    if sa_col:
        sa = float(old_df[sa_col].get(i, 0) or 0)
        if sa > 0 and cap_col:
            cv = float(old_df[cap_col].get(i, 0) or 0)
            if cv > 0:
                ratio = sa / cv
                if ratio >= 0.05:
                    sc += 5
                elif ratio >= 0.02:
                    sc += 3
                elif ratio >= 0.01:
                    sc += 2
                elif ratio >= 0.005:
                    sc += 1
        elif sa > 0:
            sc += 1
    s_qual_old.loc[i] = max(0.0, min(10.0, sc))

# momentum: old clip 0-30
if momentum is not None and not momentum.empty:
    bare_map = momentum.copy()
    bare_map.index = bare_map.index.astype(str).str.strip().str.zfill(6)
    bare_from_ts = pd.Index([
        str(x).split(".")[0] if "." in str(x) else str(x).strip().zfill(6)
        for x in idx
    ])
    s_mom_old = pd.Series(
        [bare_map.get(c, 0.0) for c in bare_from_ts],
        index=idx,
    ).fillna(0).clip(0, 30)
else:
    s_mom_old = pd.Series(0.0, index=idx)

old_signals = {
    "chain": s_chain_old,
    "sector_heat": s_sector_old,
    "seal_time": s_seal_old,
    "seal_quality": s_qual_old,
    "intraday_momentum": s_mom_old,
}
old_scores = sum(old_signals.values()).clip(0, 100)

# ── Comparison ──
print("\n" + "=" * 80)
print("SCORE COMPARISON: Old vs New")
print("=" * 80)

print(f"\n--- Overall ---")
print(f"Old: {len(old_scores)} stocks, avg={old_scores.mean():.1f}, median={old_scores.median():.1f}, max={old_scores.max():.1f}")
print(f"New: {len(new_scores)} stocks, avg={new_scores.mean():.1f}, median={new_scores.median():.1f}, max={new_scores.max():.1f}")

common_idx = old_scores.index.intersection(new_scores.index)
print(f"Common: {len(common_idx)} stocks")

print(f"\n--- Per-component avg (common stocks) ---")
for key in ["chain", "sector_heat", "seal_time", "seal_quality", "intraday_momentum"]:
    oa = old_signals[key].loc[common_idx].mean()
    na = new_signals[key].loc[common_idx].mean()
    print(f"  {key:25s}: old={oa:5.1f}  new={na:5.1f}  Δ={na-oa:+.1f}")

# ZT score change
zt_mask = new_df["limit_times"].fillna(0) > 0 if "limit_times" in new_df.columns else pd.Series(False, index=new_df.index)
zt_common = new_df.index[zt_mask].intersection(old_scores.index)
print(f"\n--- ZT common ({len(zt_common)} stocks) ---")
if len(zt_common) > 0:
    print(f"  Old avg: {old_scores.loc[zt_common].mean():.1f}, New avg: {new_scores.loc[zt_common].mean():.1f}, Δ: {new_scores.loc[zt_common].mean() - old_scores.loc[zt_common].mean():+.1f}")
    up5 = (new_scores.loc[zt_common] - old_scores.loc[zt_common] > 5).sum()
    down5 = (new_scores.loc[zt_common] - old_scores.loc[zt_common] < -5).sum()
    print(f"  Up >5: {up5}, Down >5: {down5}")

# Non-ZT
non_zt = new_df.index[~zt_mask]
print(f"\n--- Non-ZT new ({len(non_zt)} stocks) ---")
if len(non_zt) > 0:
    nz = new_scores.loc[non_zt]
    print(f"  Avg: {nz.mean():.1f}, Median: {nz.median():.1f}, Max: {nz.max():.1f}")
    print(f"  Top 10:")
    for i, (c, s) in enumerate(nz.nlargest(10).items()):
        nm = str(new_df.loc[c, "name"]) if "name" in new_df.columns else ""
        sec = str(new_df.loc[c, "sector"]) if "sector" in new_df.columns else ""
        print(f"    {i+1}. {c} {nm:8s} {sec:12s} score={s:.1f}")

# Top 20
print(f"\n--- Old Top 20 ---")
old_top20 = old_scores.nlargest(20)
for i, (c, s) in enumerate(old_top20.items()):
    nm = str(old_df.loc[c, "name"]) if c in old_df.index and "name" in old_df.columns else ""
    lt = int(old_df.loc[c, "limit_times"]) if c in old_df.index and "limit_times" in old_df.columns else 0
    in_new = "Y" if c in new_scores.nlargest(20).index else "N"
    print(f"  {i+1:2d}. {c} {nm:8s} lt={lt} old={s:.1f} in_new20={in_new}")

print(f"\n--- New Top 20 ---")
new_top20 = new_scores.nlargest(20)
for i, (c, s) in enumerate(new_top20.items()):
    nm = str(new_df.loc[c, "name"]) if c in new_df.index and "name" in new_df.columns else ""
    lt = int(new_df.loc[c, "limit_times"]) if c in new_df.index and "limit_times" in new_df.columns else 0
    is_zt = "ZT" if lt > 0 else "NZT"
    old_s = old_scores.get(c, 0)
    tag = "NEW" if c not in old_top20.index else ""
    print(f"  {i+1:2d}. {c} {nm:8s} {is_zt:3s} new={s:.1f} old={old_s:.1f} Δ={s-old_s:+.1f} {tag}")

# New entries
new_entries = new_top20.index.difference(old_top20.index)
print(f"\n--- New entries in Top 20: {len(new_entries)} ---")
for c in new_entries:
    nm = str(new_df.loc[c, "name"]) if "name" in new_df.columns else ""
    lt = int(new_df.loc[c, "limit_times"]) if "limit_times" in new_df.columns else 0
    is_zt = "ZT" if lt > 0 else "NZT"
    print(f"  {c} {nm:8s} {is_zt} score={new_scores[c]:.1f}")

# Dropped
dropped = old_top20.index.difference(new_top20.index)
print(f"\n--- Dropped from Top 20: {len(dropped)} ---")
for c in dropped:
    nm = str(old_df.loc[c, "name"]) if c in old_df.index and "name" in old_df.columns else ""
    print(f"  {c} {nm:8s} old={old_scores[c]:.1f} new={new_scores.get(c, 0):.1f}")

# Non-ZT in Top 20
nzt_top20 = [c for c in new_top20.index if c in non_zt]
print(f"\n--- Non-ZT in New Top 20: {len(nzt_top20)} ---")
for c in nzt_top20:
    nm = str(new_df.loc[c, "name"]) if "name" in new_df.columns else ""
    sec = str(new_df.loc[c, "sector"]) if "sector" in new_df.columns else ""
    print(f"  {c} {nm:8s} {sec:12s} score={new_scores[c]:.1f}")
