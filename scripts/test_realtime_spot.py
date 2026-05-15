#!/usr/bin/env python3
"""Test script for RealtimeSpotProvider — 验证腾讯→新浪→东财补充全流程。"""
import sys
import time
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
pd.set_option("display.max_columns", 20)
pd.set_option("display.width", 200)

from src.discovery.realtime_spot import RealtimeSpotProvider

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = ""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  OK  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  <- {detail}")


# ── 1. Code list ──────────────────────────────────────────────
print("\n=== 1. 代码列表 ===")
codes = RealtimeSpotProvider._get_code_list()
check("代码列表非空", len(codes) > 0, f"got {len(codes)}")
check("代码列表 > 5000", len(codes) > 5000, f"got {len(codes)}")
check("代码格式 6 位数字", all(len(str(c)) >= 6 for c in codes[:100]))
tx_all = RealtimeSpotProvider._to_tencent_codes(codes)
sh_cnt = sum(1 for c in tx_all if c.startswith("sh"))
sz_cnt = sum(1 for c in tx_all if c.startswith("sz"))
bj_cnt = sum(1 for c in tx_all if c.startswith("bj"))
check("腾讯格式有 sh/sz/bj 前缀", sh_cnt > 0 and sz_cnt > 0 and bj_cnt > 0,
      f"sh={sh_cnt}, sz={sz_cnt}, bj={bj_cnt}")
check("腾讯代码前缀完整", (sh_cnt + sz_cnt + bj_cnt) == len(tx_all))
print(f"  代码总数: {len(codes)} (sh={sh_cnt}, sz={sz_cnt}, bj={bj_cnt})")

# ── 2. Tencent live ───────────────────────────────────────────
print("\n=== 2. 腾讯实时行情 (live) ===")
t0 = time.time()
df_tx = RealtimeSpotProvider._fetch_tencent()
tx_elapsed = time.time() - t0
if df_tx is not None and not df_tx.empty:
    check("腾讯返回非空", True)
    check("腾讯返回 > 5000 只", len(df_tx) > 5000, f"got {len(df_tx)}")
    check("腾讯耗时 < 5s", tx_elapsed < 5, f"{tx_elapsed:.1f}s")
    check("腾讯有 code/name/price/pct_chg", all(
        c in df_tx.columns for c in ["code", "name", "price", "pct_chg"]
    ))
    check("腾讯名称非空", df_tx["name"].notna().sum() > 1000,
          f"{df_tx['name'].notna().sum()}/{len(df_tx)}")
    check("腾讯名称不是代码",
          not (df_tx["name"].iloc[:50].values == df_tx["code"].iloc[:50].values).all(),
          f"sample: {list(zip(df_tx['code'].iloc[:5], df_tx['name'].iloc[:5]))}")
    # 腾讯实际上提供 turnover_rate 和 volume_ratio
    check("腾讯有 turnover_rate 列", "turnover_rate" in df_tx.columns)
    check("腾讯有 volume_ratio 列", "volume_ratio" in df_tx.columns)
    print(f"  返回 {len(df_tx)} 只, 耗时 {tx_elapsed:.1f}s")
    print(f"  sample:\n{df_tx[['code','name','price','pct_chg','turnover_rate']].head(3)}")
else:
    check("腾讯返回非空", False, f"df={type(df_tx)}")

# ── 3. Sina live ──────────────────────────────────────────────
print("\n=== 3. 新浪实时行情 (live) ===")
t0 = time.time()
df_sina = RealtimeSpotProvider._fetch_sina()
sina_elapsed = time.time() - t0
if df_sina is not None and not df_sina.empty:
    check("新浪返回非空", True)
    check("新浪返回 > 5000 只", len(df_sina) > 5000, f"got {len(df_sina)}")
    check("新浪耗时 < 10s", sina_elapsed < 10, f"{sina_elapsed:.1f}s")
    # Key fix: turnover_rate/volume_ratio should be pd.NA (not 0.0)
    tr_is_na = df_sina["turnover_rate"].isna().all()
    vr_is_na = df_sina["volume_ratio"].isna().all()
    check("新浪 turnover_rate 全为 pd.NA", tr_is_na,
          f"NA={df_sina['turnover_rate'].isna().sum()} "
          f"zero={(df_sina['turnover_rate']==0).sum()}")
    check("新浪 volume_ratio 全为 pd.NA", vr_is_na,
          f"NA={df_sina['volume_ratio'].isna().sum()} "
          f"zero={(df_sina['volume_ratio']==0).sum()}")
    check("新浪名称非空", df_sina["name"].notna().sum() > 1000)
    print(f"  返回 {len(df_sina)} 只, 耗时 {sina_elapsed:.1f}s")
else:
    check("新浪返回非空", False, f"df={type(df_sina)}")

# ── 4. Normalize ───────────────────────────────────────────────
print("\n=== 4. _normalize 标准化 ===")
if df_tx is not None and not df_tx.empty:
    norm = RealtimeSpotProvider._normalize(df_tx.copy(), "tencent")
    check("code 无交易所前缀", not any(
        str(i).startswith(("sh","sz","bj")) for i in norm.index[:50]
    ))
    check("有 turnover_rate/volume_ratio 列", all(
        c in norm.columns for c in ["price", "turnover_rate", "volume_ratio", "source"]
    ))
    check("source=tencent", (norm["source"] == "tencent").all())
    check("price > 0 (过滤停牌)", (norm["price"] > 0).all())
    check("code 是 index", norm.index.name == "code")

    # Normalize Sina too, check code is index
    if df_sina is not None and not df_sina.empty:
        sina_norm = RealtimeSpotProvider._normalize(df_sina.copy(), "sina")
        check("新浪 normalize 后 code 是 index", sina_norm.index.name == "code")
        check("新浪 normalize 后 code 无前缀", not any(
            str(i).startswith(("sh","sz","bj")) for i in sina_norm.index[:50]
        ))
        # Compare code overlap after normalize
        tx_idx = set(norm.index)
        sina_idx = set(sina_norm.index)
        overlap = tx_idx & sina_idx
        check("两源交集 > 5000", len(overlap) > 5000, f"got {len(overlap)}")
        if overlap:
            sc = list(overlap)[0]
            tp = norm.loc[sc, "price"]
            sp = sina_norm.loc[sc, "price"]
            check(f"同股价格接近 ({sc})", abs(float(tp) - float(sp)) < 0.2,
                  f"tencent={tp}, sina={sp}")
    print(f"  normalize 后: {len(norm)} 只")
else:
    check("normalize (skip)", False, "data unavailable")

# ── 5. EastMoney live (3 pages only) ──────────────────────────
print("\n=== 5. 东财实时行情 (live, 3页) ===")
t0 = time.time()
df_em = RealtimeSpotProvider._fetch_eastmoney(max_pages=3)
em_elapsed = time.time() - t0
if df_em is not None and not df_em.empty:
    check("东财返回非空", True)
    check("东财有 f8/f10/f12/f2 列", all(
        c in df_em.columns for c in ["f8", "f10", "f12", "f2"]
    ), str(list(df_em.columns)[:15]))
    check("东财耗时 < 5s (3页)", em_elapsed < 5, f"{em_elapsed:.1f}s")
    has_tr = pd.to_numeric(df_em["f8"], errors="coerce").notna().sum()
    check("东财换手率 f8 有值", has_tr > 0, f"got {has_tr}")
    print(f"  返回 {len(df_em)} 只, 耗时 {em_elapsed:.1f}s")
else:
    check("东财返回非空", False, f"df={type(df_em)}")

# ── 6. 东财补充换手率/量比（核心测试）──────────────────────
print("\n=== 6. 东财补充 (supplement) — 核心测试 ===")
if df_sina is not None and not df_sina.empty and df_em is not None and not df_em.empty:
    provider = RealtimeSpotProvider()
    # 模拟完整流程: Sina 数据 → normalize (code 变 index) → supplement
    sina_norm = RealtimeSpotProvider._normalize(df_sina.copy(), "sina")
    check("normalize 后 code 在 index", sina_norm.index.name == "code")
    before_tr = sina_norm["turnover_rate"].isna().sum()
    before_vr = sina_norm["volume_ratio"].isna().sum()
    check("补充前 turnover_rate 全 NA", before_tr == len(sina_norm))
    check("补充前 volume_ratio 全 NA", before_vr == len(sina_norm))

    # 这次 supplement 应该能通过 index 匹配了
    RealtimeSpotProvider._em_supplement_ts = 0
    provider._supplement_eastmoney(sina_norm)
    after_tr = sina_norm["turnover_rate"].notna().sum()
    after_vr = sina_norm["volume_ratio"].notna().sum()
    check("补充后 turnover_rate 有值", after_tr > 0, f"filled {after_tr}/{len(sina_norm)}")
    check("补充后 volume_ratio 有值", after_vr > 0, f"filled {after_vr}/{len(sina_norm)}")
    tr_gt0 = (sina_norm["turnover_rate"] > 0).sum()
    check("有真实 turnover_rate > 0", tr_gt0 > 0, f"nonzero={tr_gt0}")
    print(f"  turnover_rate: {after_tr} filled, sample={sina_norm['turnover_rate'].dropna().head(5).tolist()}")
    print(f"  volume_ratio: {after_vr} filled")
else:
    check("东财补充 (skip)", False, "data unavailable")

# ── 7. 完整 fetch() 流程 ──────────────────────────────────────
print("\n=== 7. 完整 fetch() 流程 (live) ===")
provider = RealtimeSpotProvider()
provider._cache = {"data": None, "slot": -1, "source": ""}
t0 = time.time()
df = provider.fetch()
elapsed = time.time() - t0
if df is not None and not df.empty:
    check("fetch() 非空", True)
    check("fetch() > 5000 只", len(df) > 5000, f"got {len(df)}")
    check("fetch() 耗时 < 10s", elapsed < 10, f"{elapsed:.1f}s")
    check("fetch() 有完整列", all(
        c in df.columns for c in ["name","price","pct_chg","turnover_rate","volume_ratio","source"]
    ))
    check("fetch() index=code", df.index.name == "code")
    check("fetch() 缓存已设置", provider._cache["data"] is not None)
    check("fetch() source=tencent 或 sina",
          provider._cache["source"] in ("tencent", "sina"))

    # cache hit
    df2 = provider.fetch()
    check("同 slot 缓存命中", df2 is df, f"same={df2 is df}")
    print(f"  {len(df)} 只, source={provider._cache['source']}, {elapsed:.1f}s")
else:
    check("fetch() 非空", False, f"df={type(df)}")

# ── 8. 东财 60s 间隔验证 ──────────────────────────────────────
print("\n=== 8. 东财 60s 更新间隔 ===")
ts_before = RealtimeSpotProvider._em_supplement_ts
check("首次 supplement 后 ts > 0", ts_before > 0)

# 立即再调 fetch()，ts 不应变化（<60s 间隔）
if df is not None and not df.empty:
    ts_set = time.time()
    RealtimeSpotProvider._em_supplement_ts = ts_set
    provider._cache = {"data": None, "slot": -1, "source": ""}
    df3 = provider.fetch()
    ts_after = RealtimeSpotProvider._em_supplement_ts
    check("60s 内跳过东财, ts 不变", ts_after == ts_set)
    print(f"  ts_set={ts_set}, ts_after={ts_after}")
    # restore
    RealtimeSpotProvider._em_supplement_ts = 0

# ── Summary ───────────────────────────────────────────────────
print(f"\n{'='*50}")
print(f"结果: {PASS} PASS, {FAIL} FAIL")
if FAIL > 0:
    print("FAIL: 有测试失败，查看上面输出")
    sys.exit(1)
else:
    print("OK: 所有测试通过")
