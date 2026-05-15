# 回测数据覆盖总览

**日期**: 2026-05-15

## 当前回测覆盖

`factor_score_snapshots` 表：2026-03-24 ~ 2026-05-15，共 35 个交易日，22 个因子（16 postmarket + 6 intraday）全覆盖。

唯一缺口：postmarket 的 `technical`、`money_flow`、`hot_money`、`fundamental`、`concept_heat` 缺少今天（2026-05-15）的盘后数据。

---

## 向后扩展到 2024 年的可行性分析

### 可以直接回测到 2024 年或更早的因子

| 因子 | 模式 | 依赖表 | 最早日期 | 覆盖天数 |
|------|------|--------|----------|----------|
| ranking_momentum | post+intra | momentum_snapshot | 2016-05-16 | 2427 |
| margin | post | margin_detail | 2016-05-16 | 2426 |
| fundamental | post | daily_basic | 2016-05-16 | 2426 |
| buyback | post | repurchase | 2016-05-14 | 2879 |
| performance | post | performance_report | 2016-07-14 | 2632 |
| hot_money | post | hm_detail | 2022-08-16 | 904 |
| limit | post | limit_up_history | 2024-01-02 | 571 |
| popularity | post+intra | popularity_rank | 2024-03-20 | 517 |
| technical | post | stock_tech_indicator | 2024-12-31 | 328 |
| ma_entry | intra | stock_tech_indicator | ~2024-12-31 | ~328 |

### 覆盖很短或几乎无历史数据的因子

| 因子 | 模式 | 依赖表 | 最早日期 | 覆盖 | 说明 |
|------|------|--------|----------|------|------|
| insider_buy | post | insider_buy | 2025-11-18 | 14 条数据 | 基本不可用 |
| institution_hold | post | institution_hold | 2026Q1 | 1 个季度 | 此前无数据 |
| broker_recommend | post | broker_recommend_monthly | 2026-01 | 5 个月 | 仅 2026 年 |

### 被依赖表瓶颈卡住的因子

| 因子 | 模式 | 卡点表 | 最早日期 | 覆盖天数 |
|------|------|--------|----------|----------|
| chip | post | broker_enrichment_cyq_perf | 2026-01-30 | 41 天 |
| money_flow | post | money_flow | 2026-03-24 | 34 天 |
| momentum | intra | money_flow | 2026-03-24 | 34 天 |
| sector | intra | limit_pool | 2026-03-24 | 35 天 |
| rebound | intra | limit_pool | 2026-03-24 | 35 天 |
| profit_forecast | post | profit_forecast | 2026-05-11 | 4 天 |
| concept_heat | post | 无本地表 | — | 0（仅实时 API） |

---

## 核心瓶颈：4 张表决定回测时间边界

| 表名 | 覆盖范围 | 堵住的因子 | Tushare API |
|------|----------|------------|-------------|
| money_flow | 34 天 (20260324~) | money_flow, intraday momentum | moneyflow_mths_dc |
| limit_pool | 35 天 (20260324~) | sector, rebound | limit_list_d |
| broker_enrichment_cyq_perf | 41 天 (20260130~) | chip | cyq_perf |
| profit_forecast | 4 天 (20260511~) | profit_forecast | forecast |

---

## 待确认

- `money_flow`、`limit_pool`、`cyq_perf` 对应的 Tushare API 回补可行性与速率限制
- `profit_forecast` 的数据源是否支持历史回补
- `concept_heat` 因子是否需要新建本地缓存表
