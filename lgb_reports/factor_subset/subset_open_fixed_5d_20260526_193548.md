# 因子子集搜索报告 (fixed fwd5d)

搜索时间: 2026-05-26T19:35:48.296012
耗时: 1201.2s

## 基线 (全部因子)
- 因子数: 24
- 日均收益: 0.0341
- Rank IC: 0.0276 (±0.0244)
- ICIR: 1.1280
- CV RMSE: 0.0774

## 因子重要性排名
| 排名 | 因子 | Gain |
|------|------|------|
| 1 | technical | 0.0 |
| 2 | broker_recommend | 0.0 |
| 3 | margin | 0.0 |
| 4 | chip | 0.0 |
| 5 | vwap_reversal | 0.0 |
| 6 | buyback | 0.0 |
| 7 | ranking_momentum | 0.0 |
| 8 | fundamental | 0.0 |
| 9 | alpha042 | 0.0 |
| 10 | money_flow_osc | 0.0 |
| 11 | gtja114 | 0.0 |
| 12 | liquid_oversold | 0.0 |
| 13 | gap_reversal | 0.0 |
| 14 | vwap_deviation | 0.0 |
| 15 | money_flow | 0.0 |
| 16 | alpha60 | 0.0 |
| 17 | hot_money | 0.0 |
| 18 | limit | 0.0 |
| 19 | concept_heat | 0.0 |
| 20 | insider_buy | 0.0 |
| 21 | institution_hold | 0.0 |
| 22 | performance | 0.0 |
| 23 | popularity | 0.0 |
| 24 | profit_forecast | 0.0 |

## 贪心前向选择
| 轮次 | 添加因子 | 日均收益 | Rank IC | 改善 |
|------|----------|----------|---------|------|
| 1 | chip | 0.0301 | 0.0239 | Y |
| 2 | margin | 0.0306 | 0.0195 | Y |
| 3 | ranking_momentum | 0.0338 | 0.0245 | Y |
| 4 | hot_money | 0.0383 | 0.0279 | Y |
| 5 | concept_heat | 0.0383 | 0.0279 | N |
| 6 | insider_buy | 0.0383 | 0.0279 | N |
| 7 | institution_hold | 0.0383 | 0.0279 | N |

## Optuna TPE 精调 (80 trials)
- 最优因子数: 12
- 日均收益: 0.0397
- Rank IC: 0.0278

## 最终结果
- 最优因子数: 12
- 日均收益: 0.0397
- Rank IC: 0.0278
- ICIR: 0.7653
- CV RMSE: 0.0774
- 相比基线收益变化: +0.0056

### 最优因子 (12 个)
- broker_recommend
- chip
- concept_heat
- hot_money
- insider_buy
- limit
- margin
- money_flow
- popularity
- profit_forecast
- ranking_momentum
- vwap_deviation

### 排除因子 (12 个)
- alpha042
- alpha60
- buyback
- fundamental
- gap_reversal
- gtja114
- institution_hold
- liquid_oversold
- money_flow_osc
- performance
- technical
- vwap_reversal