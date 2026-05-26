# 因子子集搜索报告 (fixed fwd3d)

搜索时间: 2026-05-26T23:50:10.814964
耗时: 1213.0s

## 基线 (全部因子)
- 因子数: 24
- 日均收益: 0.0064
- Rank IC: 0.0162 (±0.0278)
- ICIR: 0.5814
- CV RMSE: 0.0562

## 因子重要性排名
| 排名 | 因子 | Gain |
|------|------|------|
| 1 | broker_recommend | 50.7 |
| 2 | alpha042 | 49.6 |
| 3 | vwap_reversal | 26.7 |
| 4 | margin | 23.4 |
| 5 | technical | 21.0 |
| 6 | chip | 20.9 |
| 7 | buyback | 20.2 |
| 8 | fundamental | 18.8 |
| 9 | gtja114 | 18.0 |
| 10 | ranking_momentum | 17.9 |
| 11 | liquid_oversold | 15.1 |
| 12 | gap_reversal | 14.2 |
| 13 | alpha60 | 14.0 |
| 14 | money_flow_osc | 13.7 |
| 15 | money_flow | 12.1 |
| 16 | vwap_deviation | 10.8 |
| 17 | limit | 3.1 |
| 18 | hot_money | 2.1 |
| 19 | concept_heat | 0.0 |
| 20 | insider_buy | 0.0 |
| 21 | institution_hold | 0.0 |
| 22 | performance | 0.0 |
| 23 | popularity | 0.0 |
| 24 | profit_forecast | 0.0 |

## 贪心前向选择
| 轮次 | 添加因子 | 日均收益 | Rank IC | 改善 |
|------|----------|----------|---------|------|
| 1 | margin | 0.0096 | 0.0075 | Y |
| 2 | gtja114 | 0.0134 | 0.0132 | Y |
| 3 | chip | 0.0156 | 0.0249 | Y |
| 4 | concept_heat | 0.0156 | 0.0249 | N |
| 5 | insider_buy | 0.0156 | 0.0249 | N |
| 6 | institution_hold | 0.0156 | 0.0249 | N |

## Optuna TPE 精调 (80 trials)
- 最优因子数: 11
- 日均收益: 0.0128
- Rank IC: 0.0281

## 最终结果
- 最优因子数: 3
- 日均收益: 0.0156
- Rank IC: 0.0249
- ICIR: 1.6189
- CV RMSE: 0.0560
- 相比基线收益变化: +0.0092

### 最优因子 (3 个)
- margin
- gtja114
- chip

### 排除因子 (21 个)
- alpha042
- alpha60
- broker_recommend
- buyback
- concept_heat
- fundamental
- gap_reversal
- hot_money
- insider_buy
- institution_hold
- limit
- liquid_oversold
- money_flow
- money_flow_osc
- performance
- popularity
- profit_forecast
- ranking_momentum
- technical
- vwap_deviation
- vwap_reversal