# 因子组合搜索排名（方案 A 权重）

- **因子池**: 盈利预测, 融资融券, 机构持股, 排名动量, 回购, 高管增持, 业绩
- **组合规模**: 3 / 4 / 5 因子
- **回测区间**: 20260101 ~ 最新
- **排序**: 5 日持有期总收益
- **组合数**: 91
- **生成时间**: 2026-05-30 02:44:57

## Top 20（5日总收益）

| 排名 | 规模 | 组合 | 5日收益 | 年化 | Sharpe | 回撤 | 胜率 | 交易数 |
|------|------|------|---------|------|--------|------|------|--------|
| 1 | 3 | 盈利预测+高管增持+业绩 | +204.34% | +1756.95% | +4.91 | 21.96% | 67.4% | 89 |
| 2 | 3 | 盈利预测+回购+高管增持 | +149.90% | +1006.94% | +4.63 | 13.52% | 74.2% | 89 |
| 3 | 4 | 盈利预测+融资融券+机构持股+业绩 | +136.47% | +857.56% | +3.32 | 20.87% | 68.5% | 89 |
| 4 | 5 | 盈利预测+融资融券+机构持股+高管增持+业绩 | +136.47% | +857.56% | +3.32 | 20.87% | 68.5% | 89 |
| 5 | 3 | 机构持股+回购+高管增持 | +126.49% | +755.10% | +2.96 | 20.96% | 64.0% | 89 |
| 6 | 5 | 盈利预测+融资融券+机构持股+回购+业绩 | +102.30% | +535.66% | +2.68 | 21.83% | 64.0% | 89 |
| 7 | 3 | 机构持股+回购+业绩 | +101.11% | +525.88% | +2.91 | 20.87% | 64.0% | 89 |
| 8 | 4 | 机构持股+回购+高管增持+业绩 | +101.11% | +525.88% | +2.91 | 20.87% | 64.0% | 89 |
| 9 | 3 | 盈利预测+融资融券+机构持股 | +96.94% | +492.44% | +2.58 | 21.27% | 62.9% | 89 |
| 10 | 4 | 盈利预测+融资融券+机构持股+高管增持 | +96.94% | +492.44% | +2.58 | 21.27% | 62.9% | 89 |
| 11 | 5 | 盈利预测+融资融券+机构持股+回购+高管增持 | +92.90% | +461.05% | +2.53 | 21.78% | 64.0% | 89 |
| 12 | 4 | 盈利预测+回购+高管增持+业绩 | +91.39% | +449.62% | +2.92 | 16.55% | 59.6% | 89 |
| 13 | 4 | 盈利预测+融资融券+机构持股+回购 | +88.97% | +431.51% | +2.46 | 21.87% | 64.0% | 89 |
| 14 | 3 | 回购+高管增持+业绩 | +87.71% | +422.26% | +3.89 | 8.77% | 68.5% | 89 |
| 15 | 3 | 盈利预测+回购+业绩 | +84.18% | +396.93% | +2.80 | 16.50% | 56.2% | 89 |
| 16 | 3 | 融资融券+高管增持+业绩 | +78.39% | +356.94% | +2.39 | 21.53% | 60.7% | 89 |
| 17 | 4 | 盈利预测+排名动量+回购+高管增持 | +76.39% | +343.58% | +4.37 | 11.78% | 59.6% | 89 |
| 18 | 5 | 盈利预测+排名动量+回购+高管增持+业绩 | +67.56% | +287.63% | +3.49 | 8.95% | 57.3% | 89 |
| 19 | 3 | 盈利预测+融资融券+业绩 | +61.29% | +250.71% | +2.36 | 19.88% | 59.6% | 89 |
| 20 | 4 | 盈利预测+融资融券+高管增持+业绩 | +61.29% | +250.71% | +2.36 | 19.88% | 59.6% | 89 |

## 全部组合

| 排名 | 规模 | 因子 keys | 5日收益 | Sharpe |
|------|------|-----------|---------|--------|
| 1 | 3 | profit_forecast+insider_buy+performance | +204.34% | +4.91 |
| 2 | 3 | profit_forecast+buyback+insider_buy | +149.90% | +4.63 |
| 3 | 4 | profit_forecast+margin+institution_hold+performance | +136.47% | +3.32 |
| 4 | 5 | profit_forecast+margin+institution_hold+insider_buy+performance | +136.47% | +3.32 |
| 5 | 3 | institution_hold+buyback+insider_buy | +126.49% | +2.96 |
| 6 | 5 | profit_forecast+margin+institution_hold+buyback+performance | +102.30% | +2.68 |
| 7 | 3 | institution_hold+buyback+performance | +101.11% | +2.91 |
| 8 | 4 | institution_hold+buyback+insider_buy+performance | +101.11% | +2.91 |
| 9 | 3 | profit_forecast+margin+institution_hold | +96.94% | +2.58 |
| 10 | 4 | profit_forecast+margin+institution_hold+insider_buy | +96.94% | +2.58 |
| 11 | 5 | profit_forecast+margin+institution_hold+buyback+insider_buy | +92.90% | +2.53 |
| 12 | 4 | profit_forecast+buyback+insider_buy+performance | +91.39% | +2.92 |
| 13 | 4 | profit_forecast+margin+institution_hold+buyback | +88.97% | +2.46 |
| 14 | 3 | buyback+insider_buy+performance | +87.71% | +3.89 |
| 15 | 3 | profit_forecast+buyback+performance | +84.18% | +2.80 |
| 16 | 3 | margin+insider_buy+performance | +78.39% | +2.39 |
| 17 | 4 | profit_forecast+ranking_momentum+buyback+insider_buy | +76.39% | +4.37 |
| 18 | 5 | profit_forecast+ranking_momentum+buyback+insider_buy+performance | +67.56% | +3.49 |
| 19 | 3 | profit_forecast+margin+performance | +61.29% | +2.36 |
| 20 | 4 | profit_forecast+margin+insider_buy+performance | +61.29% | +2.36 |
| 21 | 3 | institution_hold+insider_buy+performance | +49.06% | +1.86 |
| 22 | 3 | margin+buyback+insider_buy | +49.04% | +2.33 |
| 23 | 3 | margin+ranking_momentum+insider_buy | +41.98% | +2.33 |
| 24 | 4 | profit_forecast+margin+institution_hold+ranking_momentum | +40.59% | +2.09 |
| 25 | 5 | profit_forecast+margin+institution_hold+ranking_momentum+insider_buy | +40.59% | +2.09 |
| 26 | 3 | profit_forecast+ranking_momentum+buyback | +39.29% | +2.61 |
| 27 | 3 | profit_forecast+institution_hold+performance | +33.31% | +1.72 |
| 28 | 4 | profit_forecast+institution_hold+insider_buy+performance | +33.31% | +1.72 |
| 29 | 5 | margin+institution_hold+ranking_momentum+buyback+performance | +32.61% | +1.83 |
| 30 | 4 | ranking_momentum+buyback+insider_buy+performance | +31.44% | +2.39 |
| 31 | 5 | profit_forecast+margin+institution_hold+ranking_momentum+buyback | +29.62% | +1.45 |
| 32 | 3 | margin+ranking_momentum+performance | +27.40% | +1.69 |
| 33 | 4 | margin+ranking_momentum+insider_buy+performance | +27.40% | +1.69 |
| 34 | 5 | profit_forecast+margin+institution_hold+ranking_momentum+performance | +25.24% | +1.53 |
| 35 | 4 | profit_forecast+ranking_momentum+buyback+performance | +22.89% | +1.49 |
| 36 | 4 | profit_forecast+margin+buyback+performance | +21.63% | +1.26 |
| 37 | 5 | profit_forecast+margin+buyback+insider_buy+performance | +21.63% | +1.26 |
| 38 | 4 | profit_forecast+margin+ranking_momentum+performance | +21.10% | +1.26 |
| 39 | 5 | profit_forecast+margin+ranking_momentum+insider_buy+performance | +21.10% | +1.26 |
| 40 | 3 | institution_hold+ranking_momentum+insider_buy | +20.53% | +1.24 |
| 41 | 3 | profit_forecast+margin+ranking_momentum | +18.35% | +1.11 |
| 42 | 4 | profit_forecast+margin+ranking_momentum+insider_buy | +18.35% | +1.11 |
| 43 | 3 | institution_hold+ranking_momentum+performance | +18.32% | +1.15 |
| 44 | 4 | institution_hold+ranking_momentum+insider_buy+performance | +18.32% | +1.15 |
| 45 | 5 | profit_forecast+margin+ranking_momentum+buyback+performance | +17.74% | +1.17 |
| 46 | 3 | profit_forecast+ranking_momentum+insider_buy | +16.70% | +1.24 |
| 47 | 3 | ranking_momentum+buyback+performance | +16.44% | +1.34 |
| 48 | 3 | ranking_momentum+buyback+insider_buy | +14.42% | +1.38 |
| 49 | 5 | institution_hold+ranking_momentum+buyback+insider_buy+performance | +13.67% | +0.91 |
| 50 | 4 | institution_hold+ranking_momentum+buyback+insider_buy | +12.86% | +0.85 |
| 51 | 3 | profit_forecast+institution_hold+buyback | +12.55% | +0.79 |
| 52 | 3 | profit_forecast+institution_hold+insider_buy | +12.55% | +0.79 |
| 53 | 4 | profit_forecast+institution_hold+buyback+insider_buy | +12.55% | +0.79 |
| 54 | 4 | profit_forecast+institution_hold+buyback+performance | +12.55% | +0.79 |
| 55 | 5 | profit_forecast+institution_hold+buyback+insider_buy+performance | +12.55% | +0.79 |
| 56 | 4 | margin+institution_hold+ranking_momentum+buyback | +11.56% | +0.81 |
| 57 | 5 | margin+institution_hold+ranking_momentum+buyback+insider_buy | +11.56% | +0.81 |
| 58 | 4 | institution_hold+ranking_momentum+buyback+performance | +11.10% | +0.78 |
| 59 | 3 | profit_forecast+institution_hold+ranking_momentum | +10.79% | +0.75 |
| 60 | 4 | profit_forecast+institution_hold+ranking_momentum+insider_buy | +10.79% | +0.75 |
| 61 | 3 | institution_hold+ranking_momentum+buyback | +10.03% | +0.71 |
| 62 | 4 | profit_forecast+margin+ranking_momentum+buyback | +9.09% | +0.69 |
| 63 | 5 | profit_forecast+margin+ranking_momentum+buyback+insider_buy | +9.09% | +0.69 |
| 64 | 4 | margin+institution_hold+ranking_momentum+performance | +8.01% | +0.63 |
| 65 | 5 | margin+institution_hold+ranking_momentum+insider_buy+performance | +8.01% | +0.63 |
| 66 | 3 | margin+ranking_momentum+buyback | +7.54% | +0.66 |
| 67 | 4 | margin+ranking_momentum+buyback+insider_buy | +7.54% | +0.66 |
| 68 | 3 | margin+institution_hold+ranking_momentum | +6.79% | +0.56 |
| 69 | 4 | margin+institution_hold+ranking_momentum+insider_buy | +6.79% | +0.56 |
| 70 | 3 | margin+buyback+performance | +5.01% | +0.48 |
| 71 | 4 | margin+buyback+insider_buy+performance | +5.01% | +0.48 |
| 72 | 3 | margin+institution_hold+buyback | +4.72% | +0.45 |
| 73 | 4 | margin+institution_hold+buyback+insider_buy | +4.72% | +0.45 |
| 74 | 4 | margin+institution_hold+buyback+performance | +4.72% | +0.45 |
| 75 | 5 | margin+institution_hold+buyback+insider_buy+performance | +4.72% | +0.45 |
| 76 | 4 | profit_forecast+ranking_momentum+insider_buy+performance | +4.25% | +0.45 |
| 77 | 3 | margin+institution_hold+performance | +3.76% | +0.40 |
| 78 | 4 | margin+institution_hold+insider_buy+performance | +3.76% | +0.40 |
| 79 | 3 | ranking_momentum+insider_buy+performance | +1.66% | +0.25 |
| 80 | 3 | margin+institution_hold+insider_buy | +0.04% | +0.20 |
| 81 | 3 | profit_forecast+ranking_momentum+performance | -1.59% | +0.18 |
| 82 | 4 | profit_forecast+institution_hold+ranking_momentum+buyback | -2.91% | +0.02 |
| 83 | 5 | profit_forecast+institution_hold+ranking_momentum+buyback+insider_buy | -2.91% | +0.02 |
| 84 | 3 | profit_forecast+margin+buyback | -3.61% | +0.07 |
| 85 | 4 | profit_forecast+margin+buyback+insider_buy | -3.61% | +0.07 |
| 86 | 4 | margin+ranking_momentum+buyback+performance | -3.81% | -0.13 |
| 87 | 5 | margin+ranking_momentum+buyback+insider_buy+performance | -3.81% | -0.13 |
| 88 | 4 | profit_forecast+institution_hold+ranking_momentum+performance | -6.12% | -0.22 |
| 89 | 5 | profit_forecast+institution_hold+ranking_momentum+insider_buy+performance | -6.12% | -0.22 |
| 90 | 5 | profit_forecast+institution_hold+ranking_momentum+buyback+performance | -8.19% | -0.32 |
| 91 | 3 | profit_forecast+margin+insider_buy | -9.22% | -0.19 |
