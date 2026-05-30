# 因子组合搜索排名（方案 A 权重）

- **因子池**: 盈利预测, 融资融券, 机构持股, 排名动量, 回购, 高管增持, 业绩
- **组合规模**: 3 / 4 / 5 因子
- **回测区间**: 20250101 ~ 最新
- **排序**: 5 日持有期总收益
- **组合数**: 91
- **生成时间**: 2026-05-30 01:39:39

## Top 20（5日总收益）

| 排名 | 规模 | 组合 | 5日收益 | 年化 | Sharpe | 回撤 | 胜率 | 交易数 |
|------|------|------|---------|------|--------|------|------|--------|
| 1 | 3 | 融资融券+排名动量+回购 | +795.35% | +412.59% | +3.76 | 19.25% | 62.3% | 332 |
| 2 | 4 | 融资融券+排名动量+回购+高管增持 | +795.35% | +410.12% | +3.76 | 19.25% | 62.3% | 332 |
| 3 | 4 | 融资融券+排名动量+回购+业绩 | +698.02% | +368.30% | +3.47 | 18.80% | 60.5% | 332 |
| 4 | 5 | 融资融券+排名动量+回购+高管增持+业绩 | +698.02% | +368.30% | +3.47 | 18.80% | 60.5% | 332 |
| 5 | 3 | 盈利预测+回购+业绩 | +526.91% | +292.98% | +2.39 | 46.16% | 58.7% | 332 |
| 6 | 3 | 融资融券+回购+高管增持 | +470.21% | +266.16% | +2.59 | 27.04% | 60.8% | 332 |
| 7 | 3 | 机构持股+回购+业绩 | +444.36% | +253.71% | +2.66 | 23.51% | 64.5% | 332 |
| 8 | 4 | 机构持股+回购+高管增持+业绩 | +444.36% | +252.40% | +2.66 | 23.51% | 64.5% | 332 |
| 9 | 3 | 机构持股+回购+高管增持 | +439.23% | +251.22% | +2.44 | 21.00% | 61.7% | 332 |
| 10 | 3 | 融资融券+排名动量+高管增持 | +431.65% | +247.53% | +2.96 | 26.18% | 59.6% | 332 |
| 11 | 3 | 融资融券+回购+业绩 | +373.79% | +218.92% | +2.32 | 23.69% | 58.1% | 332 |
| 12 | 4 | 融资融券+回购+高管增持+业绩 | +373.79% | +217.84% | +2.31 | 23.69% | 58.1% | 332 |
| 13 | 4 | 盈利预测+融资融券+机构持股+业绩 | +366.64% | +215.33% | +2.23 | 30.73% | 59.6% | 332 |
| 14 | 5 | 盈利预测+融资融券+机构持股+高管增持+业绩 | +366.64% | +214.26% | +2.23 | 30.73% | 59.6% | 332 |
| 15 | 4 | 盈利预测+融资融券+排名动量+回购 | +341.87% | +202.77% | +2.51 | 22.96% | 56.0% | 332 |
| 16 | 5 | 盈利预测+融资融券+排名动量+回购+高管增持 | +341.87% | +201.78% | +2.51 | 22.96% | 56.0% | 332 |
| 17 | 3 | 盈利预测+排名动量+回购 | +308.92% | +185.77% | +3.68 | 12.72% | 55.1% | 332 |
| 18 | 5 | 盈利预测+融资融券+机构持股+回购+业绩 | +299.19% | +179.83% | +1.99 | 30.73% | 58.4% | 332 |
| 19 | 3 | 盈利预测+融资融券+机构持股 | +286.51% | +174.01% | +1.95 | 30.73% | 58.1% | 332 |
| 20 | 4 | 盈利预测+融资融券+机构持股+高管增持 | +286.51% | +174.01% | +1.95 | 30.73% | 58.1% | 332 |

## 全部组合

| 排名 | 规模 | 因子 keys | 5日收益 | Sharpe |
|------|------|-----------|---------|--------|
| 1 | 3 | margin+ranking_momentum+buyback | +795.35% | +3.76 |
| 2 | 4 | margin+ranking_momentum+buyback+insider_buy | +795.35% | +3.76 |
| 3 | 4 | margin+ranking_momentum+buyback+performance | +698.02% | +3.47 |
| 4 | 5 | margin+ranking_momentum+buyback+insider_buy+performance | +698.02% | +3.47 |
| 5 | 3 | profit_forecast+buyback+performance | +526.91% | +2.39 |
| 6 | 3 | margin+buyback+insider_buy | +470.21% | +2.59 |
| 7 | 3 | institution_hold+buyback+performance | +444.36% | +2.66 |
| 8 | 4 | institution_hold+buyback+insider_buy+performance | +444.36% | +2.66 |
| 9 | 3 | institution_hold+buyback+insider_buy | +439.23% | +2.44 |
| 10 | 3 | margin+ranking_momentum+insider_buy | +431.65% | +2.96 |
| 11 | 3 | margin+buyback+performance | +373.79% | +2.32 |
| 12 | 4 | margin+buyback+insider_buy+performance | +373.79% | +2.31 |
| 13 | 4 | profit_forecast+margin+institution_hold+performance | +366.64% | +2.23 |
| 14 | 5 | profit_forecast+margin+institution_hold+insider_buy+performance | +366.64% | +2.23 |
| 15 | 4 | profit_forecast+margin+ranking_momentum+buyback | +341.87% | +2.51 |
| 16 | 5 | profit_forecast+margin+ranking_momentum+buyback+insider_buy | +341.87% | +2.51 |
| 17 | 3 | profit_forecast+ranking_momentum+buyback | +308.92% | +3.68 |
| 18 | 5 | profit_forecast+margin+institution_hold+buyback+performance | +299.19% | +1.99 |
| 19 | 3 | profit_forecast+margin+institution_hold | +286.51% | +1.95 |
| 20 | 4 | profit_forecast+margin+institution_hold+insider_buy | +286.51% | +1.95 |
| 21 | 5 | profit_forecast+margin+institution_hold+buyback+insider_buy | +279.77% | +1.94 |
| 22 | 4 | profit_forecast+margin+institution_hold+buyback | +272.14% | +1.91 |
| 23 | 4 | profit_forecast+margin+buyback+performance | +268.98% | +1.70 |
| 24 | 5 | profit_forecast+margin+buyback+insider_buy+performance | +268.98% | +1.69 |
| 25 | 5 | profit_forecast+margin+institution_hold+ranking_momentum+performance | +266.66% | +2.49 |
| 26 | 5 | profit_forecast+margin+ranking_momentum+buyback+performance | +262.05% | +2.25 |
| 27 | 3 | profit_forecast+buyback+insider_buy | +255.60% | +2.32 |
| 28 | 3 | institution_hold+insider_buy+performance | +252.51% | +2.03 |
| 29 | 3 | buyback+insider_buy+performance | +252.03% | +2.46 |
| 30 | 3 | profit_forecast+insider_buy+performance | +241.94% | +1.68 |
| 31 | 3 | profit_forecast+margin+ranking_momentum | +232.11% | +2.17 |
| 32 | 4 | profit_forecast+margin+ranking_momentum+insider_buy | +232.11% | +2.17 |
| 33 | 4 | profit_forecast+margin+ranking_momentum+performance | +227.01% | +2.17 |
| 34 | 5 | profit_forecast+margin+ranking_momentum+insider_buy+performance | +227.01% | +2.17 |
| 35 | 4 | profit_forecast+ranking_momentum+buyback+insider_buy | +210.71% | +2.81 |
| 36 | 5 | profit_forecast+ranking_momentum+buyback+insider_buy+performance | +201.92% | +2.50 |
| 37 | 3 | margin+ranking_momentum+performance | +181.34% | +1.81 |
| 38 | 4 | margin+ranking_momentum+insider_buy+performance | +181.34% | +1.81 |
| 39 | 4 | profit_forecast+buyback+insider_buy+performance | +179.70% | +1.80 |
| 40 | 4 | profit_forecast+ranking_momentum+buyback+performance | +168.15% | +2.06 |
| 41 | 4 | profit_forecast+margin+institution_hold+ranking_momentum | +166.20% | +1.98 |
| 42 | 5 | profit_forecast+margin+institution_hold+ranking_momentum+insider_buy | +166.20% | +1.98 |
| 43 | 5 | profit_forecast+margin+institution_hold+ranking_momentum+buyback | +165.78% | +1.85 |
| 44 | 3 | margin+insider_buy+performance | +157.66% | +1.32 |
| 45 | 3 | profit_forecast+margin+performance | +154.11% | +1.29 |
| 46 | 4 | profit_forecast+margin+insider_buy+performance | +154.11% | +1.29 |
| 47 | 3 | profit_forecast+margin+buyback | +131.22% | +1.21 |
| 48 | 4 | profit_forecast+margin+buyback+insider_buy | +131.22% | +1.21 |
| 49 | 3 | ranking_momentum+buyback+performance | +115.64% | +1.87 |
| 50 | 4 | ranking_momentum+buyback+insider_buy+performance | +102.38% | +1.70 |
| 51 | 5 | margin+institution_hold+ranking_momentum+buyback+performance | +86.96% | +1.50 |
| 52 | 3 | profit_forecast+ranking_momentum+performance | +82.83% | +1.05 |
| 53 | 3 | ranking_momentum+buyback+insider_buy | +77.34% | +1.59 |
| 54 | 5 | institution_hold+ranking_momentum+buyback+insider_buy+performance | +76.75% | +1.41 |
| 55 | 4 | institution_hold+ranking_momentum+buyback+performance | +74.80% | +1.36 |
| 56 | 3 | profit_forecast+ranking_momentum+insider_buy | +70.61% | +1.29 |
| 57 | 3 | margin+institution_hold+performance | +69.30% | +0.96 |
| 58 | 4 | margin+institution_hold+insider_buy+performance | +69.30% | +0.96 |
| 59 | 3 | profit_forecast+institution_hold+ranking_momentum | +66.47% | +1.19 |
| 60 | 4 | profit_forecast+institution_hold+ranking_momentum+insider_buy | +66.47% | +1.19 |
| 61 | 3 | margin+institution_hold+insider_buy | +62.89% | +0.91 |
| 62 | 4 | margin+institution_hold+buyback+performance | +62.86% | +1.10 |
| 63 | 5 | margin+institution_hold+buyback+insider_buy+performance | +62.86% | +1.10 |
| 64 | 3 | ranking_momentum+insider_buy+performance | +59.68% | +1.11 |
| 65 | 4 | institution_hold+ranking_momentum+buyback+insider_buy | +59.50% | +1.11 |
| 66 | 5 | profit_forecast+institution_hold+ranking_momentum+buyback+insider_buy | +55.70% | +1.06 |
| 67 | 4 | profit_forecast+institution_hold+ranking_momentum+buyback | +54.24% | +1.04 |
| 68 | 3 | margin+institution_hold+ranking_momentum | +53.40% | +0.93 |
| 69 | 4 | margin+institution_hold+ranking_momentum+insider_buy | +53.40% | +0.93 |
| 70 | 3 | institution_hold+ranking_momentum+buyback | +52.96% | +1.02 |
| 71 | 3 | institution_hold+ranking_momentum+insider_buy | +49.53% | +0.95 |
| 72 | 3 | profit_forecast+institution_hold+buyback | +43.58% | +0.82 |
| 73 | 3 | profit_forecast+institution_hold+insider_buy | +43.58% | +0.82 |
| 74 | 4 | profit_forecast+institution_hold+buyback+insider_buy | +43.58% | +0.82 |
| 75 | 4 | profit_forecast+institution_hold+buyback+performance | +43.58% | +0.82 |
| 76 | 5 | profit_forecast+institution_hold+buyback+insider_buy+performance | +43.58% | +0.82 |
| 77 | 3 | profit_forecast+institution_hold+performance | +41.77% | +0.87 |
| 78 | 4 | profit_forecast+institution_hold+insider_buy+performance | +41.77% | +0.87 |
| 79 | 4 | margin+institution_hold+ranking_momentum+performance | +39.64% | +0.75 |
| 80 | 5 | margin+institution_hold+ranking_momentum+insider_buy+performance | +39.64% | +0.75 |
| 81 | 4 | margin+institution_hold+ranking_momentum+buyback | +39.00% | +0.82 |
| 82 | 5 | margin+institution_hold+ranking_momentum+buyback+insider_buy | +39.00% | +0.82 |
| 83 | 4 | institution_hold+ranking_momentum+insider_buy+performance | +32.39% | +0.72 |
| 84 | 4 | profit_forecast+ranking_momentum+insider_buy+performance | +26.76% | +0.58 |
| 85 | 3 | margin+institution_hold+buyback | +26.64% | +0.59 |
| 86 | 4 | margin+institution_hold+buyback+insider_buy | +26.64% | +0.59 |
| 87 | 3 | institution_hold+ranking_momentum+performance | +26.55% | +0.63 |
| 88 | 3 | profit_forecast+margin+insider_buy | +25.32% | +0.60 |
| 89 | 4 | profit_forecast+institution_hold+ranking_momentum+performance | +20.60% | +0.53 |
| 90 | 5 | profit_forecast+institution_hold+ranking_momentum+insider_buy+performance | +20.60% | +0.53 |
| 91 | 5 | profit_forecast+institution_hold+ranking_momentum+buyback+performance | +12.50% | +0.37 |
