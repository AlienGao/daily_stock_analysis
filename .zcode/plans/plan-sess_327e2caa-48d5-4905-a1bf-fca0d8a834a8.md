## 目标

券商金股页面当前「当月收益」数据(当月明细表/券商汇总曲线、「上月推荐×当月收益」Top5、历史月查看时的当月累计收益列)目前都停在上一交易日收盘口径。本次让它们在**交易日盘中(北京时间 09:30–15:00)基于实时行情估算展示**,收盘后/非交易日维持现有收盘口径;前端修正盘中 30 秒轮询的时区判定并给出「盘中估算」标注。

## 根因(已定位)

- 后端 `compute_backtest`(src/services/broker_recommend_service.py:1427)当月分支已通过 `_get_realtime_prices_batch`(:1257)拿到今日实时价,但合并前闸门 `_has_exact_adj_factor(adj_map, today)`(:1534)要求**当日复权因子已入库**(盘后约 18:01 才刷新),盘中恒为 false → 实时价被丢弃,`trading_days`/`sell_date` 停在上一交易日。
- 即使放行,`_sync_daily_returns_from_ohlc`(:1976)与 `_resolve_sell_date_with_adj`(:1036)会因今日无精确因子把 `end_date` 回落上一交易日、今日 bar 被 OHLC 过滤裁掉,`month_cumulative_return` 仍回昨日口径。
- `get_current_month_stock_returns`(:1813)存在同构闸门(:1838),供当月 Top5 与历史月「当月累计收益」列消费。
- 前端 BrokerRecommendPage.tsx:1353-1370 已有 30 秒盘中轮询,但交易时段判定用浏览器本地时区 + 死代码分支,不判工作日/节假日,且无法改变后端返回口径。

## 改动方案

### 后端 src/services/broker_recommend_service.py(核心)
1. 新增盘中判定 helper(如 `_intraday_live_window()`):`is_trading_day()`(复用 src/discovery/engine.py:62)+ 服务器时间 09:30≤t<15:00(与 `_effective_month_end` 既有本地时间基调一致)。**不改 `_effective_month_end`** 及九转反转/历史展开等其他消费方,避免行为漂移。
2. `compute_backtest` 当月实时合并段(:1523-1571)改造:
   - 个股级放行条件改为「盘中窗口 且 `rt_change_dates[ts]==today`」(spot `trade_date` 为今天,证明是真实今日快照;防止把昨收/非交易日残留误当今日价)。因子改用 `adj_map.get(today) or _lookup_adj_factor(adj_map, today)`(盘中无当日因子时用最近可得因子近似)。
   - 写 `stock_daily` 今日 OHLC(:1547-1567)只保留在收盘态路径;盘中放行路径**不写库**,避免盘中中间价污染日线表(今日收盘价由盘后管线照常写入覆盖)。
   - 放行成功时响应顶层加 `is_realtime: true`,对齐服务内九转信号的 `is_realtime` 先例(:2318/:2380)。
3. `_sync_daily_returns_from_ohlc` 与 `_resolve_sell_date_with_adj` 增加默认 `False` 的参数(如 `allow_live_today`):盘中调用时今日 bar 不因缺精确因子/缺 OHLC 被裁掉(close 用实时价,OHLC 用实时快照 open/high/low 注入);其余 6 处历史/单股调用点不传参,行为完全不变。
4. `compute_backtest` 的 OHLC 同步段(:1706-1712)仅在当月盘中放行时把实时 OHLC/flag 传入 `_sync`。
5. `get_current_month_stock_returns`(:1830-1853)做同构个股级放行,并让 `_cumulative_return_from_price_window`(:1775)盘中端日不回退;`prev-month-current-top`、`current-month-returns` 端点自动受益。
6. YTD 当月段(compute_ytd_backtest → `_append_live_current_month_backtest`:2468 调用同一 compute_backtest)自动同步盘中口径,不额外改动。

### API api/v1/endpoints/broker_recommend.py
- `BrokerBacktestResponse` 及 current-month-returns 响应模型新增可选字段 `is_realtime: bool = False`(向后兼容,只追加不删改)。

### 前端 apps/dsa-web
1. BrokerRecommendPage.tsx:1353-1370 轮询 effect:交易时段判定改为 Asia/Shanghai + 工作日 + 分钟级窗口 09:30–15:00(对齐 DiscoveryPage.tsx:24-49 模式,午休轮询保留无害)。
2. `is_realtime` 为真时展示「盘中估算」标注(参照九转反转卡片「(实时估算)」样式),位置:当月明细「累计收益/最新价」相关标题或说明文案 + 历史月「当月累计收益」列;文案注明交易时段内每 30 秒自动刷新。
3. brokerRecommend.ts 前端类型同步加 `is_realtime?`。

### 文档
- docs/CHANGELOG.md [Unreleased] 追加一行扁平条目(类型:改进/新功能)。
- 无新配置项,.env.example 不动。

## 验证

- 后端:py_compile;`./scripts/ci_gate.sh`;检查并运行 tests/ 下 broker_recommend 相关既有测试;新增单测覆盖:盘中放行(时间/is_trading_day monkeypatch + spot trade_date=today 用例 → sell_date=today、累计收益按实时价估算)、防误放行(trade_date≠today、非交易日 → 维持上一交易日口径)、历史月路径默认行为回归。
- 前端:`npm run lint && npm run build`(改动仅 2 个 ts 文件)。
- 若执行时不在交易时段,盘中实测项(真实请求返回 sell_date/is_realtime)列为未验证并说明。

## 风险点与取舍

- **除权除息日盘中**:当日复权因子盘后才更新,盘中按最近因子估算,个股当日收益可能与盘后精确值有偏差;UI 以「盘中估算」标注透明化,收盘后因子入库自动收敛为精确值。
- **15:00–当日因子入库(约 18:01)窗口**维持现状(上一交易日收盘),不在本次诉求内。
- **实时数据源**:依赖现有 realtime_spot 链路(腾讯→新浪全市场快照 30s slot + 陈旧直连兜底),未引入新 provider;每 30s 一次的前端轮询已是现状频率,无新增请求压力。
- 服务器时区沿用「北京时间」的既有代码假设;前端用 Asia/Shanghai 独立判定。
- end_price/daily_returns 价格口径不随本改动变化(端价仍由同一 price_cache 后复权链生成),执行时对照一处现网数据确认展示口径与改造前一致。

## 回滚

后端回滚:撤销 broker_recommend_service.py / broker_recommend.py 改动即恢复收盘口径(无数据迁移、无 schema 破坏);前端改动独立回退。盘中路径不写库,无残留数据。