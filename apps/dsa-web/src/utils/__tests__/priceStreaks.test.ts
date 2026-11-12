import { describe, expect, it } from 'vitest';
import type { HfqNewHighItem } from '../../api/market';
import { filterRecentDrawdowns, filterRecentGains } from '../priceStreaks';

const item = (tsCode: string, extra: Partial<HfqNewHighItem>): HfqNewHighItem => ({
  ts_code: tsCode,
  stock_code: tsCode.split('.')[0],
  stock_name: tsCode,
  latest_new_high_date: '20261110',
  latest_new_high_close: 10,
  new_high_count: 1,
  new_high_dates: [],
  ...extra,
});

describe('后复权新高页最近连续涨跌段', () => {
  const recentDates = ['20261112', '20261111', '20261110', '20261107', '20261106'];

  it('只保留最近 5 个交易日内结束的连跌段，按跌幅从深到浅取前 15', () => {
    const items = [
      item('000001.SZ', {
        latest_consecutive_drawdown_pct: -6,
        latest_consecutive_drawdown_days: 2,
        latest_consecutive_drawdown_end_date: '20261112',
      }),
      item('000002.SZ', {
        latest_consecutive_drawdown_pct: -12,
        latest_consecutive_drawdown_days: 3,
        latest_consecutive_drawdown_end_date: '20261110',
      }),
      item('000003.SZ', {
        latest_consecutive_drawdown_pct: -30,
        latest_consecutive_drawdown_days: 4,
        latest_consecutive_drawdown_end_date: '20261020',
      }),
      item('000004.SZ', {
        latest_consecutive_drawdown_pct: -9,
        latest_consecutive_drawdown_days: 1,
        latest_consecutive_drawdown_end_date: '20261112',
      }),
    ];

    expect(filterRecentDrawdowns(items, recentDates).map(row => row.ts_code)).toEqual([
      '000002.SZ',
      '000001.SZ',
    ]);
  });

  it('只保留最近 5 个交易日内结束的连涨段，按涨幅从高到低取前 12', () => {
    const items = [
      item('000001.SZ', {
        latest_consecutive_gain_pct: 6,
        latest_consecutive_gain_days: 2,
        latest_consecutive_gain_end_date: '20261111',
      }),
      item('000002.SZ', {
        latest_consecutive_gain_pct: 18,
        latest_consecutive_gain_days: 3,
        latest_consecutive_gain_end_date: '20261112',
      }),
      item('000003.SZ', {
        latest_consecutive_gain_pct: 21,
        latest_consecutive_gain_days: 3,
        latest_consecutive_gain_end_date: '20261113',
      }),
    ];

    expect(filterRecentGains(items, recentDates).map(row => row.ts_code)).toEqual([
      '000002.SZ',
      '000001.SZ',
    ]);
  });

  it('缺少最近交易日列表时不做时间过滤，仅按幅度排序', () => {
    const items = [
      item('000001.SZ', {
        latest_consecutive_gain_pct: 6,
        latest_consecutive_gain_days: 2,
        latest_consecutive_gain_end_date: '20260302',
      }),
      item('000002.SZ', {
        latest_consecutive_gain_pct: 18,
        latest_consecutive_gain_days: 3,
        latest_consecutive_gain_end_date: '20260303',
      }),
    ];

    expect(filterRecentGains(items, []).map(row => row.ts_code)).toEqual([
      '000002.SZ',
      '000001.SZ',
    ]);
  });
});
