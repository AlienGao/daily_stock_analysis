import { describe, expect, it } from 'vitest';
import type { HkStockListItem } from '../../api/hkMonitor';
import { calcBollBandWidthPct, compareBollBandWidth } from '../../utils/hkBollBandwidth';
import { filterRecentDrawdowns, filterRecentGains } from '../../utils/hkMonitorDrawdown';
import { mergeHkRealtimeBollPicks, mergeHkRealtimeItems } from '../../utils/hkMonitorRealtime';
import { sortHkItemsByPctChangeDesc } from '../../utils/hkMonitorSort';

const stock = (
  hkCode: string,
  upper: number | null,
  mid: number | null,
  lower: number | null,
): HkStockListItem => ({
  hk_code: hkCode,
  boll_upper: upper,
  boll_mid: mid,
  boll_lower: lower,
});

describe('港股通 BOLL 轨道宽度列', () => {
  it('按相对中轨的上下轨宽度计算百分比', () => {
    expect(calcBollBandWidthPct(stock('00700', 12, 10, 8))).toBe(40);
  });

  it('排序时将缺失轨道数据固定放在末尾', () => {
    const narrow = stock('00001', 11, 10, 9);
    const wide = stock('00002', 14, 10, 6);
    const missing = stock('00003', null, 10, 8);

    expect(compareBollBandWidth(narrow, wide, 'ascend')).toBeLessThan(0);
    expect(compareBollBandWidth(missing, narrow, 'ascend')).toBeGreaterThan(0);
    expect(compareBollBandWidth(missing, narrow, 'descend')).toBeLessThan(0);
  });
});

describe('港股通刷新排序', () => {
  it('按最新涨跌幅降序排列，并将缺失数据放到末尾', () => {
    const refreshed = [
      { hk_code: '00700', pct_change: -0.57 },
      { hk_code: '00941', pct_change: null },
      { hk_code: '03690', pct_change: 3.91 },
      { hk_code: '01810', pct_change: 0.25 },
    ] satisfies HkStockListItem[];

    expect(sortHkItemsByPctChangeDesc(refreshed).map(item => item.hk_code)).toEqual([
      '03690',
      '01810',
      '00700',
      '00941',
    ]);
  });
});

describe('港股通近期连续回撤', () => {
  it('只展示最近五个交易日结束的回撤，并按跌幅从深到浅排列', () => {
    const items = [
      {
        hk_code: '00001',
        latest_consecutive_drawdown_pct: -8,
        latest_consecutive_drawdown_days: 2,
        latest_consecutive_drawdown_end_date: '20260701',
      },
      {
        hk_code: '00002',
        latest_consecutive_drawdown_pct: -12,
        latest_consecutive_drawdown_days: 3,
        latest_consecutive_drawdown_end_date: '20260704',
      },
      {
        hk_code: '00003',
        latest_consecutive_drawdown_pct: -10,
        latest_consecutive_drawdown_days: 2,
        latest_consecutive_drawdown_end_date: '20260707',
      },
    ] satisfies HkStockListItem[];

    expect(filterRecentDrawdowns(items, ['20260707', '20260706', '20260705', '20260704', '20260703']).map(item => item.hk_code))
      .toEqual(['00002', '00003']);
  });
});

describe('港股通近期连续上涨', () => {
  it('只展示最近五个交易日结束的上涨，按涨幅从高到低排列并取前 8 只', () => {
    const items = [
      {
        hk_code: '00001',
        latest_consecutive_gain_pct: 5,
        latest_consecutive_gain_days: 2,
        latest_consecutive_gain_end_date: '20260701',
      },
      {
        hk_code: '00002',
        latest_consecutive_gain_pct: 12,
        latest_consecutive_gain_days: 3,
        latest_consecutive_gain_end_date: '20260704',
      },
      {
        hk_code: '00003',
        latest_consecutive_gain_pct: 10,
        latest_consecutive_gain_days: 2,
        latest_consecutive_gain_end_date: '20260707',
      },
      {
        hk_code: '00004',
        latest_consecutive_gain_pct: 9,
        latest_consecutive_gain_days: 2,
        latest_consecutive_gain_end_date: '20260706',
      },
      {
        hk_code: '00005',
        latest_consecutive_gain_pct: 8,
        latest_consecutive_gain_days: 2,
        latest_consecutive_gain_end_date: '20260705',
      },
      {
        hk_code: '00006',
        latest_consecutive_gain_pct: 7,
        latest_consecutive_gain_days: 2,
        latest_consecutive_gain_end_date: '20260704',
      },
      {
        hk_code: '00007',
        latest_consecutive_gain_pct: 6,
        latest_consecutive_gain_days: 2,
        latest_consecutive_gain_end_date: '20260704',
      },
      {
        hk_code: '00008',
        latest_consecutive_gain_pct: 5.5,
        latest_consecutive_gain_days: 2,
        latest_consecutive_gain_end_date: '20260704',
      },
      {
        hk_code: '00009',
        latest_consecutive_gain_pct: 5.4,
        latest_consecutive_gain_days: 2,
        latest_consecutive_gain_end_date: '20260704',
      },
      {
        hk_code: '00010',
        latest_consecutive_gain_pct: 20,
        latest_consecutive_gain_days: 1,
        latest_consecutive_gain_end_date: '20260707',
      },
      {
        hk_code: '00011',
        latest_consecutive_gain_pct: 4.5,
        latest_consecutive_gain_days: 2,
        latest_consecutive_gain_end_date: '20260704',
      },
    ] satisfies HkStockListItem[];

    expect(filterRecentGains(items, ['20260707', '20260706', '20260705', '20260704', '20260703']).map(item => item.hk_code))
      .toEqual(['00002', '00003', '00004', '00005', '00006', '00007', '00008', '00009']);
  });
});

describe('港股通分钟行情刷新', () => {
  it('用最新分钟价更新价格、涨跌幅、轨道距离和历史高点回撤', () => {
    const [updated] = mergeHkRealtimeItems([
      {
        hk_code: '00700',
        latest_price: 500,
        pct_change: 0,
        boll_upper: 520,
        boll_mid: 500,
        boll_lower: 480,
        high_n_price: 600,
        drawdown_pct: -16.67,
      },
    ], [{ hk_code: '00700', latest_price: 510, pct_change: 2 }]);

    expect(updated.latest_price).toBe(510);
    expect(updated.pct_change).toBe(2);
    expect(updated.boll_upper_dist_pct).toBe(-1.92);
    expect(updated.boll_mid_dist_pct).toBe(2);
    expect(updated.drawdown_pct).toBe(-15);
  });

  it('同步更新 BOLL 推荐卡片现价与距轨道百分比', () => {
    const [updated] = mergeHkRealtimeBollPicks([
      {
        hk_code: '00700',
        name: '腾讯控股',
        close: 500,
        band: 'upper',
        boll_mid: 480,
        boll_upper: 510,
        boll_lower: 450,
      },
    ], [{ hk_code: '00700', latest_price: 505 }]);

    expect(updated.close).toBe(505);
    expect(updated.dist_pct).toBe(-0.98);
  });
});
