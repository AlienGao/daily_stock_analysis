import type React from 'react';
import { useState, useEffect, useMemo } from 'react';
import { Table } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { Users, Loader2 } from 'lucide-react';
import { AppPage, Card, EmptyState } from '../components/common';
import {
  getInstitutionSurvey,
  type InstitutionSurveyItem,
  type InstitutionSurveyResponse,
  type SurveyDetail,
} from '../api/brokerRecommend';

/** 权重标签样式 */
const WeightBadge: React.FC<{ weight: number }> = ({ weight }) => {
  const cls =
    weight >= 2
      ? 'bg-red-500/20 text-red-400'
      : weight >= 1
        ? 'bg-amber-500/20 text-amber-400'
        : 'bg-zinc-500/20 text-zinc-400';
  return (
    <span className={`inline-block rounded px-1.5 py-0.5 text-xs font-medium ${cls}`}>
      {weight.toFixed(1)}
    </span>
  );
};

/** 日期格式化 YYYYMMDD → MM-DD */
const fmtDate = (d: string) => {
  if (!d || d.length < 8) return d;
  return `${d.slice(4, 6)}-${d.slice(6, 8)}`;
};

/** 日期范围格式化 */
const fmtRange = (s: string, e: string) => {
  return `${fmtDate(s)} ~ ${fmtDate(e)}`;
};

const InstitutionSurveyPage: React.FC = () => {
  const [data, setData] = useState<InstitutionSurveyResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    void getInstitutionSurvey()
      .then((res) => {
        if (!cancelled) setData(res);
      })
      .catch((err) => {
        if (!cancelled) setError(err?.response?.data?.detail || err.message || '加载失败');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, []);

  const tableData = useMemo(
    () =>
      (data?.items || []).map((item, i) => ({
        key: item.ts_code,
        rank: i + 1,
        ...item,
      })),
    [data],
  );

  const columns: ColumnsType<typeof tableData[number]> = [
    {
      title: '#',
      dataIndex: 'rank',
      key: 'rank',
      width: 40,
      render: (v: number) => <span className="text-xs text-tertiary-text">{v}</span>,
    },
    {
      title: '股票',
      key: 'stock',
      render: (_: unknown, record: InstitutionSurveyItem) => (
        <div className="flex flex-col">
          <span className="text-sm font-medium">{record.name}</span>
          <span className="text-xs text-tertiary-text">{record.ts_code.split('.')[0]}</span>
        </div>
      ),
    },
    {
      title: '加权分',
      dataIndex: 'weighted_score',
      key: 'weighted_score',
      sorter: (a, b) => a.weighted_score - b.weighted_score,
      defaultSortOrder: 'descend',
      render: (v: number) => (
        <span className="text-sm font-bold text-red-400">{v.toFixed(1)}</span>
      ),
    },
    {
      title: '调研次数',
      dataIndex: 'visit_count',
      key: 'visit_count',
      render: (v: number) => <span className="text-sm">{v}</span>,
    },
    {
      title: '最近调研',
      dataIndex: 'last_surv_date',
      key: 'last_surv_date',
      render: (v: string) => <span className="text-xs text-tertiary-text">{fmtDate(v)}</span>,
    },
    {
      title: '主要来访机构',
      dataIndex: 'top_orgs',
      key: 'top_orgs',
      render: (orgs: string[]) => (
        <span className="text-xs text-secondary-text">
          {orgs.slice(0, 3).join('、')}
          {orgs.length > 3 ? ` 等${orgs.length}家` : ''}
        </span>
      ),
    },
  ];

  const expandedRowRender = (record: InstitutionSurveyItem) => {
    const detailCols: ColumnsType<SurveyDetail> = [
      { title: '日期', dataIndex: 'surv_date', key: 'surv_date', width: 80, render: (v: string) => <span className="text-xs">{fmtDate(v)}</span> },
      { title: '来访机构', dataIndex: 'rece_org', key: 'rece_org', render: (v: string) => <span className="text-xs">{v}</span> },
      { title: '机构类型', dataIndex: 'org_type', key: 'org_type', width: 100, render: (v: string) => <span className="text-xs text-tertiary-text">{v}</span> },
      { title: '调研方式', dataIndex: 'rece_mode', key: 'rece_mode', render: (v: string) => <span className="text-xs">{v}</span> },
      { title: '权重', dataIndex: 'weight', key: 'weight', width: 60, render: (v: number) => <WeightBadge weight={v} /> },
      { title: '接待人', dataIndex: 'comp_rece', key: 'comp_rece', render: (v: string) => <span className="text-xs text-tertiary-text">{v}</span> },
    ];
    return (
      <Table
        dataSource={record.details.map((d, i) => ({ key: i, ...d }))}
        columns={detailCols}
        size="small"
        pagination={false}
      />
    );
  };

  return (
    <AppPage>
      <div className="space-y-4">
        <div className="flex items-center gap-2">
          <Users className="h-5 w-5 text-tertiary-text" />
          <h1 className="text-lg font-semibold">机构调研</h1>
        </div>

        {loading && (
          <Card className="p-4 text-center text-sm text-tertiary-text">
            <Loader2 className="h-4 w-4 animate-spin inline mr-2" />
            加载中...
          </Card>
        )}

        {error && !loading && (
          <Card className="p-4 text-center text-sm text-red-400">{error}</Card>
        )}

        {data && !loading && !error && (
          <>
            {/* 概览卡片 */}
            <div className="grid grid-cols-3 gap-3">
              <Card className="p-3 text-center">
                <div className="text-lg font-bold">{fmtRange(data.start_date, data.end_date)}</div>
                <div className="text-xs text-secondary-text">统计周期（近两周）</div>
              </Card>
              <Card className="p-3 text-center">
                <div className="text-lg font-bold">{data.total_stocks}</div>
                <div className="text-xs text-secondary-text">涉及公司数</div>
              </Card>
              <Card className="p-3 text-center">
                <div className="text-lg font-bold">{data.items.reduce((s, i) => s + i.visit_count, 0)}</div>
                <div className="text-xs text-secondary-text">总调研次数</div>
              </Card>
            </div>

            {/* Top 10 表格 */}
            <Card className="p-4">
              <div className="text-sm font-medium mb-3">调研热度 Top 10（加权排序）</div>
              {data.items.length === 0 ? (
                <EmptyState
                  icon={<Users className="h-8 w-8" />}
                  title="暂无调研数据"
                  description="近两周无机构调研记录"
                />
              ) : (
                <Table
                  dataSource={tableData}
                  columns={columns}
                  size="small"
                  pagination={false}
                  expandable={{
                    expandedRowRender,
                    rowExpandable: (record) => record.details.length > 0,
                  }}
                />
              )}
            </Card>
          </>
        )}

        {!data && !loading && !error && (
          <EmptyState
            icon={<Users className="h-8 w-8" />}
            title="暂无数据"
            description="请检查 Tushare Token 配置"
          />
        )}
      </div>
    </AppPage>
  );
};

export default InstitutionSurveyPage;
