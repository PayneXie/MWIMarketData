'use client';

import { useEffect, useState } from 'react';
import { Card, Row, Col, Statistic, Spin, Alert, Layout, Typography, Table, Tag, Tooltip } from 'antd';
import { Line } from '@ant-design/charts';
import { ArrowUpOutlined, ArrowDownOutlined } from '@ant-design/icons';

const { Title } = Typography;
const { Content } = Layout;

// 添加数字格式化函数
const formatNumber = (num: number): string => {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1) + 'M';
  }
  if (num >= 1000) {
    return (num / 1000).toFixed(1) + 'K';
  }
  return num.toFixed(1);
};

interface PriceTrend {
  date: string;
  price: number;
}

interface PriceStats {
  item_id: number;
  name: string;
  name_cn: string;
  price_change: number;
  start_price: number;
  end_price: number;
}

interface StatsData {
  top_increase_7d: PriceStats[];
  top_decrease_7d: PriceStats[];
  top_increase_24h: PriceStats[];
  top_decrease_24h: PriceStats[];
}

export default function HomePage() {
  const [priceTrend, setPriceTrend] = useState<PriceTrend[]>([]);
  const [stats, setStats] = useState<StatsData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        // 获取价格走势数据
        const trendResponse = await fetch('http://localhost:5000/api/prices/trend');
        const trendResult = await trendResponse.json();
        
        if (trendResult.code === 0) {
          setPriceTrend(trendResult.data);
        }

        // 获取统计数据
        const statsResponse = await fetch('http://localhost:5000/api/prices/stats');
        const statsResult = await statsResponse.json();
        
        if (statsResult.code === 0) {
          setStats(statsResult.data);
        }
      } catch (err) {
        setError('获取数据失败');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  if (loading) {
    return (
      <Layout style={{ minHeight: '100vh', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
        <Spin size="large" />
      </Layout>
    );
  }

  if (error) {
    return (
      <Layout style={{ minHeight: '100vh', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
        <Alert
          message="错误"
          description={error}
          type="error"
          showIcon
        />
      </Layout>
    );
  }

  const trendConfig = {
    data: priceTrend,
    xField: 'date',
    yField: 'price',
    yAxis: {
      label: {
        formatter: (v: string) => `${formatNumber(Number(v))} 金币`,
      },
    },
    smooth: true,
    animation: {
      appear: {
        animation: 'path-in',
        duration: 1000,
      },
    },
  };

  return (
    <Layout>
      <Content style={{ padding: '24px', maxWidth: '1200px', margin: '0 auto' }}>
        <Title level={2} style={{ marginBottom: '24px' }}>市场概览</Title>
        
        {/* 价格走势图 */}
        <Card title="市场整体价格走势" style={{ marginBottom: '24px' }}>
          <Line {...trendConfig} />
        </Card>

        {/* 价格变化统计 */}
        <Row gutter={[16, 16]}>
          <Col xs={24} md={12}>
            <Card title="7天涨幅榜" bordered={false} style={{ height: '100%' }}>
              <Table
                dataSource={stats?.top_increase_7d}
                columns={[
                  {
                    title: '商品',
                    dataIndex: 'name_cn',
                    key: 'name',
                    width: '40%',
                    render: (text, record) => (
                      <Tooltip title={record.name}>
                        <span>{text}</span>
                      </Tooltip>
                    ),
                  },
                  {
                    title: '涨跌幅',
                    dataIndex: 'price_change',
                    key: 'price_change',
                    width: '30%',
                    align: 'right',
                    render: (value) => (
                      <span style={{ color: value > 0 ? '#cf1322' : '#3f8600' }}>
                        {value > 0 ? '+' : ''}{value.toFixed(2)}%
                      </span>
                    ),
                  },
                  {
                    title: '价格',
                    key: 'price',
                    width: '30%',
                    align: 'right',
                    render: (_, record) => (
                      <span>
                        {formatNumber(record.start_price)} → {formatNumber(record.end_price)}
                      </span>
                    ),
                  },
                ]}
                pagination={false}
                size="small"
              />
            </Card>
          </Col>
          <Col xs={24} md={12}>
            <Card title="7天跌幅榜" bordered={false} style={{ height: '100%' }}>
              <Table
                dataSource={stats?.top_decrease_7d}
                columns={[
                  {
                    title: '商品',
                    dataIndex: 'name_cn',
                    key: 'name',
                    width: '40%',
                    render: (text, record) => (
                      <Tooltip title={record.name}>
                        <span>{text}</span>
                      </Tooltip>
                    ),
                  },
                  {
                    title: '涨跌幅',
                    dataIndex: 'price_change',
                    key: 'price_change',
                    width: '30%',
                    align: 'right',
                    render: (value) => (
                      <span style={{ color: value > 0 ? '#cf1322' : '#3f8600' }}>
                        {value > 0 ? '+' : ''}{value.toFixed(2)}%
                      </span>
                    ),
                  },
                  {
                    title: '价格',
                    key: 'price',
                    width: '30%',
                    align: 'right',
                    render: (_, record) => (
                      <span>
                        {formatNumber(record.start_price)} → {formatNumber(record.end_price)}
                      </span>
                    ),
                  },
                ]}
                pagination={false}
                size="small"
              />
            </Card>
          </Col>
          <Col xs={24} md={12}>
            <Card title="24小时涨幅榜" bordered={false} style={{ height: '100%' }}>
              <Table
                dataSource={stats?.top_increase_24h}
                columns={[
                  {
                    title: '商品',
                    dataIndex: 'name_cn',
                    key: 'name',
                    width: '40%',
                    render: (text, record) => (
                      <Tooltip title={record.name}>
                        <span>{text}</span>
                      </Tooltip>
                    ),
                  },
                  {
                    title: '涨跌幅',
                    dataIndex: 'price_change',
                    key: 'price_change',
                    width: '30%',
                    align: 'right',
                    render: (value) => (
                      <span style={{ color: value > 0 ? '#cf1322' : '#3f8600' }}>
                        {value > 0 ? '+' : ''}{value.toFixed(2)}%
                      </span>
                    ),
                  },
                  {
                    title: '价格',
                    key: 'price',
                    width: '30%',
                    align: 'right',
                    render: (_, record) => (
                      <span>
                        {formatNumber(record.start_price)} → {formatNumber(record.end_price)}
                      </span>
                    ),
                  },
                ]}
                pagination={false}
                size="small"
              />
            </Card>
          </Col>
          <Col xs={24} md={12}>
            <Card title="24小时跌幅榜" bordered={false} style={{ height: '100%' }}>
              <Table
                dataSource={stats?.top_decrease_24h}
                columns={[
                  {
                    title: '商品',
                    dataIndex: 'name_cn',
                    key: 'name',
                    width: '40%',
                    render: (text, record) => (
                      <Tooltip title={record.name}>
                        <span>{text}</span>
                      </Tooltip>
                    ),
                  },
                  {
                    title: '涨跌幅',
                    dataIndex: 'price_change',
                    key: 'price_change',
                    width: '30%',
                    align: 'right',
                    render: (value) => (
                      <span style={{ color: value > 0 ? '#cf1322' : '#3f8600' }}>
                        {value > 0 ? '+' : ''}{value.toFixed(2)}%
                      </span>
                    ),
                  },
                  {
                    title: '价格',
                    key: 'price',
                    width: '30%',
                    align: 'right',
                    render: (_, record) => (
                      <span>
                        {formatNumber(record.start_price)} → {formatNumber(record.end_price)}
                      </span>
                    ),
                  },
                ]}
                pagination={false}
                size="small"
              />
            </Card>
          </Col>
        </Row>
      </Content>
    </Layout>
  );
}
