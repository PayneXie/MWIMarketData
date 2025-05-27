'use client';

import { useEffect, useState, useRef } from 'react';
import { Card, Row, Col, Statistic, Spin, Alert, Layout, Typography, Table, Tag, Tooltip, Empty } from 'antd';
import ReactECharts from 'echarts-for-react';
import { ArrowUpOutlined, ArrowDownOutlined } from '@ant-design/icons';

const { Title } = Typography;
const { Content } = Layout;

// 添加数字格式化函数
const formatNumber = (num: number | null | undefined): string => {
  if (num === null || num === undefined) return '-';
  
  // 只处理到百万级别
  if (num >= 1000000) {  // 百万
    return (num / 1000000).toFixed(1) + 'M';
  }
  if (num >= 1000) {  // 千
    return (num / 1000).toFixed(1) + 'K';
  }
  return num.toFixed(1);
};

// 格式化时间戳为日期字符串
const formatDate = (timestamp: number): string => {
  const date = new Date(timestamp * 1000);
  return `${date.getMonth() + 1}/${date.getDate()}`;
};

interface MarketTrend {
  timestamp: number;
  avg_price: number;
  open: number;
  close: number;
  low: number;
  high: number;
  start_time: number;
  end_time: number;
  volume: number;
  ma5: number;
  ma10: number;
}

interface ItemStats {
  id: number;
  name: string;
  name_cn: string;
  current_price: number;
  old_price: number;
  change_percentage: number;
}

interface StatsData {
  day7: ItemStats[];
  day1: ItemStats[];
}

// 计算移动平均线
const calculateMA = (data: any[], dayCount: number) => {
  const result = [];
  for (let i = 0, len = data.length; i < len; i++) {
    if (i < dayCount - 1) {
      result.push('-');
      continue;
    }
    let sum = 0;
    for (let j = 0; j < dayCount; j++) {
      sum += data[i - j][1];
    }
    result.push(+(sum / dayCount).toFixed(2));
  }
  return result;
};

// 添加自定义样式组件
const ChangePercentageCard = ({ value }: { value: number }) => {
  const isPositive = value > 0;
  const bgColor = isPositive ? 'rgba(207, 19, 34, 0.1)' : 'rgba(63, 134, 0, 0.1)';
  const textColor = isPositive ? '#cf1322' : '#3f8600';
  const borderColor = isPositive ? 'rgba(207, 19, 34, 0.3)' : 'rgba(63, 134, 0, 0.3)';

  return (
    <div
      style={{
        display: 'inline-block',
        padding: '4px 8px',
        borderRadius: '6px',
        backgroundColor: bgColor,
        border: `1px solid ${borderColor}`,
        color: textColor,
        fontSize: '14px',
        fontWeight: 500,
        minWidth: '70px',
        textAlign: 'center'
      }}
    >
      {value > 0 ? '+' : ''}{value.toFixed(2)}%
    </div>
  );
};

export default function HomePage() {
  const [priceTrend, setPriceTrend] = useState<MarketTrend[]>([]);
  const [stats, setStats] = useState<StatsData | null>(null);
  const [loading, setLoading] = useState(true);
  const [trendError, setTrendError] = useState<string | null>(null);
  const [statsError, setStatsError] = useState<string | null>(null);
  const chartRef = useRef<any>(null);

  // 计算Y轴范围的函数
  const calculateYAxisRange = (data: MarketTrend[], startIndex: number, endIndex: number) => {
    const visibleData = data.slice(startIndex, endIndex + 1).map(item => [
      item.open,
      item.close,
      item.low,
      item.high
    ]);
    
    const allPrices = visibleData.reduce((acc, [open, close, low, high]) => {
      acc.push(low, high);
      if (open) acc.push(open);
      if (close) acc.push(close);
      return acc;
    }, [] as number[]);
    
    const maxPrice = Math.max(...allPrices);
    const minPrice = Math.min(...allPrices);
    const priceRange = maxPrice - minPrice;
    
    return {
      min: minPrice - priceRange * 0.1,
      max: maxPrice + priceRange * 0.1
    };
  };

  useEffect(() => {
    const fetchData = async () => {
      try {
        // 获取价格走势数据
        const trendResponse = await fetch('http://localhost:5000/api/v1/market/trends');
        const trendResult = await trendResponse.json();
        
        if (trendResult.code === 0) {
          setPriceTrend(trendResult.data);
        } else {
          setTrendError('获取价格走势数据失败');
        }
      } catch (err) {
        setTrendError('获取价格走势数据失败');
      }

      try {
        // 获取统计数据
        const statsResponse = await fetch('http://localhost:5000/api/v1/market/statistics');
        const statsResult = await statsResponse.json();
        
        if (statsResult.code === 0) {
          // 数据处理：按涨跌幅排序
          const day7Data = statsResult.data.day7.sort((a: ItemStats, b: ItemStats) => 
            Math.abs(b.change_percentage) - Math.abs(a.change_percentage)
          );
          const day1Data = statsResult.data.day1.sort((a: ItemStats, b: ItemStats) => 
            Math.abs(b.change_percentage) - Math.abs(a.change_percentage)
          );

          setStats({
            day7: day7Data,
            day1: day1Data
          });
        } else {
          setStatsError('获取市场统计数据失败');
        }
      } catch (err) {
        setStatsError('获取市场统计数据失败');
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

  // 生成K线图配置
  const getKLineOption = (data: MarketTrend[]) => {
    // 准备数据
    const dates = data.map(item => formatDate(item.timestamp));
    const klineData = data.map(item => [
      item.open,  // 开盘价
      item.close, // 收盘价
      item.low,   // 最低价
      item.high   // 最高价
    ]);

    // 准备MA5和MA10数据
    const ma5Data = data.map(item => item.ma5);
    const ma10Data = data.map(item => item.ma10);

    // 初始Y轴范围
    const initialRange = calculateYAxisRange(data, 0, data.length - 1);

    return {
      title: {
        text: '市场价格走势',
        left: 'center'
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross'
        },
        formatter: function (params: any) {
          const date = params[0].name;
          let res = `<div style="font-weight:bold;margin-bottom:10px;">${date}</div>`;
          
          params.forEach((param: any) => {
            let color = param.color;
            let value = param.value;
            
            if (param.seriesName === 'K线') {
              const [open, close, low, high] = value;
              color = close > open ? '#ef232a' : '#14b143';
              res += `<div style="color:${color}">
                开盘价: ${formatNumber(open)}<br/>
                收盘价: ${formatNumber(close)}<br/>
                最低价: ${formatNumber(low)}<br/>
                最高价: ${formatNumber(high)}<br/>
              </div>`;
            } else {
              res += `<div style="color:${color}">
                ${param.seriesName}: ${formatNumber(value)}<br/>
              </div>`;
            }
          });
          
          return res;
        }
      },
      legend: {
        data: ['K线', 'MA5', 'MA10'],
        top: 30
      },
      grid: {
        left: '10%',
        right: '10%',
        bottom: '15%',
        top: '15%'
      },
      xAxis: {
        type: 'category',
        data: dates,
        scale: true,
        boundaryGap: true,
        axisLine: { onZero: false },
        splitLine: { show: false },
        splitNumber: 20,
        min: 'dataMin',
        max: 'dataMax'
      },
      yAxis: {
        scale: true,
        splitArea: {
          show: true
        },
        min: initialRange.min,
        max: initialRange.max,
        splitNumber: 8,
        axisLabel: {
          formatter: (value: number) => formatNumber(value)
        }
      },
      dataZoom: [
        {
          type: 'inside',
          start: 0,
          end: 100,
          minValueSpan: 3,
          zoomOnMouseWheel: 'shift',  // 按住shift键时才能缩放
          moveOnMouseMove: true,      // 允许鼠标拖动
          preventDefaultMouseMove: true
        },
        {
          show: true,
          type: 'slider',
          bottom: 60,
          start: 0,
          end: 100,
          minValueSpan: 3
        }
      ],
      series: [
        {
          name: 'K线',
          type: 'candlestick',
          data: klineData,
          itemStyle: {
            color: '#ef232a',
            color0: '#14b143',
            borderColor: '#ef232a',
            borderColor0: '#14b143'
          }
        },
        {
          name: 'MA5',
          type: 'line',
          data: ma5Data,
          smooth: true,
          lineStyle: {
            opacity: 0.8,
            width: 2
          },
          itemStyle: {
            color: '#e6b800'
          }
        },
        {
          name: 'MA10',
          type: 'line',
          data: ma10Data,
          smooth: true,
          lineStyle: {
            opacity: 0.8,
            width: 2
          },
          itemStyle: {
            color: '#66b3ff'
          }
        }
      ]
    };
  };

  return (
    <Layout>
      <Content style={{ padding: '24px', maxWidth: '1200px', margin: '0 auto' }}>
        <Title level={2} style={{ marginBottom: '24px' }}>市场概览</Title>
        
        {/* 价格变化统计 */}
        {statsError ? (
          <Alert
            message="数据获取失败"
            description={statsError}
            type="error"
            showIcon
            style={{ marginBottom: '16px' }}
          />
        ) : null}
        <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
          <Col xs={24} md={12}>
            <Card title="7天涨幅榜 TOP5" variant="outlined" style={{ height: '100%' }}>
              {stats?.day7 ? (
                <Table
                  dataSource={stats.day7
                    .filter(item => item.change_percentage > 0)
                    .slice(0, 5)
                    .map(item => ({ ...item, key: `7d-up-${item.id}` }))}
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
                      dataIndex: 'change_percentage',
                      key: 'change_percentage',
                      width: '30%',
                      align: 'right',
                      render: (value) => <ChangePercentageCard value={value} />,
                    },
                    {
                      title: '价格',
                      key: 'price',
                      width: '30%',
                      align: 'right',
                      render: (_, record) => (
                        <span>
                          {formatNumber(record.old_price)} → {formatNumber(record.current_price)}
                        </span>
                      ),
                    },
                  ]}
                  pagination={false}
                  size="small"
                  locale={{
                    emptyText: '暂无数据'
                  }}
                />
              ) : (
                <Empty description="暂无数据" />
              )}
            </Card>
          </Col>
          <Col xs={24} md={12}>
            <Card title="7天跌幅榜 TOP5" variant="outlined" style={{ height: '100%' }}>
              {stats?.day7 ? (
                <Table
                  dataSource={stats.day7
                    .filter(item => item.change_percentage < 0)
                    .slice(0, 5)
                    .map(item => ({ ...item, key: `7d-down-${item.id}` }))}
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
                      dataIndex: 'change_percentage',
                      key: 'change_percentage',
                      width: '30%',
                      align: 'right',
                      render: (value) => <ChangePercentageCard value={value} />,
                    },
                    {
                      title: '价格',
                      key: 'price',
                      width: '30%',
                      align: 'right',
                      render: (_, record) => (
                        <span>
                          {formatNumber(record.old_price)} → {formatNumber(record.current_price)}
                        </span>
                      ),
                    },
                  ]}
                  pagination={false}
                  size="small"
                  locale={{
                    emptyText: '暂无数据'
                  }}
                />
              ) : (
                <Empty description="暂无数据" />
              )}
            </Card>
          </Col>
          <Col xs={24} md={12}>
            <Card title="24小时涨幅榜 TOP5" variant="outlined" style={{ height: '100%' }}>
              {stats?.day1 ? (
                <Table
                  dataSource={stats.day1
                    .filter(item => item.change_percentage > 0)
                    .slice(0, 5)
                    .map(item => ({ ...item, key: `1d-up-${item.id}` }))}
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
                      dataIndex: 'change_percentage',
                      key: 'change_percentage',
                      width: '30%',
                      align: 'right',
                      render: (value) => <ChangePercentageCard value={value} />,
                    },
                    {
                      title: '价格',
                      key: 'price',
                      width: '30%',
                      align: 'right',
                      render: (_, record) => (
                        <span>
                          {formatNumber(record.old_price)} → {formatNumber(record.current_price)}
                        </span>
                      ),
                    },
                  ]}
                  pagination={false}
                  size="small"
                  locale={{
                    emptyText: '暂无数据'
                  }}
                />
              ) : (
                <Empty description="暂无数据" />
              )}
            </Card>
          </Col>
          <Col xs={24} md={12}>
            <Card title="24小时跌幅榜 TOP5" variant="outlined" style={{ height: '100%' }}>
              {stats?.day1 ? (
                <Table
                  dataSource={stats.day1
                    .filter(item => item.change_percentage < 0)
                    .slice(0, 5)
                    .map(item => ({ ...item, key: `1d-down-${item.id}` }))}
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
                      dataIndex: 'change_percentage',
                      key: 'change_percentage',
                      width: '30%',
                      align: 'right',
                      render: (value) => <ChangePercentageCard value={value} />,
                    },
                    {
                      title: '价格',
                      key: 'price',
                      width: '30%',
                      align: 'right',
                      render: (_, record) => (
                        <span>
                          {formatNumber(record.old_price)} → {formatNumber(record.current_price)}
                        </span>
                      ),
                    },
                  ]}
                  pagination={false}
                  size="small"
                  locale={{
                    emptyText: '暂无数据'
                  }}
                />
              ) : (
                <Empty description="暂无数据" />
              )}
            </Card>
          </Col>
        </Row>

        {/* 价格走势图 */}
        <Card title="市场整体价格走势" variant="outlined">
          {trendError ? (
            <Alert
              message="数据获取失败"
              description={trendError}
              type="error"
              showIcon
            />
          ) : priceTrend.length > 0 ? (
            <div style={{ height: '700px', position: 'relative' }}>
              <ReactECharts
                ref={chartRef}
                option={getKLineOption(priceTrend)}
                style={{ height: '100%' }}
                notMerge={true}
                lazyUpdate={true}
                onEvents={{
                  datazoom: (params: any) => {
                    const chartInstance = (params.batch ? params.batch[0] : params) as {start: number; end: number};
                    const startIndex = Math.floor(priceTrend.length * chartInstance.start / 100);
                    const endIndex = Math.floor(priceTrend.length * chartInstance.end / 100);
                    
                    // 获取当前图表实例
                    const echartsInstance = chartRef.current?.getEchartsInstance();
                    if (!echartsInstance) return;
                    
                    // 计算新的Y轴范围
                    const newRange = calculateYAxisRange(priceTrend, startIndex, endIndex);
                    
                    // 更新Y轴配置
                    echartsInstance.setOption({
                      yAxis: {
                        min: newRange.min,
                        max: newRange.max
                      }
                    });
                  }
                }}
              />
            </div>
          ) : (
            <Empty description="暂无数据" />
          )}
        </Card>
      </Content>
    </Layout>
  );
}

