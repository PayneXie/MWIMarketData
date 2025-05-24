'use client';

import { useEffect, useState } from 'react';
import { Card, List, Typography, Spin, Alert, Input, Space, Layout } from 'antd';
import { SearchOutlined } from '@ant-design/icons';

const { Title } = Typography;
const { Content } = Layout;

interface Item {
  id: number;
  name: string;
  name_cn: string;
}

export default function ItemsPage() {
  const [items, setItems] = useState<Item[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchText, setSearchText] = useState('');

  useEffect(() => {
    const fetchItems = async () => {
      try {
        const response = await fetch('http://localhost:5000/api/items');
        const data = await response.json();
        
        if (data.code === 0) {
          setItems(data.data);
        } else {
          setError(data.message);
        }
      } catch (err) {
        setError('获取商品列表失败');
      } finally {
        setLoading(false);
      }
    };

    fetchItems();
  }, []);

  // 过滤商品列表
  const filteredItems = items.filter(item => 
    item.name.toLowerCase().includes(searchText.toLowerCase()) ||
    item.name_cn.toLowerCase().includes(searchText.toLowerCase())
  );

  if (loading) {
    return (
      <Layout style={{ minHeight: '100vh', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
        <Spin>
          <div style={{ padding: '32px', background: '#fff', borderRadius: '8px' }}>
            <div style={{ fontSize: '16px' }}>加载中...</div>
          </div>
        </Spin>
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

  return (
    <Layout>
      <Content style={{ padding: '24px', maxWidth: '1200px', margin: '0 auto' }}>
        <Space direction="vertical" size="large" style={{ width: '100%' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Title level={2}>商品列表</Title>
            <Input
              placeholder="搜索商品名称"
              prefix={<SearchOutlined />}
              onChange={e => setSearchText(e.target.value)}
              style={{ width: 300 }}
            />
          </div>

          <List
            grid={{
              gutter: 16,
              xs: 1,
              sm: 2,
              md: 3,
              lg: 3,
              xl: 4,
              xxl: 4,
            }}
            dataSource={filteredItems}
            renderItem={item => (
              <List.Item>
                <Card hoverable>
                  <div style={{ display: 'flex', flexDirection: 'column' }}>
                    <Typography.Text strong>{item.name}</Typography.Text>
                    <Typography.Text type="secondary">{item.name_cn}</Typography.Text>
                  </div>
                </Card>
              </List.Item>
            )}
          />
        </Space>
      </Content>
    </Layout>
  );
} 