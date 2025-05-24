# 市场数据数据库结构说明

## SQLite 数据库 (market.db)

SQLite数据库包含两个主要表：`ask` 和 `bid`，分别存储询价和售价数据。

### ask 表
- 存储市场询价数据
- 第一列为时间戳（INTEGER类型）
- 其余列为各个商品的价格（INTEGER或REAL类型）
- 列名即为商品名称

### bid 表
- 存储市场售价数据
- 结构与ask表相同
- 第一列为时间戳（INTEGER类型）
- 其余列为各个商品的价格（INTEGER或REAL类型）
- 列名即为商品名称

## MySQL 数据库

MySQL数据库包含两个表：`items` 和 `prices`，用于规范化存储市场数据。

### items 表
```sql
CREATE TABLE items (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    UNIQUE KEY (name)
)
```
- `id`: 自增主键
- `name`: 商品名称，唯一索引
- 用于存储所有商品的基本信息

### prices 表
```sql
CREATE TABLE prices (
    id INT PRIMARY KEY AUTO_INCREMENT,
    timestamp INT NOT NULL,
    item_id INT NOT NULL,
    price DOUBLE NOT NULL,
    type ENUM('ask', 'bid') NOT NULL,
    INDEX (timestamp, item_id, type)
)
```
- `id`: 自增主键
- `timestamp`: 时间戳
- `item_id`: 商品ID，关联items表
- `price`: 价格
- `type`: 价格类型（ask或bid）
- 包含复合索引 (timestamp, item_id, type)

## 数据同步说明

1. 商品表（items）采用增量同步策略：
   - 使用 INSERT IGNORE 语句
   - 只添加新的商品，不修改现有商品

2. 价格表（prices）采用全量同步策略：
   - 每次同步前清空表
   - 重新导入所有价格数据
   - 使用批量插入提高性能

## 注意事项

1. SQLite数据库中的价格列可能是INTEGER或REAL类型，在同步时会统一转换为DOUBLE类型
2. 时间戳使用INTEGER类型存储
3. 价格类型使用ENUM类型限制为'ask'或'bid'
4. 使用批量插入（每1000条数据一批）提高性能 