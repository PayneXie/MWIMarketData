"""
将SQLite数据库中的市场数据同步到MySQL数据库

功能说明：
1. 读取SQLite数据库中的ask和bid表数据
2. 将数据规范化存储到MySQL数据库中
3. 商品表(items)采用增量同步策略
4. 价格表(prices)采用全量同步策略

使用方法：
1. 确保已安装所需依赖（mysql-connector-python, tqdm）
2. 配置好database.json中的MySQL连接信息
3. 运行脚本即可开始数据同步
4. 同步过程中会显示进度条

注意事项：
1. 需要MySQL数据库连接
2. 价格表会在同步前清空
3. 商品表只添加新商品，不修改现有商品
4. 使用批量插入提高性能
"""

import json
import sqlite3
import mysql.connector
import os
from tqdm import tqdm

try:
    # 读取 database.json 获取连接信息
    with open('database.json', 'r') as f:
        db_config = json.load(f)['mysql']

    # 连接到 MySQL 数据库
    try:
        mysql_conn = mysql.connector.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        mysql_cursor = mysql_conn.cursor()
        print("MySQL 数据库连接成功。")
    except mysql.connector.Error as err:
        print(f"MySQL 连接失败: {err}")
        raise

    # 连接到 SQLite 数据库
    sqlite_conn = sqlite3.connect('static/db/market.db')
    sqlite_cursor = sqlite_conn.cursor()
    print("SQLite 数据库连接成功。")

    # 创建新表
    mysql_cursor.execute("""
        CREATE TABLE IF NOT EXISTS items (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL,
            UNIQUE KEY (name)
        )
    """)

    mysql_cursor.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            id INT PRIMARY KEY AUTO_INCREMENT,
            timestamp INT NOT NULL,
            item_id INT NOT NULL,
            price DOUBLE NOT NULL,
            type ENUM('ask', 'bid') NOT NULL,
            INDEX (timestamp, item_id, type)
        )
    """)

    # 获取 SQLite 数据库中的所有表名
    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = sqlite_cursor.fetchall()
    print(f"找到的表: {tables}")

    # 清空价格表
    print("清空价格表...")
    mysql_cursor.execute("TRUNCATE TABLE prices")
    mysql_conn.commit()
    print("价格表已清空")

    # 获取所有商品名称
    all_items = set()
    for table_name in tables:
        table_name = table_name[0]
        sqlite_cursor.execute(f"PRAGMA table_info({table_name});")
        columns = sqlite_cursor.fetchall()
        for col in columns[1:]:  # 跳过第一列（时间戳）
            all_items.add(col[1])

    # 增量同步商品表
    print("同步商品表...")
    for item_name in tqdm(all_items, desc="同步商品"):
        mysql_cursor.execute(
            "INSERT IGNORE INTO items (name) VALUES (%s)",
            (item_name,)
        )
    mysql_conn.commit()
    print("商品表同步完成")

    # 获取商品ID映射
    mysql_cursor.execute("SELECT id, name FROM items")
    item_id_map = {name: id for id, name in mysql_cursor.fetchall()}

    # 处理每个表的数据
    for table_name in tables:
        table_name = table_name[0]
        print(f"\n处理表: {table_name}")
        
        # 获取表结构
        sqlite_cursor.execute(f"PRAGMA table_info({table_name});")
        columns = sqlite_cursor.fetchall()
        
        # 获取所有数据
        sqlite_cursor.execute(f"SELECT * FROM {table_name};")
        rows = sqlite_cursor.fetchall()
        
        # 准备批量插入
        batch_size = 1000
        batch_data = []
        
        # 处理每一行数据
        for row in tqdm(rows, desc=f"处理{table_name}表数据"):
            timestamp = row[0]  # 第一列是时间戳
            
            # 处理每个商品的价格
            for i in range(1, len(row)):
                if row[i] is not None:  # 只处理非空价格
                    item_name = columns[i][1]  # 获取商品名称
                    price = float(row[i])  # 确保价格是浮点数
                    item_id = item_id_map[item_name]
                    
                    batch_data.append((timestamp, item_id, price, table_name))
                    
                    # 当批次达到指定大小时执行插入
                    if len(batch_data) >= batch_size:
                        mysql_cursor.executemany(
                            "INSERT INTO prices (timestamp, item_id, price, type) VALUES (%s, %s, %s, %s)",
                            batch_data
                        )
                        mysql_conn.commit()
                        batch_data = []
        
        # 插入剩余的数据
        if batch_data:
            mysql_cursor.executemany(
                "INSERT INTO prices (timestamp, item_id, price, type) VALUES (%s, %s, %s, %s)",
                batch_data
            )
            mysql_conn.commit()

    print("\n数据导入完成。")

except Exception as e:
    print(f"发生错误: {e}")

finally:
    # 关闭连接
    if 'mysql_cursor' in locals():
        mysql_cursor.close()
    if 'mysql_conn' in locals():
        mysql_conn.close()
    if 'sqlite_cursor' in locals():
        sqlite_cursor.close()
    if 'sqlite_conn' in locals():
        sqlite_conn.close()
