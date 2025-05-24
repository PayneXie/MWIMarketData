"""
从API同步最新的市场数据到MySQL数据库

功能说明：
1. 从API获取最新的市场数据
2. 将数据同步到MySQL数据库
3. 商品表(items)采用增量同步策略
4. 价格表(prices)采用增量同步策略，只添加最新时间戳的数据
5. 检查时间戳是否已存在，避免重复同步

使用方法：
1. 确保已安装所需依赖（mysql-connector-python, requests）
2. 配置好database.json中的MySQL连接信息
3. 运行脚本即可开始数据同步

注意事项：
1. 需要MySQL数据库连接
2. 需要网络连接
3. 只同步最新时间戳的数据
4. 使用批量插入提高性能
5. 避免重复同步相同时间戳的数据
"""

import json
import requests
import mysql.connector
from datetime import datetime
from tqdm import tqdm

def sync_latest_data():
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

        # 从API获取最新数据
        print("正在从API获取最新数据...")
        response = requests.get('https://raw.githubusercontent.com/holychikenz/MWIApi/main/milkyapi.json')
        if response.status_code != 200:
            raise Exception(f"API请求失败: {response.status_code}")
        
        data = response.json()
        market_data = data['market']
        timestamp = int(data['time'])

        # 检查时间戳是否已存在
        mysql_cursor.execute(
            "SELECT COUNT(*) FROM prices WHERE timestamp = %s",
            (timestamp,)
        )
        if mysql_cursor.fetchone()[0] > 0:
            print(f"时间戳 {datetime.fromtimestamp(timestamp)} 的数据已存在，跳过同步。")
            return

        # 获取所有商品名称
        all_items = set(market_data.keys())

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

        # 准备批量插入
        batch_size = 1000
        batch_data = []

        # 处理每个商品的价格数据
        print("同步价格数据...")
        for item_name, prices in tqdm(market_data.items(), desc="处理价格数据"):
            item_id = item_id_map[item_name]
            
            # 处理ask价格
            if prices['ask'] != -1:
                batch_data.append((timestamp, item_id, float(prices['ask']), 'ask'))
            
            # 处理bid价格
            if prices['bid'] != -1:
                batch_data.append((timestamp, item_id, float(prices['bid']), 'bid'))
            
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

        print(f"\n数据同步完成。时间戳: {datetime.fromtimestamp(timestamp)}")

    except Exception as e:
        print(f"发生错误: {e}")

    finally:
        # 关闭连接
        if 'mysql_cursor' in locals():
            mysql_cursor.close()
        if 'mysql_conn' in locals():
            mysql_conn.close()

if __name__ == '__main__':
    sync_latest_data() 