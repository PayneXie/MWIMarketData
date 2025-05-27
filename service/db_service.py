"""数据库服务模块

提供数据库连接和查询功能
"""

import mysql.connector
import json
import logging
import os

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """获取数据库连接"""
    # 获取项目根目录路径
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    config_path = os.path.join(project_root, 'database.json')
    
    # 读取数据库配置
    with open(config_path, 'r') as f:
        config = json.load(f)

    try:
        return mysql.connector.connect(
            host=config['mysql']['host'],
            port=int(config['mysql']['port']),
            user=config['mysql']['user'],
            password=config['mysql']['password'],
            database=config['mysql']['database'],
            charset='utf8mb4',
            use_unicode=True,
            get_warnings=True,
            raise_on_warnings=True,
            autocommit=True
        )
    except Exception as e:
        logger.error(f'数据库连接失败: {e}')
        raise

def execute_query(query, params=None, fetch=True, dictionary=True):
    """执行SQL查询
    
    Args:
        query: SQL查询语句
        params: 查询参数
        fetch: 是否获取结果
        dictionary: 是否以字典形式返回结果
    
    Returns:
        如果fetch为True，返回查询结果
        如果fetch为False，返回受影响的行数
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=dictionary)
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        if fetch:
            result = cursor.fetchall()
        else:
            result = cursor.rowcount
            conn.commit()
        
        return result
    except Exception as e:
        logger.error(f'查询执行失败: {e}\nSQL: {query}\n参数: {params}')
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def execute_many(query, params):
    """批量执行SQL查询
    
    Args:
        query: SQL查询语句
        params: 参数列表
    
    Returns:
        受影响的行数
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.executemany(query, params)
        conn.commit()
        
        return cursor.rowcount
    except Exception as e:
        logger.error(f'批量查询执行失败: {e}\nSQL: {query}\n参数: {params}')
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()