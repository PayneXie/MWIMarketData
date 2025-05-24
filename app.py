"""
Flask应用主文件

功能说明：
1. 提供RESTful API接口
2. 处理数据库查询
3. 返回JSON格式数据
4. 集成Swagger文档
5. 定期同步市场数据

API接口：
1. GET /api/items - 获取商品列表
2. GET /api/items/<item_id>/prices - 获取商品价格历史数据
"""

from flask import Flask, jsonify, request
from flasgger import Swagger, swag_from
import mysql.connector
import json
from datetime import datetime
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sync_latest import sync_latest_data

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 初始化后台调度器
scheduler = BackgroundScheduler()

def sync_market_data():
    """同步市场数据
    
    从API获取最新的市场数据并同步到数据库
    """
    try:
        logger.info('开始同步市场数据...')
        sync_latest_data()
        logger.info('市场数据同步完成')
    except Exception as e:
        logger.error(f'同步市场数据时发生错误: {e}')

# 启动调度器
scheduler.start()

# 添加定期同步数据的任务
scheduler.add_job(
    func=sync_market_data,
    trigger=IntervalTrigger(hours=6),  # 每6小时同步一次
    id='sync_market_data',
    next_run_time=datetime.now()  # 立即执行一次
)

app = Flask(__name__)

# Swagger配置
swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": 'apispec',
            "route": '/apispec.json',
            "rule_filter": lambda rule: True,
            "model_filter": lambda tag: True,
        }
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/"
}

swagger_template = {
    "swagger": "2.0",
    "info": {
        "title": "MWI Market Data API",
        "description": "市场数据查询接口",
        "version": "1.0.0",
        "contact": {
            "name": "API Support",
            "email": "your-email@example.com"
        }
    },
    "basePath": "/",
    "schemes": [
        "http",
        "https"
    ],
    "consumes": [
        "application/json"
    ],
    "produces": [
        "application/json"
    ]
}

swagger = Swagger(app, config=swagger_config, template=swagger_template)

def get_db_connection():
    """获取数据库连接"""
    with open('database.json', 'r') as f:
        db_config = json.load(f)['mysql']
    
    return mysql.connector.connect(
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database']
    )

@app.route('/api/items', methods=['GET'])
@swag_from({
    'tags': ['商品'],
    'summary': '获取商品列表',
    'description': '获取所有商品的列表，按名称排序',
    'responses': {
        200: {
            'description': '成功获取商品列表',
            'schema': {
                'type': 'object',
                'properties': {
                    'code': {
                        'type': 'integer',
                        'description': '状态码，0表示成功'
                    },
                    'message': {
                        'type': 'string',
                        'description': '状态描述'
                    },
                    'data': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'id': {
                                    'type': 'integer',
                                    'description': '商品ID'
                                },
                                'name': {
                                    'type': 'string',
                                    'description': '商品名称'
                                }
                            }
                        }
                    }
                }
            }
        },
        500: {
            'description': '服务器错误',
            'schema': {
                'type': 'object',
                'properties': {
                    'code': {
                        'type': 'integer',
                        'description': '错误码'
                    },
                    'message': {
                        'type': 'string',
                        'description': '错误信息'
                    },
                    'data': {
                        'type': 'null'
                    }
                }
            }
        }
    }
})
def get_items():
    """获取商品列表
    
    返回格式：
    {
        "code": 0,  # 0表示成功，非0表示错误
        "message": "success",  # 状态描述
        "data": [  # 商品列表
            {
                "id": 1,
                "name": "商品名称"
            },
            ...
        ]
    }
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 查询所有商品
        cursor.execute("SELECT id, name FROM items ORDER BY id")
        items = cursor.fetchall()
        
        return jsonify({
            "code": 0,
            "message": "success",
            "data": items
        })
        
    except Exception as e:
        return jsonify({
            "code": 500,
            "message": str(e),
            "data": None
        }), 500
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@app.route('/api/items/<int:item_id>/prices', methods=['GET'])
@swag_from({
    'tags': ['商品'],
    'summary': '获取商品价格历史数据',
    'description': '获取指定商品在特定时间段内的价格历史数据',
    'parameters': [
        {
            'name': 'item_id',
            'in': 'path',
            'type': 'integer',
            'required': True,
            'description': '商品ID'
        },
        {
            'name': 'start_time',
            'in': 'query',
            'type': 'integer',
            'required': True,
            'description': '开始时间戳（Unix时间戳）'
        },
        {
            'name': 'end_time',
            'in': 'query',
            'type': 'integer',
            'required': True,
            'description': '结束时间戳（Unix时间戳）'
        },
        {
            'name': 'type',
            'in': 'query',
            'type': 'string',
            'enum': ['ask', 'bid', 'all'],
            'default': 'all',
            'description': '价格类型：ask(询价)、bid(售价)、all(全部)'
        }
    ],
    'responses': {
        200: {
            'description': '成功获取价格历史数据',
            'schema': {
                'type': 'object',
                'properties': {
                    'code': {
                        'type': 'integer',
                        'description': '状态码，0表示成功'
                    },
                    'message': {
                        'type': 'string',
                        'description': '状态描述'
                    },
                    'data': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'timestamp': {
                                    'type': 'integer',
                                    'description': '时间戳'
                                },
                                'price': {
                                    'type': 'number',
                                    'description': '价格'
                                },
                                'type': {
                                    'type': 'string',
                                    'description': '价格类型：ask或bid'
                                }
                            }
                        }
                    }
                }
            }
        },
        400: {
            'description': '请求参数错误',
            'schema': {
                'type': 'object',
                'properties': {
                    'code': {
                        'type': 'integer',
                        'description': '错误码'
                    },
                    'message': {
                        'type': 'string',
                        'description': '错误信息'
                    },
                    'data': {
                        'type': 'null'
                    }
                }
            }
        },
        500: {
            'description': '服务器错误',
            'schema': {
                'type': 'object',
                'properties': {
                    'code': {
                        'type': 'integer',
                        'description': '错误码'
                    },
                    'message': {
                        'type': 'string',
                        'description': '错误信息'
                    },
                    'data': {
                        'type': 'null'
                    }
                }
            }
        }
    }
})
def get_item_prices(item_id):
    """获取商品价格历史数据
    
    返回格式：
    {
        "code": 0,  # 0表示成功，非0表示错误
        "message": "success",  # 状态描述
        "data": [  # 价格历史数据
            {
                "timestamp": 1234567890,  # 时间戳
                "price": 100.5,  # 价格
                "type": "ask"  # 价格类型：ask或bid
            },
            ...
        ]
    }
    """
    try:
        # 获取查询参数
        start_time = request.args.get('start_time')
        end_time = request.args.get('end_time')
        price_type = request.args.get('type', 'all')
        
        # 验证参数
        if not start_time or not end_time:
            return jsonify({
                "code": 400,
                "message": "缺少必要参数：start_time和end_time",
                "data": None
            }), 400
            
        try:
            start_time = int(start_time)
            end_time = int(end_time)
        except ValueError:
            return jsonify({
                "code": 400,
                "message": "时间戳必须是整数",
                "data": None
            }), 400
            
        if start_time > end_time:
            return jsonify({
                "code": 400,
                "message": "开始时间不能大于结束时间",
                "data": None
            }), 400
            
        if price_type not in ['ask', 'bid', 'all']:
            return jsonify({
                "code": 400,
                "message": "价格类型必须是ask、bid或all",
                "data": None
            }), 400
        
        # 连接数据库
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 构建SQL查询
        sql = """
            SELECT timestamp, price, type
            FROM prices
            WHERE item_id = %s
            AND timestamp BETWEEN %s AND %s
        """
        params = [item_id, start_time, end_time]
        
        if price_type != 'all':
            sql += " AND type = %s"
            params.append(price_type)
            
        sql += " ORDER BY timestamp"
        
        # 执行查询
        cursor.execute(sql, params)
        prices = cursor.fetchall()
        
        return jsonify({
            "code": 0,
            "message": "success",
            "data": prices
        })
        
    except Exception as e:
        return jsonify({
            "code": 500,
            "message": str(e),
            "data": None
        }), 500
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    app.run(debug=True)
