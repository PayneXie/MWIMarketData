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
from flask_cors import CORS
import mysql.connector
import json
import os
from datetime import datetime, timedelta
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
#scheduler.start()

# 添加定期同步数据的任务
scheduler.add_job(
    func=sync_market_data,
    trigger=IntervalTrigger(hours=6),  # 每6小时同步一次
    id='sync_market_data',
    next_run_time=datetime.now()  # 立即执行一次
)

app = Flask(__name__)
# 配置CORS，允许所有路由支持跨域
CORS(app, resources={r"/api/*": {"origins": ["http://localhost:3000"]}})

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
                                },
                                'name_cn': {
                                    'type': 'string',
                                    'description': '商品中文名称'
                                }
                            }
                        }
                    }
                }
            }
        }
    }
})
def get_items():
    """获取所有商品列表"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 获取所有商品
        cursor.execute("""
            SELECT id, name, name_cn
            FROM items 
            ORDER BY name
        """)
        items = cursor.fetchall()
        
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': items
        })
        
    except Exception as e:
        return jsonify({
            'code': 500,
            'message': f'获取商品列表失败: {str(e)}'
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

@app.route('/api/prices/stats')
@swag_from({
    'tags': ['价格'],
    'summary': '获取市场统计数据',
    'description': '获取价格变化幅度最大的商品统计，包括7天和24小时的变化',
    'responses': {
        200: {
            'description': '成功获取统计数据',
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
                        'type': 'object',
                        'properties': {
                            'top_increase_7d': {
                                'type': 'array',
                                'description': '7天内涨价幅度最高的商品',
                                'items': {
                                    'type': 'object',
                                    'properties': {
                                        'item_id': {
                                            'type': 'integer',
                                            'description': '商品ID'
                                        },
                                        'name': {
                                            'type': 'string',
                                            'description': '商品名称'
                                        },
                                        'name_cn': {
                                            'type': 'string',
                                            'description': '商品中文名称'
                                        },
                                        'price_change': {
                                            'type': 'number',
                                            'description': '价格变化幅度(%)'
                                        },
                                        'start_price': {
                                            'type': 'number',
                                            'description': '起始价格'
                                        },
                                        'end_price': {
                                            'type': 'number',
                                            'description': '当前价格'
                                        }
                                    }
                                }
                            },
                            'top_decrease_7d': {
                                'type': 'array',
                                'description': '7天内跌价幅度最高的商品',
                                'items': {
                                    'type': 'object',
                                    'properties': {
                                        'item_id': {
                                            'type': 'integer',
                                            'description': '商品ID'
                                        },
                                        'name': {
                                            'type': 'string',
                                            'description': '商品名称'
                                        },
                                        'name_cn': {
                                            'type': 'string',
                                            'description': '商品中文名称'
                                        },
                                        'price_change': {
                                            'type': 'number',
                                            'description': '价格变化幅度(%)'
                                        },
                                        'start_price': {
                                            'type': 'number',
                                            'description': '起始价格'
                                        },
                                        'end_price': {
                                            'type': 'number',
                                            'description': '当前价格'
                                        }
                                    }
                                }
                            },
                            'top_increase_24h': {
                                'type': 'array',
                                'description': '24小时内涨价幅度最高的商品',
                                'items': {
                                    'type': 'object',
                                    'properties': {
                                        'item_id': {
                                            'type': 'integer',
                                            'description': '商品ID'
                                        },
                                        'name': {
                                            'type': 'string',
                                            'description': '商品名称'
                                        },
                                        'name_cn': {
                                            'type': 'string',
                                            'description': '商品中文名称'
                                        },
                                        'price_change': {
                                            'type': 'number',
                                            'description': '价格变化幅度(%)'
                                        },
                                        'start_price': {
                                            'type': 'number',
                                            'description': '起始价格'
                                        },
                                        'end_price': {
                                            'type': 'number',
                                            'description': '当前价格'
                                        }
                                    }
                                }
                            },
                            'top_decrease_24h': {
                                'type': 'array',
                                'description': '24小时内跌价幅度最高的商品',
                                'items': {
                                    'type': 'object',
                                    'properties': {
                                        'item_id': {
                                            'type': 'integer',
                                            'description': '商品ID'
                                        },
                                        'name': {
                                            'type': 'string',
                                            'description': '商品名称'
                                        },
                                        'name_cn': {
                                            'type': 'string',
                                            'description': '商品中文名称'
                                        },
                                        'price_change': {
                                            'type': 'number',
                                            'description': '价格变化幅度(%)'
                                        },
                                        'start_price': {
                                            'type': 'number',
                                            'description': '起始价格'
                                        },
                                        'end_price': {
                                            'type': 'number',
                                            'description': '当前价格'
                                        }
                                    }
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
                    }
                }
            }
        }
    }
})
def get_price_stats():
    try:
        # 获取时间范围
        end_time = datetime.now()
        start_time_7d = end_time - timedelta(days=7)
        start_time_24h = end_time - timedelta(hours=24)
        
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 获取7天价格变化
        query_7d = """
            WITH price_data AS (
                SELECT 
                    p.item_id,
                    i.name,
                    i.name_cn,
                    p.timestamp,
                    p.price,
                    LAG(p.price) OVER (PARTITION BY p.item_id ORDER BY p.timestamp) as prev_price,
                    AVG(p.price) OVER (
                        PARTITION BY p.item_id
                        ORDER BY p.timestamp 
                        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
                    ) as trend_price,
                    AVG(p.price) OVER (PARTITION BY p.item_id) as avg_price
                FROM prices p
                JOIN items i ON p.item_id = i.id
                WHERE p.timestamp BETWEEN %s AND %s
                AND p.price > 0
                AND p.type = 'ask'
            ),
            cleaned_prices AS (
                SELECT 
                    item_id,
                    name,
                    name_cn,
                    timestamp,
                    CASE 
                        WHEN prev_price IS NULL THEN price
                        WHEN price > prev_price * 2.5 OR price < prev_price * 0.6 THEN
                            CASE 
                                WHEN ABS(price - trend_price) / trend_price > 0.5 THEN
                                    prev_price
                                ELSE 
                                    price
                            END
                        ELSE 
                            price
                    END as cleaned_price,
                    avg_price
                FROM price_data
            ),
            price_changes AS (
                SELECT 
                    item_id,
                    name,
                    name_cn,
                    MIN(timestamp) as first_timestamp,
                    MAX(timestamp) as last_timestamp,
                    avg_price
                FROM cleaned_prices
                GROUP BY item_id, name, name_cn, avg_price
            ),
            first_prices AS (
                SELECT cp.item_id, cp.cleaned_price as first_price
                FROM cleaned_prices cp
                JOIN price_changes pc ON cp.item_id = pc.item_id AND cp.timestamp = pc.first_timestamp
            ),
            last_prices AS (
                SELECT cp.item_id, cp.cleaned_price as last_price
                FROM cleaned_prices cp
                JOIN price_changes pc ON cp.item_id = pc.item_id AND cp.timestamp = pc.last_timestamp
            )
            SELECT 
                pc.item_id,
                pc.name,
                pc.name_cn,
                fp.first_price as start_price,
                lp.last_price as end_price,
                CASE 
                    WHEN fp.first_price > 0 THEN ((lp.last_price - fp.first_price) / fp.first_price * 100)
                    ELSE 0 
                END as price_change
            FROM price_changes pc
            JOIN first_prices fp ON pc.item_id = fp.item_id
            JOIN last_prices lp ON pc.item_id = lp.item_id
            WHERE fp.first_price > 0 AND lp.last_price > 0
            AND (
                (lp.last_price BETWEEN pc.avg_price * 0.5 AND pc.avg_price * 2.0)
                AND (fp.first_price BETWEEN pc.avg_price * 0.5 AND pc.avg_price * 2.0)
                AND ABS((lp.last_price - fp.first_price) / fp.first_price) <= 2.0
            )
            ORDER BY price_change DESC
            LIMIT 5
        """
        
        # 获取24小时价格变化
        query_24h = """
            WITH price_data AS (
                SELECT 
                    p.item_id,
                    i.name,
                    i.name_cn,
                    p.timestamp,
                    p.price,
                    LAG(p.price) OVER (PARTITION BY p.item_id ORDER BY p.timestamp) as prev_price,
                    AVG(p.price) OVER (
                        PARTITION BY p.item_id
                        ORDER BY p.timestamp 
                        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
                    ) as trend_price,
                    AVG(p.price) OVER (PARTITION BY p.item_id) as avg_price
                FROM prices p
                JOIN items i ON p.item_id = i.id
                WHERE p.timestamp BETWEEN %s AND %s
                AND p.price > 0
                AND p.type = 'ask'
            ),
            cleaned_prices AS (
                SELECT 
                    item_id,
                    name,
                    name_cn,
                    timestamp,
                    CASE 
                        WHEN prev_price IS NULL THEN price
                        WHEN price > prev_price * 2.5 OR price < prev_price * 0.6 THEN
                            CASE 
                                WHEN ABS(price - trend_price) / trend_price > 0.5 THEN
                                    prev_price
                                ELSE 
                                    price
                            END
                        ELSE 
                            price
                    END as cleaned_price,
                    avg_price
                FROM price_data
            ),
            price_changes AS (
                SELECT 
                    item_id,
                    name,
                    name_cn,
                    MIN(timestamp) as first_timestamp,
                    MAX(timestamp) as last_timestamp,
                    avg_price
                FROM cleaned_prices
                GROUP BY item_id, name, name_cn, avg_price
            ),
            first_prices AS (
                SELECT cp.item_id, cp.cleaned_price as first_price
                FROM cleaned_prices cp
                JOIN price_changes pc ON cp.item_id = pc.item_id AND cp.timestamp = pc.first_timestamp
            ),
            last_prices AS (
                SELECT cp.item_id, cp.cleaned_price as last_price
                FROM cleaned_prices cp
                JOIN price_changes pc ON cp.item_id = pc.item_id AND cp.timestamp = pc.last_timestamp
            )
            SELECT 
                pc.item_id,
                pc.name,
                pc.name_cn,
                fp.first_price as start_price,
                lp.last_price as end_price,
                CASE 
                    WHEN fp.first_price > 0 THEN ((lp.last_price - fp.first_price) / fp.first_price * 100)
                    ELSE 0 
                END as price_change
            FROM price_changes pc
            JOIN first_prices fp ON pc.item_id = fp.item_id
            JOIN last_prices lp ON pc.item_id = lp.item_id
            WHERE fp.first_price > 0 AND lp.last_price > 0
            AND (
                (lp.last_price BETWEEN pc.avg_price * 0.5 AND pc.avg_price * 2.0)
                AND (fp.first_price BETWEEN pc.avg_price * 0.5 AND pc.avg_price * 2.0)
                AND ABS((lp.last_price - fp.first_price) / fp.first_price) <= 2.0
            )
            ORDER BY price_change DESC
            LIMIT 5
        """
        
        # 执行7天查询
        cursor.execute(query_7d, (int(start_time_7d.timestamp()), int(end_time.timestamp())))
        top_increase_7d = cursor.fetchall()
        
        cursor.execute(query_7d.replace('ORDER BY price_change DESC', 'ORDER BY price_change ASC'), 
                      (int(start_time_7d.timestamp()), int(end_time.timestamp())))
        top_decrease_7d = cursor.fetchall()
        
        # 执行24小时查询
        cursor.execute(query_24h, (int(start_time_24h.timestamp()), int(end_time.timestamp())))
        top_increase_24h = cursor.fetchall()
        
        cursor.execute(query_24h.replace('ORDER BY price_change DESC', 'ORDER BY price_change ASC'), 
                      (int(start_time_24h.timestamp()), int(end_time.timestamp())))
        top_decrease_24h = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # 处理数据
        def process_results(results):
            return [{
                'item_id': row['item_id'],
                'name': row['name'],
                'name_cn': row['name_cn'],
                'price_change': round(float(row['price_change']), 2),
                'start_price': round(float(row['start_price']), 2),
                'end_price': round(float(row['end_price']), 2)
            } for row in results]
        
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': {
                'top_increase_7d': process_results(top_increase_7d),
                'top_decrease_7d': process_results(top_decrease_7d),
                'top_increase_24h': process_results(top_increase_24h),
                'top_decrease_24h': process_results(top_decrease_24h)
            }
        })
        
    except Exception as e:
        return jsonify({
            'code': 1,
            'message': str(e)
        }), 500

@app.route('/api/prices/trend')
@swag_from({
    'tags': ['价格'],
    'summary': '获取市场整体价格走势',
    'description': '获取最近7天的市场整体价格走势数据，用于展示折线图',
    'parameters': [
        {
            'name': 'interval',
            'in': 'query',
            'type': 'string',
            'enum': ['hour', 'day'],
            'default': 'hour',
            'description': '数据聚合间隔：hour(小时)或day(天)'
        }
    ],
    'responses': {
        200: {
            'description': '成功获取价格走势数据',
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
                                'date': {
                                    'type': 'string',
                                    'description': '时间点'
                                },
                                'price': {
                                    'type': 'number',
                                    'description': '平均价格'
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
                    }
                }
            }
        }
    }
})
def get_price_trend():
    try:
        # 获取查询参数
        interval = request.args.get('interval', 'hour')
        
        # 获取最近7天的数据
        end_time = datetime.now()
        start_time = end_time - timedelta(days=7)
        
        # 根据间隔选择不同的时间格式
        time_format = '%Y-%m-%d %H:00:00' if interval == 'hour' else '%Y-%m-%d'
        
        # 按指定间隔聚合数据
        query = f"""
            WITH price_data AS (
                SELECT 
                    timestamp,
                    price,
                    LAG(price) OVER (ORDER BY timestamp) as prev_price,
                    AVG(price) OVER (
                        ORDER BY timestamp 
                        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
                    ) as trend_price
                FROM prices 
                WHERE timestamp BETWEEN %s AND %s
                AND price > 0  -- 清洗脏数据，只保留价格大于0的记录
            ),
            cleaned_prices AS (
                SELECT 
                    timestamp,
                    CASE 
                        WHEN prev_price IS NULL THEN price  -- 第一条数据
                        WHEN price > prev_price * 2.5 OR price < prev_price * 0.6 THEN  -- 价格异常
                            CASE 
                                WHEN ABS(price - trend_price) / trend_price > 0.5 THEN  -- 与趋势预测值差异过大
                                    prev_price  -- 使用前一个有效值
                                ELSE 
                                    price  -- 保持原值
                            END
                        ELSE 
                            price  -- 正常价格
                    END as cleaned_price
                FROM price_data
            )
            SELECT 
                DATE_FORMAT(FROM_UNIXTIME(timestamp), '{time_format}') as date,
                AVG(cleaned_price) as price
            FROM cleaned_prices
            GROUP BY DATE_FORMAT(FROM_UNIXTIME(timestamp), '{time_format}')
            ORDER BY date
        """
        
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, (int(start_time.timestamp()), int(end_time.timestamp())))
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # 处理数据
        trend_data = []
        for row in results:
            trend_data.append({
                'date': row['date'],
                'price': float(row['price'])
            })
        
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': trend_data
        })
        
    except Exception as e:
        return jsonify({
            'code': 1,
            'message': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True)
