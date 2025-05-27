"""商品相关的API接口服务

提供以下接口：
1. GET /api/v1/items - 获取商品列表
2. GET /api/v1/items/<int:item_id>/price-history - 获取商品价格历史
"""

from flask import jsonify
from flasgger import swag_from
import logging
from datetime import datetime, timedelta
from . import db_service

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_items():
    """获取商品列表"""
    try:
        query = """
            SELECT 
                i.id,
                i.name,
                i.name_cn,
                p.price as current_price,
                p.timestamp as price_updated_at
            FROM items i
            LEFT JOIN (
                SELECT 
                    item_id,
                    price,
                    timestamp
                FROM prices
                WHERE (item_id, timestamp) IN (
                    SELECT 
                        item_id,
                        MAX(timestamp)
                    FROM prices
                    WHERE type = 'ask'
                    GROUP BY item_id
                )
            ) p ON i.id = p.item_id
            ORDER BY i.id
        """
        
        results = db_service.execute_query(query)
        
        # 处理数据
        items = [{
            'id': row['id'],
            'name': row['name'],
            'name_cn': row['name_cn'],
            'current_price': float(row['current_price']) if row['current_price'] else None,
            'price_updated_at': row['price_updated_at']
        } for row in results]
        
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': items
        })
        
    except Exception as e:
        logger.error(f'获取商品列表失败: {e}')
        return jsonify({
            'code': 1,
            'message': str(e)
        }), 500

def get_item_prices(item_id):
    """获取商品价格历史"""
    try:
        # 获取最近7天的数据
        end_time = datetime.now()
        start_time = end_time - timedelta(days=7)
        
        # 获取商品信息
        item_query = "SELECT name, name_cn FROM items WHERE id = %s"
        item_result = db_service.execute_query(item_query, (item_id,))
        
        if not item_result:
            return jsonify({
                'code': 1,
                'message': '商品不存在'
            }), 404
        
        item = item_result[0]
        
        # 获取价格历史
        price_query = """
            WITH price_data AS (
                SELECT 
                    timestamp,
                    price,
                    type,
                    LAG(price) OVER (PARTITION BY type ORDER BY timestamp) as prev_price,
                    AVG(price) OVER (
                        PARTITION BY type
                        ORDER BY timestamp 
                        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
                    ) as trend_price
                FROM prices 
                WHERE item_id = %s
                AND timestamp BETWEEN %s AND %s
                AND price > 0
            )
            SELECT 
                timestamp,
                type,
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
                END as price
            FROM price_data
            ORDER BY timestamp
        """
        
        price_results = db_service.execute_query(
            price_query, 
            (item_id, int(start_time.timestamp()), int(end_time.timestamp()))
        )
        
        # 处理数据
        prices = [{
            'timestamp': row['timestamp'],
            'price': float(row['price']),
            'type': row['type']
        } for row in price_results]
        
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': {
                'item_id': item_id,
                'name': item['name'],
                'name_cn': item['name_cn'],
                'prices': prices
            }
        })
        
    except Exception as e:
        logger.error(f'获取商品价格历史失败: {e}')
        return jsonify({
            'code': 1,
            'message': str(e)
        }), 500

def register_routes(app):
    """注册路由"""
    # 获取商品列表
    app.route('/api/v1/items')(
        swag_from({
            'tags': ['商品'],
            'summary': '获取商品列表',
            'description': '获取所有商品的基本信息和当前价格',
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
                                        },
                                        'current_price': {
                                            'type': 'number',
                                            'description': '当前价格'
                                        },
                                        'price_updated_at': {
                                            'type': 'integer',
                                            'description': '价格更新时间戳'
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
        })(get_items)
    )
    
    # 获取商品价格历史
    app.route('/api/v1/items/<int:item_id>/price-history')(
        swag_from({
            'tags': ['商品'],
            'summary': '获取商品价格历史',
            'description': '获取指定商品的价格历史数据',
            'parameters': [
                {
                    'name': 'item_id',
                    'in': 'path',
                    'type': 'integer',
                    'required': True,
                    'description': '商品ID'
                }
            ],
            'responses': {
                200: {
                    'description': '成功获取价格历史',
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
                                    'prices': {
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
                                                    'description': '价格类型：ask(卖价)或bid(买价)'
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                404: {
                    'description': '商品不存在',
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
        })(get_item_prices)
    )