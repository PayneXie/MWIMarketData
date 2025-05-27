"""统计相关的API接口服务

提供以下接口：
1. GET /api/v1/market/statistics - 获取市场统计数据
2. GET /api/v1/market/trends - 获取市场整体价格走势
"""

from flask import jsonify
from flasgger import swag_from
import logging
from datetime import datetime, timedelta
from . import db_service
from . import trend_service

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_market_stats():
    """获取市场统计数据"""
    try:
        # 获取时间范围
        now = datetime.now()
        day7_ago = now - timedelta(days=7)
        day1_ago = datetime(now.year, now.month, now.day) - timedelta(days=1)
        
        # 记录时间范围
        logger.info(f"计算时间范围:")
        logger.info(f"now: {now} ({int(now.timestamp())})")
        logger.info(f"day7_ago: {day7_ago} ({int(day7_ago.timestamp())})")
        logger.info(f"day1_ago: {day1_ago} ({int(day1_ago.timestamp())})")
        
        # 获取7天内的价格变化
        day7_query = """
            WITH latest_prices AS (
                SELECT 
                    item_id,
                    price,
                    ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY timestamp DESC) as rn
                FROM prices
                WHERE type = 'ask'
                AND timestamp >= %s
            ),
            earliest_prices AS (
                SELECT 
                    item_id,
                    price,
                    ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY timestamp ASC) as rn
                FROM prices
                WHERE type = 'ask'
                AND timestamp >= %s
            )
            SELECT 
                i.id,
                i.name,
                i.name_cn,
                lp.price as current_price,
                ep.price as old_price,
                CASE 
                    WHEN ep.price > 0 THEN
                        ((lp.price - ep.price) / ep.price) * 100
                    ELSE
                        0
                END as change_percentage
            FROM items i
            LEFT JOIN latest_prices lp ON i.id = lp.item_id AND lp.rn = 1
            LEFT JOIN earliest_prices ep ON i.id = ep.item_id AND ep.rn = 1
            WHERE lp.price IS NOT NULL
            AND ep.price IS NOT NULL
            AND lp.price > 0
            AND ep.price > 0
            ORDER BY change_percentage DESC
        """
        
        # 记录7天查询参数
        day7_timestamp = int(day7_ago.timestamp())
        logger.info(f"执行7天价格变化查询:")
        logger.info(f"SQL: {day7_query}")
        logger.info(f"参数: timestamp = {day7_timestamp}")
        
        day7_results = db_service.execute_query(
            day7_query,
            (day7_timestamp, day7_timestamp)
        )
        
        logger.info(f"7天查询结果数量: {len(day7_results)}")
        
        # 获取24小时内的价格变化
        day1_query = """
            WITH latest_prices AS (
                SELECT 
                    item_id,
                    price,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY timestamp DESC) as rn
                FROM prices
                WHERE type = 'ask'
                AND timestamp >= %s
            ),
            earliest_prices AS (
                SELECT 
                    item_id,
                    price,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY timestamp ASC) as rn
                FROM prices
                WHERE type = 'ask'
                AND timestamp >= %s
            ),
            single_price_items AS (
                -- 找出24小时内只有一条数据的商品
                SELECT item_id
                FROM latest_prices
                GROUP BY item_id
                HAVING COUNT(*) = 1
            ),
            previous_prices AS (
                -- 对于只有一条数据的商品，获取24小时之前的最后一条数据
                SELECT 
                    p.item_id,
                    p.price,
                    p.timestamp,
                    ROW_NUMBER() OVER (PARTITION BY p.item_id ORDER BY p.timestamp DESC) as rn
                FROM prices p
                JOIN single_price_items spi ON p.item_id = spi.item_id
                WHERE p.type = 'ask'
                AND p.timestamp < %s
            )
            SELECT 
                i.id,
                i.name,
                i.name_cn,
                COALESCE(lp.price, 0) as current_price,
                CASE 
                    -- 如果是单条数据的商品，使用之前的价格
                    WHEN spi.item_id IS NOT NULL THEN pp.price
                    -- 否则使用当前时间范围内的最早价格
                    ELSE ep.price
                END as old_price,
                CASE 
                    WHEN spi.item_id IS NOT NULL AND pp.price > 0 THEN
                        ((lp.price - pp.price) / pp.price) * 100
                    WHEN ep.price > 0 THEN
                        ((lp.price - ep.price) / ep.price) * 100
                    ELSE
                        0
                END as change_percentage,
                CASE 
                    WHEN spi.item_id IS NOT NULL THEN pp.timestamp
                    ELSE ep.timestamp
                END as old_price_time
            FROM items i
            LEFT JOIN latest_prices lp ON i.id = lp.item_id AND lp.rn = 1
            LEFT JOIN earliest_prices ep ON i.id = ep.item_id AND ep.rn = 1
            LEFT JOIN single_price_items spi ON i.id = spi.item_id
            LEFT JOIN previous_prices pp ON i.id = pp.item_id AND pp.rn = 1
            WHERE lp.price IS NOT NULL
            AND (
                (spi.item_id IS NULL AND ep.price IS NOT NULL AND ep.price > 0)
                OR 
                (spi.item_id IS NOT NULL AND pp.price IS NOT NULL AND pp.price > 0)
            )
            AND lp.price > 0
            ORDER BY change_percentage DESC
        """
        
        # 记录24小时查询参数
        day1_timestamp = int(day1_ago.timestamp())
        logger.info(f"执行24小时价格变化查询:")
        logger.info(f"SQL: {day1_query}")
        logger.info(f"参数: timestamp = {day1_timestamp}")
        
        day1_results = db_service.execute_query(
            day1_query,
            (day1_timestamp, day1_timestamp, day1_timestamp)
        )
        
        logger.info(f"24小时查询结果数量: {len(day1_results)}")
        
        # 处理数据
        day7_stats = [{
            'id': row['id'],
            'name': row['name'],
            'name_cn': row['name_cn'],
            'current_price': float(row['current_price']),
            'old_price': float(row['old_price']),
            'change_percentage': float(row['change_percentage'])
        } for row in day7_results]
        
        day1_stats = [{
            'id': row['id'],
            'name': row['name'],
            'name_cn': row['name_cn'],
            'current_price': float(row['current_price']),
            'old_price': float(row['old_price']),
            'change_percentage': float(row['change_percentage']),
            'old_price_time': datetime.fromtimestamp(row['old_price_time']).strftime('%Y-%m-%d %H:%M:%S')
        } for row in day1_results]
        
        # 记录处理后的数据数量
        logger.info(f"最终7天统计数据数量: {len(day7_stats)}")
        logger.info(f"最终24小时统计数据数量: {len(day1_stats)}")
        
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': {
                'day7': day7_stats,
                'day1': day1_stats
            }
        })
        
    except Exception as e:
        logger.error(f'获取市场统计数据失败: {e}')
        return jsonify({
            'code': 1,
            'message': str(e)
        }), 500

def get_market_trend():
    """获取市场整体价格走势"""
    try:
        # 从预计算的趋势数据表中获取所有数据
        results = trend_service.get_trends()
        
        if not results:
            return jsonify({
                'code': 0,
                'message': 'success',
                'data': []
            })

        trend_data = []
        for row in results:
            # 添加人类可读的日期格式
            readable_date = datetime.fromtimestamp(row['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            day_data = {
                'timestamp': row['timestamp'],
                'human_readable_date': readable_date,  # 添加易读日期
                'open': float(row['open']),
                'close': float(row['close']),
                'high': float(row['high']),
                'low': float(row['low']),
                'ma5': float(row['ma5']) if row['ma5'] is not None else None,
                'ma10': float(row['ma10']) if row['ma10'] is not None else None,
                'volume': int(row['volume'])
            }
            trend_data.append(day_data)

        # 计算环比变化
        for i in range(1, len(trend_data)):
            prev_close = trend_data[i-1]['close']
            curr_close = trend_data[i]['close']
            trend_data[i]['change_rate'] = ((curr_close - prev_close) / prev_close) * 100 if prev_close > 0 else 0
        
        if trend_data:
            trend_data[0]['change_rate'] = 0

        return jsonify({
            'code': 0,
            'message': 'success',
            'data': trend_data
        })
        
    except Exception as e:
        logger.error(f'获取市场整体价格走势失败: {e}')
        return jsonify({
            'code': 1,
            'message': str(e)
        }), 500

def register_routes(app):
    """注册路由"""
    # 获取市场统计数据
    app.route('/api/v1/market/statistics')(
        swag_from({
            'tags': ['市场统计'],
            'summary': '获取市场统计数据',
            'description': '获取市场7天和24小时的价格变化统计',
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
                                'type': 'array',
                                'items': {
                                    'type': 'object',
                                    'properties': {
                                        'timestamp': {
                                            'type': 'integer',
                                            'description': '时间戳'
                                        },
                                        'open': {
                                            'type': 'number',
                                            'description': '开盘价'
                                        },
                                        'close': {
                                            'type': 'number',
                                            'description': '收盘价'
                                        },
                                        'high': {
                                            'type': 'number',
                                            'description': '最高价'
                                        },
                                        'low': {
                                            'type': 'number',
                                            'description': '最低价'
                                        },
                                        'avg_price': {
                                            'type': 'number',
                                            'description': '平均价格'
                                        },
                                        'volume': {
                                            'type': 'integer',
                                            'description': '成交量'
                                        },
                                        'change_rate': {
                                            'type': 'number',
                                            'description': '价格变化百分比'
                                        },
                                        'item_count': {
                                            'type': 'integer',
                                            'description': '交易商品数量'
                                        },
                                        'start_time': {
                                            'type': 'integer',
                                            'description': '开始时间戳'
                                        },
                                        'end_time': {
                                            'type': 'integer',
                                            'description': '结束时间戳'
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
        })(get_market_stats)
    )
    
    # 获取市场整体价格走势
    app.route('/api/v1/market/trends')(
        swag_from({
            'tags': ['市场统计'],
            'summary': '获取市场整体价格走势',
            'description': '获取市场最近30天的整体价格走势',
            'responses': {
                200: {
                    'description': '成功获取价格走势',
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
                                        'open': {
                                            'type': 'number',
                                            'description': '开盘价'
                                        },
                                        'close': {
                                            'type': 'number',
                                            'description': '收盘价'
                                        },
                                        'high': {
                                            'type': 'number',
                                            'description': '最高价'
                                        },
                                        'low': {
                                            'type': 'number',
                                            'description': '最低价'
                                        },
                                        'avg_price': {
                                            'type': 'number',
                                            'description': '平均价格'
                                        },
                                        'volume': {
                                            'type': 'integer',
                                            'description': '成交量'
                                        },
                                        'change_rate': {
                                            'type': 'number',
                                            'description': '价格变化百分比'
                                        },
                                        'item_count': {
                                            'type': 'integer',
                                            'description': '交易商品数量'
                                        },
                                        'start_time': {
                                            'type': 'integer',
                                            'description': '开始时间戳'
                                        },
                                        'end_time': {
                                            'type': 'integer',
                                            'description': '结束时间戳'
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
        })(get_market_trend)
    )