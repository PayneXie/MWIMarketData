"""
Flask应用主文件

功能说明：
1. 提供RESTful API接口
2. 处理数据库查询
3. 返回JSON格式数据
4. 集成Swagger文档
5. 定期同步市场数据
"""

from flask import Flask
from flasgger import Swagger
from flask_cors import CORS
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime
from sync_latest import sync_latest_data
from service import items_service, stats_service

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
# 配置CORS，允许所有路由支持跨域
CORS(app, resources={r"/api/v1/*": {"origins": ["http://localhost:3000"]}})

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

# 注册服务路由
items_service.register_routes(app)
stats_service.register_routes(app)

if __name__ == '__main__':
    app.run(debug=True)
