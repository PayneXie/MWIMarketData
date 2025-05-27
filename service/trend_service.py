"""趋势数据预计算服务

该服务负责：
1. 计算每日市场趋势数据
2. 将数据存储到market_trends表
3. 提供趋势数据查询接口
"""

from datetime import datetime, timedelta
import logging
import os
import sys

# 添加项目根目录到Python路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from service import db_service

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_table_exists(table_name):
    """检查表是否存在"""
    sql = """
    SELECT COUNT(*)
    FROM information_schema.tables 
    WHERE table_schema = DATABASE()
    AND table_name = %s
    """
    result = db_service.execute_query(sql, (table_name,))[0]['COUNT(*)']
    return result > 0

def create_trends_table():
    """创建market_trends表"""
    logger.info("检查market_trends表是否存在...")
    
    if check_table_exists('market_trends'):
        logger.info("✓ market_trends表已存在，跳过创建步骤")
        return
        
    logger.info("开始创建market_trends表...")
    create_table_sql = """
    CREATE TABLE market_trends (
        day DATE PRIMARY KEY,
        open_price DECIMAL(30,8) NOT NULL,
        close_price DECIMAL(30,8) NOT NULL,
        high_price DECIMAL(30,8) NOT NULL,
        low_price DECIMAL(30,8) NOT NULL,
        ma5 DECIMAL(30,8),
        ma10 DECIMAL(30,8),
        volume INT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """
    try:
        db_service.execute_query(create_table_sql)
        logger.info("✓ market_trends表创建成功")
    except Exception as e:
        logger.error(f"✗ 创建market_trends表失败: {e}")
        raise

def get_data_stats():
    """获取数据统计信息"""
    try:
        stats_sql = """
        SELECT 
            MIN(DATE(FROM_UNIXTIME(TIMESTAMP))) as start_date,
            MAX(DATE(FROM_UNIXTIME(TIMESTAMP))) as end_date,
            COUNT(DISTINCT DATE(FROM_UNIXTIME(TIMESTAMP))) as total_days,
            COUNT(*) as total_records
        FROM prices
        WHERE TYPE = 'ASK'
        """
        stats = db_service.execute_query(stats_sql)[0]
        return stats
    except Exception as e:
        logger.error(f"获取数据统计信息失败: {e}")
        raise

def get_all_price_data(start_date, end_date):
    """一次性获取指定日期范围内的所有价格数据"""
    sql = """
    SELECT 
        DATE(FROM_UNIXTIME(TIMESTAMP)) as day,
        TIMESTAMP,
        SUM(PRICE) as SUM_PRICE
    FROM prices 
    WHERE TYPE = 'ASK' 
    AND DATE(FROM_UNIXTIME(TIMESTAMP)) BETWEEN %s AND %s
    GROUP BY TIMESTAMP
    ORDER BY TIMESTAMP
    """
    return db_service.execute_query(sql, (start_date, end_date))

def group_prices_by_day(price_data):
    """将价格数据按天分组"""
    daily_prices = {}
    for row in price_data:
        day = row['day']
        if day not in daily_prices:
            daily_prices[day] = []
        daily_prices[day].append(float(row['SUM_PRICE']))
    return daily_prices

def clean_prices(prices, ma5=None, ma10=None, recursion_depth=0):
    """清洗价格数据，处理异常值
    
    Args:
        prices: 价格列表
        ma5: 5日移动平均线值
        ma10: 10日移动平均线值
        recursion_depth: 递归深度计数器
    Returns:
        清洗后的价格列表
    """
    if not prices:
        return prices
        
    # 限制递归深度
    if recursion_depth >= 3:
        logger.warning("达到最大递归深度，停止进一步清洗")
        return prices
        
    # 计算基本统计量
    sorted_prices = sorted(prices)
    n = len(sorted_prices)
    q1_idx = n // 4
    q3_idx = (n * 3) // 4
    q1 = sorted_prices[q1_idx]
    q3 = sorted_prices[q3_idx]
    iqr = q3 - q1
    median = sorted_prices[n // 2]
    mean = sum(sorted_prices) / n
    
    # 计算标准差
    std_dev = (sum((x - mean) ** 2 for x in sorted_prices) / n) ** 0.5
    
    # 设定基础异常值界限
    iqr_lower = q1 - 1.5 * iqr
    iqr_upper = q3 + 1.5 * iqr
    std_lower = mean - 2 * std_dev
    std_upper = mean + 2 * std_dev
    med_lower = median * 0.4
    med_upper = median * 2.0  # 降低上限倍数
    
    # 如果有移动平均线数据，加入考虑
    if ma5 is not None and ma10 is not None:
        ma_avg = (ma5 + ma10) / 2
        ma_lower = ma_avg * 0.3  # 不允许低于移动平均线的30%
        ma_upper = ma_avg * 3.0  # 不允许高于移动平均线的3倍
        
        # 更新界限
        lower_bound = max(iqr_lower, std_lower, med_lower, ma_lower, 0)
        upper_bound = min(iqr_upper, std_upper, med_upper, ma_upper)
    else:
        lower_bound = max(iqr_lower, std_lower, med_lower, 0)
        upper_bound = min(iqr_upper, std_upper, med_upper)
    
    # 清洗数据
    cleaned_prices = []
    has_outliers = False
    
    # 第一遍：处理明显的异常值
    for price in prices:
        if price > upper_bound:
            cleaned_price = min(upper_bound, median * 1.5)  # 使用更保守的上限
            has_outliers = True
            logger.info(f"检测到异常高值: {price:.2f} -> {cleaned_price:.2f}")
        elif price < lower_bound:
            cleaned_price = max(lower_bound, median * 0.5)  # 使用更保守的下限
            has_outliers = True
            logger.info(f"检测到异常低值: {price:.2f} -> {cleaned_price:.2f}")
        else:
            cleaned_price = price
        cleaned_prices.append(cleaned_price)
    
    # 第二遍：检查相邻价格的合理性
    if len(cleaned_prices) > 1:
        final_prices = [cleaned_prices[0]]
        for i in range(1, len(cleaned_prices)):
            prev_price = final_prices[-1]
            curr_price = cleaned_prices[i]
            
            # 如果相邻价格变化太大（超过50%），使用中间值
            if abs(curr_price - prev_price) / prev_price > 0.5:
                adjusted_price = (prev_price + curr_price) / 2
                logger.info(f"调整相邻价格: {curr_price:.2f} -> {adjusted_price:.2f}")
                final_prices.append(adjusted_price)
                has_outliers = True
            else:
                final_prices.append(curr_price)
        cleaned_prices = final_prices
    
    # 如果仍然存在较大波动且有异常值，进行二次平滑
    if has_outliers and max(cleaned_prices) / min(cleaned_prices) > 2:
        logger.info("检测到清洗后的数据仍有较大波动，进行二次平滑")
        return clean_prices(cleaned_prices, ma5, ma10, recursion_depth + 1)
            
    return cleaned_prices

def truncate_trends_table():
    """清空market_trends表"""
    logger.info("正在清空market_trends表...")
    try:
        truncate_sql = "TRUNCATE TABLE market_trends"
        db_service.execute_query(truncate_sql, fetch=False)
        logger.info("✓ market_trends表已清空")
    except Exception as e:
        logger.error(f"✗ 清空market_trends表失败: {e}")
        raise

def calculate_and_store_trends():
    """计算并存储趋势数据"""
    try:
        # 获取数据统计信息
        stats = get_data_stats()
        if not stats:
            logger.warning("✗ 无法获取数据统计信息")
            return
            
        start_date = stats['start_date']
        end_date = stats['end_date']
        
        logger.info(f"开始处理从 {start_date} 到 {end_date} 的数据")
        
        # 清空现有数据
        truncate_trends_table()
        
        # 一次性获取所有价格数据
        logger.info("正在获取所有价格数据...")
        all_price_data = get_all_price_data(start_date, end_date)
        if not all_price_data:
            logger.warning("✗ 没有找到任何价格数据")
            return

        # 按日期分组数据
        logger.info("正在按日期分组数据...")
        daily_prices = group_prices_by_day(all_price_data)
        total_days = len(daily_prices)
        processed_days = 0

        # 处理每天的数据
        all_daily_data = []
        for day, prices in sorted(daily_prices.items()):
            processed_days += 1
            logger.info(f"正在处理 {day} 的数据 ({processed_days}/{total_days})...")

            # 获取MA5和MA10
            ma5 = None
            ma10 = None
            if len(all_daily_data) >= 5:
                ma5 = sum(d['close_price'] for d in all_daily_data[-5:]) / 5
            if len(all_daily_data) >= 10:
                ma10 = sum(d['close_price'] for d in all_daily_data[-10:]) / 10

            # 清洗当天的价格数据
            cleaned_prices = clean_prices(prices, ma5, ma10)
            
            if not cleaned_prices:
                logger.warning(f"✗ {day} 没有有效的价格数据")
                continue

            # 计算当日OHLC
            daily_data = {
                'day': day,
                'open_price': cleaned_prices[0],
                'close_price': cleaned_prices[-1],
                'high_price': max(cleaned_prices),
                'low_price': min(cleaned_prices),
                'volume': len(cleaned_prices)
            }

            logger.info(f"✓ {day} 数据处理完成 - "
                       f"开盘: {daily_data['open_price']:.2f}, "
                       f"收盘: {daily_data['close_price']:.2f}, "
                       f"最高: {daily_data['high_price']:.2f}, "
                       f"最低: {daily_data['low_price']:.2f}, "
                       f"成交量: {daily_data['volume']}")

            all_daily_data.append(daily_data)

        # 计算移动平均线
        logger.info("正在计算移动平均线...")
        for i, data in enumerate(all_daily_data):
            # MA5
            if i >= 4:
                ma5_prices = [d['close_price'] for d in all_daily_data[i-4:i+1]]
                data['ma5'] = sum(ma5_prices) / 5
            else:
                data['ma5'] = None

            # MA10
            if i >= 9:
                ma10_prices = [d['close_price'] for d in all_daily_data[i-9:i+1]]
                data['ma10'] = sum(ma10_prices) / 10
            else:
                data['ma10'] = None

        # 存储数据
        logger.info(f"正在将 {len(all_daily_data)} 天的数据写入数据库...")
        insert_sql = """
        INSERT INTO market_trends 
            (day, open_price, close_price, high_price, low_price, ma5, ma10, volume)
        VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s) AS new_data
        ON DUPLICATE KEY UPDATE
            open_price = new_data.open_price,
            close_price = new_data.close_price,
            high_price = new_data.high_price,
            low_price = new_data.low_price,
            ma5 = new_data.ma5,
            ma10 = new_data.ma10,
            volume = new_data.volume,
            updated_at = CURRENT_TIMESTAMP
        """

        insert_data = []
        for data in all_daily_data:
            insert_data.append((
                data['day'],
                data['open_price'],
                data['close_price'],
                data['high_price'],
                data['low_price'],
                data['ma5'],
                data['ma10'],
                data['volume']
            ))

        db_service.execute_many(insert_sql, insert_data)
        logger.info(f"✓ 数据存储完成，成功更新 {len(insert_data)} 条趋势数据")

        # 验证数据
        logger.info("正在验证数据完整性...")
        verify_sql = "SELECT COUNT(*) as count FROM market_trends"
        verify_result = db_service.execute_query(verify_sql)[0]
        logger.info(f"✓ 数据验证完成，market_trends表共有 {verify_result['count']} 条记录")

    except Exception as e:
        logger.error(f"✗ 计算和存储趋势数据失败: {e}")
        raise

def get_trends(days=None):
    """从market_trends表获取趋势数据
    
    Args:
        days: 可选，获取最近几天的数据。如果不指定，则获取所有数据
    """
    try:
        if days:
            query = """
            SELECT 
                day,
                UNIX_TIMESTAMP(day) as timestamp,
                open_price as open,
                close_price as close,
                high_price as high,
                low_price as low,
                ma5,
                ma10,
                volume
            FROM market_trends
            WHERE day >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            ORDER BY day
            """
            results = db_service.execute_query(query, (days,))
        else:
            query = """
            SELECT 
                day,
                UNIX_TIMESTAMP(day) as timestamp,
                open_price as open,
                close_price as close,
                high_price as high,
                low_price as low,
                ma5,
                ma10,
                volume
            FROM market_trends
            ORDER BY day
            """
            results = db_service.execute_query(query)
            
        return results

    except Exception as e:
        logger.error(f"获取趋势数据失败: {e}")
        raise

if __name__ == "__main__":
    logger.info("=== 开始执行趋势数据计算任务 ===")
    try:
        create_trends_table()
        calculate_and_store_trends()
        logger.info("=== 趋势数据计算任务完成 ===")
    except Exception as e:
        logger.error("=== 趋势数据计算任务失败 ===")
        raise 