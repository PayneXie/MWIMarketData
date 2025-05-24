import json
import mysql.connector
from mysql.connector import pooling

def load_database_config():
    try:
        print("正在读取数据库配置文件...")
        with open('database.json', 'r') as f:
            config = json.load(f)
            print("成功读取数据库配置")
            return config['mysql']
    except FileNotFoundError:
        print("错误：找不到 database.json 文件")
        return None
    except json.JSONDecodeError:
        print("错误：database.json 文件格式不正确")
        return None
    except KeyError:
        print("错误：database.json 中缺少 mysql 配置")
        return None

def test_database_connection():
    config = load_database_config()
    if not config:
        return

    connection = None
    cursor = None
    try:
        print("\n尝试连接到数据库...")
        print(f"连接信息：主机={config['host']}, 端口={config['port']}, 数据库={config['database']}, 用户={config['user']}")
        
        connection = mysql.connector.connect(**config)
        print("成功建立数据库连接！")

        cursor = connection.cursor(dictionary=True)
        print("成功创建数据库游标")

        print("\n执行测试查询: SELECT * FROM users")
        cursor.execute("SELECT * FROM users")
        users = cursor.fetchall()
        
        print(f"\n查询成功！找到 {len(users)} 条用户记录")
        if users:
            print("示例用户数据：")
            print(f"第一条记录：{users[0]}")

    except mysql.connector.Error as err:
        print(f"\n数据库错误：{err}")
        if err.errno == 2003:
            print("提示：无法连接到数据库服务器，请检查主机名和端口是否正确")
        elif err.errno == 1045:
            print("提示：访问被拒绝，请检查用户名和密码是否正确")
        elif err.errno == 1049:
            print("提示：数据库不存在，请检查数据库名称是否正确")
        elif err.errno == 1146:
            print("提示：users表不存在，请检查表是否已创建")
    except Exception as e:
        print(f"\n发生未知错误：{e}")
    finally:
        if cursor:
            cursor.close()
            print("\n已关闭数据库游标")
        if connection and connection.is_connected():
            connection.close()
            print("已关闭数据库连接")

if __name__ == '__main__':
    print("=== 数据库连接测试程序 ===")
    test_database_connection()