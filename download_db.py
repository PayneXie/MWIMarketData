"""
下载市场数据并保存到SQLite数据库

功能说明：
1. 从API获取市场数据
2. 将数据保存到SQLite数据库的ask和bid表中
3. ask表存储询价数据
4. bid表存储售价数据

使用方法：
1. 确保已安装所需依赖
2. 运行脚本即可下载最新数据
3. 数据将保存在 static/db/market.db 中

注意事项：
1. 需要网络连接
2. 需要足够的磁盘空间
3. 建议定期运行以保持数据更新
"""

import os
import requests
from tqdm import tqdm

def download_db():
    # Create db directory if it doesn't exist
    db_dir = os.path.join('static', 'db')
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    # URL of the database file
    url = 'https://raw.githubusercontent.com/holychikenz/MWIApi/main/market.db'
    
    # Download the file with progress bar
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        # Get total file size
        total_size = int(response.headers.get('content-length', 0))
        
        # Create progress bar
        progress_bar = tqdm(
            total=total_size,
            unit='iB',
            unit_scale=True,
            desc='Downloading market.db'
        )
        # Save the file to db folder with progress updates
        file_path = os.path.join(db_dir, 'market.db')
        with open(file_path, 'wb') as f:
            for data in response.iter_content(1024):
                size = f.write(data)
                progress_bar.update(size)
        
        progress_bar.close()
        print('Database file downloaded successfully')
    else:
        print(f'Failed to download database. Status code: {response.status_code}')

if __name__ == '__main__':
    download_db()