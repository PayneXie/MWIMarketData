# MilkyWayIdle Market Analysis

<div align="center">
  <img src="https://img.shields.io/badge/Game-MilkyWayIdle-blue" alt="Game">
  <img src="https://img.shields.io/badge/Type-Market%20Analysis-green" alt="Type">
  <img src="https://img.shields.io/badge/License-MIT-yellow" alt="License">
</div>

## 📝 项目简介

这是一个为银河奶牛放置（MilkyWayIdle）游戏开发的市场数据分析工具。通过实时监控和分析游戏内物品价格，辅助市场决策。（开发中）

## ✨ 主要功能

- 📊 市场整体价格走势分析
- 📈 7天价格涨跌幅排行
- ⏰ 24小时价格涨跌幅排行
- 🔍 异常价格检测和清洗
- 💰 价格数据可视化展示

## 🛠️ 技术栈

- 后端：Python + Flask
- 前端：React + TypeScript
- 数据库：MySQL
- UI框架：Ant Design
- 图表：Ant Design Charts

## 🚀 快速开始

### 环境要求

- Python 3.8+
- Node.js 16+
- MySQL 8.0+

### 安装步骤

1. 克隆仓库
```bash
git clone https://github.com/yourusername/mwimarketdata.git
cd mwimarketdata
```

2. 安装后端依赖
```bash
pip install -r requirements.txt
```

3. 安装前端依赖
```bash
cd frontend
npm install
```

4. 配置数据库
```bash
# 创建数据库
mysql -u root -p < database/init.sql
```

5. 启动服务
```bash
# 启动后端服务
python app.py

# 启动前端服务
cd frontend
npm run dev
```

## 📊 数据展示

- 市场整体价格走势图
- 7天/24小时涨幅榜
- 7天/24小时跌幅榜



## 📄 开源协议

本项目采用 MIT 协议 - 详见 [LICENSE](LICENSE) 文件

## 🙏 致谢

- [MilkyWayIdle](https://milkywayidle.com/) - 游戏本体
- [Ant Design](https://ant.design/) - UI组件库
- [Ant Design Charts](https://charts.ant.design/) - 图表库



---

<div align="center">
  <sub>Built by Payne</sub>
</div> 