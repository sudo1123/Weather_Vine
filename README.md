[![GPLv3 License](https://img.shields.io/badge/License-GPL%20v3-yellow.svg)](https://opensource.org/licenses/)

# WeatherVine 天气数据采集系统

**License**: This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.


定时从高德地图API获取指定城市各区天气数据并存储到MySQL数据库

## 功能特性
- 定时自动更新天气数据
- 支持任意中国城市
- 自动创建数据库表结构
- 错误重试机制

## 快速开始
1. 复制 `config.example.yaml` 为 `config.yaml`
2. 填写数据库和高德API配置
3. 安装依赖：`pip install -r requirements.txt`
4. 运行：`python weather_vine.py`

## 配置说明
- `update_interval`: 数据更新频率(秒)
- `city`: 要监控的城市中文名