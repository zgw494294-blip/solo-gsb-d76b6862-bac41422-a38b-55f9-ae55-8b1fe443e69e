#!/bin/sh
# 容器启动入口：先安装依赖，再启动应用。
set -e
pip install --no-cache-dir -r requirements.txt
exec python app.py
