FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt docker-entrypoint.sh app.py ./
COPY templates/ templates/
COPY static/ static/

EXPOSE 8000

# 依赖随容器启动安装（见 docker-entrypoint.sh）
CMD ["sh", "docker-entrypoint.sh"]
