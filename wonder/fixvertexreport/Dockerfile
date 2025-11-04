FROM python:3.11-slim

WORKDIR /app

# 安装依赖
RUN pip install --no-cache-dir requests

# 复制脚本
COPY get_es_trace_data.py .

CMD ["python", "get_es_trace_data.py"]
