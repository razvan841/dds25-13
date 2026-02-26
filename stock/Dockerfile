FROM python:3.12-slim

WORKDIR /app

# Copy shared package and service code (build context must be repo root)
COPY common_kafka ./common_kafka
COPY stock ./stock

WORKDIR /app/stock

RUN pip install -e /app/common_kafka && \
    pip install -r requirements.txt

EXPOSE 5000
