FROM python:3.12-slim

WORKDIR /app

COPY common_kafka ./common_kafka
COPY gateway ./gateway

WORKDIR /app/gateway

RUN pip install -e /app/common_kafka && pip install -r requirements.txt

EXPOSE 8000
