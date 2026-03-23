FROM python:3.12-slim

WORKDIR /home/flask-app

COPY ./common ./common
COPY ./order/requirements.txt .

RUN pip install -r requirements.txt

COPY ./order .

EXPOSE 5000
