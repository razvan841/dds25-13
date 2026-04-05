FROM python:3.12-slim

WORKDIR /home/flask-app

COPY ./common ./common
COPY ./stock/requirements.txt .

RUN pip install -r requirements.txt

COPY ./stock .

EXPOSE 5000
