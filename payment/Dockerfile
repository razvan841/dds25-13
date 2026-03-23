FROM python:3.12-slim

WORKDIR /home/flask-app

COPY ./common ./common
COPY ./payment/requirements.txt .

RUN pip install -r requirements.txt

COPY ./payment .

EXPOSE 5000
