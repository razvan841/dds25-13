FROM python:3.12-slim

WORKDIR /home/flask-app

COPY ./common ./common
COPY ./orchestrator/requirements.txt .

RUN pip install -r requirements.txt

COPY ./orchestrator .

EXPOSE 5000
