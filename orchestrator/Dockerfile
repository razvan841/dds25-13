FROM python:3.12-slim

WORKDIR /home/orchestrator-app

COPY ./common ./common
COPY ./orchestrator/requirements.txt .

RUN pip install -r requirements.txt

COPY ./orchestrator .

CMD ["python", "-u", "app.py"]
