FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka==2.5.3 pymssql==2.2.11 clickhouse-connect==0.7.19 pymongo==4.8.0

COPY consumer.py .

CMD ["python", "-u", "consumer.py"]
