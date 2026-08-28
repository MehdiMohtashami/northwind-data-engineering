FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka==2.5.3 pymssql==2.2.11

COPY producer.py .

CMD ["python", "-u", "producer.py"]
