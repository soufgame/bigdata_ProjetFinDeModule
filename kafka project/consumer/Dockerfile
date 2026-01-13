FROM python:3.12-slim

WORKDIR /app

COPY consumer.py .

RUN pip install kafka-python pymongo

CMD ["python", "consumer.py"]
