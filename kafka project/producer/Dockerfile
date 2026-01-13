FROM python:3.12-slim

WORKDIR /app

COPY producer.py .
COPY data ./data

RUN pip install kafka-python

CMD ["python", "producer.py"]
