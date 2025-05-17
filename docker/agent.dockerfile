FROM python:3.9-slim

WORKDIR /app

RUN pip install transformers torch flask kafka-python snowflake-connector-python pandas

CMD ["python", "gold_agent.py"]