FROM python:3.8-slim

# Installer les dépendances Python
RUN pip install pandas snowflake-sqlalchemy snowflake-connector-python sqlalchemy dash plotly streamlit cassandra-driver wordcloud networkx snowflake-snowpark-python google-generativeai

WORKDIR /app

COPY while.py /app/while.py
CMD ["python","while.py"]