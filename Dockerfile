FROM python:3.8-slim

# Installer les dépendances Python
RUN pip install pandas snowflake-sqlalchemy snowflake-connector-python sqlalchemy dash plotly streamlit

    WORKDIR /app
    COPY gold_trading_dashboard.py /app/gold_trading_dashboard.py
    CMD ["streamlit", "run", "gold_trading_dashboard.py", "--server.port=8050", "--server.address=0.0.0.0"]