
# Fonctions de vérification
def check_yfinance_api():
    import requests
    try:
        response = requests.get("https://query1.finance.yahoo.com/v1/test/getcrumb")
        if response.status_code == 200:
            print("YFinance API is accessible")
        else:
            raise Exception(f"YFinance API check failed with status: {response.status_code}")
    except Exception as e:
        raise Exception(f"YFinance API check failed: {str(e)}")

def check_fred_api():
    import requests
    FRED_KEY = "552653b1665602249d879cccf7c6d22f"
    try:
        response = requests.get(
            f"https://api.stlouisfed.org/fred/series?series_id=GNPCA&api_key={FRED_KEY}&file_type=json"
        )
        if response.status_code == 200:
            print("FRED API is accessible")
        else:
            raise Exception(f"FRED API check failed with status: {response.status_code}")
    except Exception as e:
        raise Exception(f"FRED API check failed: {str(e)}")

def check_hdfs_connection():
    from hdfs import InsecureClient

    try:
        HDFS_CLIENT = InsecureClient("http://namenode:9870", user="hadoop")
        status = HDFS_CLIENT.status("/", False)
        print("HDFS connection successful")
    except Exception as e:
        raise Exception(f"HDFS connection check failed: {str(e)}")

def check_snowflake_connection():
    import snowflake.connector

    SNOWFLAKE_CONN = {
        "account": "LEKYCXI-ZO52842",
        "user": "ZAHNOUNE",
        "password": "CdFbMNyjc87vueV",
        "database": "GOLD_ANALYSIS",
        "schema": "MARCHE",
        "warehouse": "COMPUTE_WH"
    }
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
        conn.cursor().execute("SELECT CURRENT_VERSION()")
        conn.close()
        print("Snowflake connection successful")
    except Exception as e:
        raise Exception(f"Snowflake connection check failed: {str(e)}")
