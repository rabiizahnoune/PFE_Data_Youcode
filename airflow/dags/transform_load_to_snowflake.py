import pandas as pd
from hdfs import InsecureClient
import snowflake.connector
from datetime import datetime

HDFS_CLIENT = InsecureClient("http://namenode:9870", user="hadoop")
SNOWFLAKE_CONN = {
    "account": "RRWRUAS-VI48008",
    "user": "RABIIZAHNOUNE05",
    "password": "CdFbMNyjc87vueV",
    "database": "GOLD",
    "schema": "PUBLIC",
    "warehouse": "COMPUTE_WH"
}

def transform_load_data():
    # Lire les fichiers ETF et Macro depuis HDFS
    today = datetime.now().strftime("%Y%m%d")
    etf_path = f"/gold_data/raw/etf_{today}.csv"
    macro_path = f"/gold_data/raw/macro_{today}.csv"
    
    # Charger les données ETF (XAUUSD)
    with HDFS_CLIENT.read(etf_path) as reader:
        etf_df = pd.read_csv(reader)
    print(f"Colonnes de etf_df : {etf_df.columns.tolist()}")
    
    # Charger les données Macro
    with HDFS_CLIENT.read(macro_path) as reader:
        macro_df = pd.read_csv(reader)
    print(f"Colonnes de macro_df : {macro_df.columns.tolist()}")
    
    # Renommer les colonnes pour correspondre à la table Snowflake (en majuscules)
    etf_df = etf_df.rename(columns={
        "raw_date": "RAW_DATE",
        "source_name": "SOURCE_NAME",
        "ticker_symbol": "TICKER_SYMBOL",
        "open_price": "OPEN_PRICE",
        "close_price": "CLOSE_PRICE",
        "volume": "VOLUME",
        "ingestion_timestamp": "INGESTION_TIMESTAMP"
    })
    
    macro_df = macro_df.rename(columns={
        "raw_date": "RAW_DATE",
        "source_name": "SOURCE_NAME",
        "indicator_name": "INDICATOR_NAME",
        "value": "VALUE",
        "ingestion_timestamp": "INGESTION_TIMESTAMP"
    })
    
    # Convertir RAW_DATE en datetime pour les deux DataFrames
    etf_df['RAW_DATE'] = pd.to_datetime(etf_df['RAW_DATE'])
    macro_df['RAW_DATE'] = pd.to_datetime(macro_df['RAW_DATE'])
    
    # Remplir les valeurs manquantes pour macro_df avec forward fill/backward fill
    date_range = pd.date_range(start=etf_df['RAW_DATE'].min(), end=etf_df['RAW_DATE'].max(), freq='D')
    indicators = macro_df['INDICATOR_NAME'].unique()
    all_dates_indicators = pd.MultiIndex.from_product([date_range, indicators], names=['RAW_DATE', 'INDICATOR_NAME'])
    full_macro_df = pd.DataFrame(index=all_dates_indicators).reset_index()
    
    full_macro_df = full_macro_df.merge(
        macro_df[['RAW_DATE', 'INDICATOR_NAME', 'SOURCE_NAME', 'VALUE', 'INGESTION_TIMESTAMP']],
        on=['RAW_DATE', 'INDICATOR_NAME'],
        how='left'
    )
    
    full_macro_df = full_macro_df.sort_values(['INDICATOR_NAME', 'RAW_DATE'])
    full_macro_df['VALUE'] = full_macro_df.groupby('INDICATOR_NAME')['VALUE'].fillna(method='ffill')
    full_macro_df['VALUE'] = full_macro_df.groupby('INDICATOR_NAME')['VALUE'].fillna(method='bfill')
    full_macro_df['SOURCE_NAME'] = full_macro_df.groupby('INDICATOR_NAME')['SOURCE_NAME'].fillna(method='ffill')
    full_macro_df['SOURCE_NAME'] = full_macro_df.groupby('INDICATOR_NAME')['SOURCE_NAME'].fillna(method='bfill')
    full_macro_df['INGESTION_TIMESTAMP'] = full_macro_df.groupby('INDICATOR_NAME')['INGESTION_TIMESTAMP'].fillna(method='ffill')
    full_macro_df['INGESTION_TIMESTAMP'] = full_macro_df.groupby('INDICATOR_NAME')['INGESTION_TIMESTAMP'].fillna(method='bfill')
    
    # Convertir RAW_DATE en string pour Snowflake
    etf_df['RAW_DATE'] = etf_df['RAW_DATE'].astype(str)
    full_macro_df['RAW_DATE'] = full_macro_df['RAW_DATE'].astype(str)
    
    # Connexion à Snowflake
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    
    # Activer explicitement le warehouse
    try:
        conn.cursor().execute(f"USE WAREHOUSE {SNOWFLAKE_CONN['warehouse']}")
        print(f"Warehouse {SNOWFLAKE_CONN['warehouse']} activé avec succès")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Erreur lors de l'activation du warehouse : {e}")
        conn.close()
        raise
    
    # Créer les tables de staging
    try:
        conn.cursor().execute("""
            CREATE TABLE IF NOT EXISTS STAGING_GOLD_PRICES (
                RAW_DATE VARCHAR,
                SOURCE_NAME VARCHAR,
                TICKER_SYMBOL VARCHAR,
                OPEN_PRICE FLOAT,
                CLOSE_PRICE FLOAT,
                VOLUME BIGINT,
                INGESTION_TIMESTAMP TIMESTAMP
            )
        """)
        conn.cursor().execute("""
            CREATE TABLE IF NOT EXISTS STAGING_MACRO_INDICATORS (
                RAW_DATE VARCHAR,
                SOURCE_NAME VARCHAR,
                INDICATOR_NAME VARCHAR,
                VALUE FLOAT,
                INGESTION_TIMESTAMP TIMESTAMP
            )
        """)
        print("Tables de staging créées ou déjà existantes")
    except Exception as e:
        print(f"Erreur lors de la création des tables de staging : {e}")
        conn.close()
        raise
    
    # Charger dans les tables de staging
    from snowflake.connector.pandas_tools import write_pandas
    try:
        success, num_chunks, num_rows, _ = write_pandas(conn, etf_df, "STAGING_GOLD_PRICES", auto_create_table=False)
        print(f"Données chargées dans STAGING_GOLD_PRICES : {success}, {num_rows} lignes, {num_chunks} chunks")
        success, num_chunks, num_rows, _ = write_pandas(conn, full_macro_df, "STAGING_MACRO_INDICATORS", auto_create_table=False)
        print(f"Données chargées dans STAGING_MACRO_INDICATORS : {success}, {num_rows} lignes, {num_chunks} chunks")
    except Exception as e:
        print(f"Erreur lors du chargement dans les tables de staging : {e}")
        conn.close()
        raise
    
    # Peupler les dimensions
    try:
    # dim_date
        conn.cursor().execute("""
            MERGE INTO dim_date AS target
            USING (
                SELECT DISTINCT
                    EXTRACT(YEAR FROM TO_TIMESTAMP(RAW_DATE)) * 1000000 + 
                    EXTRACT(MONTH FROM TO_TIMESTAMP(RAW_DATE)) * 10000 + 
                    EXTRACT(DAY FROM TO_TIMESTAMP(RAW_DATE)) * 100 + 
                    EXTRACT(HOUR FROM TO_TIMESTAMP(RAW_DATE)) AS date_id,
                    TO_DATE(RAW_DATE) AS date,
                    EXTRACT(DAY FROM TO_TIMESTAMP(RAW_DATE)) AS day,
                    EXTRACT(MONTH FROM TO_TIMESTAMP(RAW_DATE)) AS month,
                    EXTRACT(YEAR FROM TO_TIMESTAMP(RAW_DATE)) AS year,
                    DAYNAME(TO_TIMESTAMP(RAW_DATE)) AS weekday,
                    EXTRACT(HOUR FROM TO_TIMESTAMP(RAW_DATE)) AS hour
                FROM STAGING_GOLD_PRICES
                UNION
                SELECT DISTINCT
                    EXTRACT(YEAR FROM TO_TIMESTAMP(RAW_DATE)) * 1000000 + 
                    EXTRACT(MONTH FROM TO_TIMESTAMP(RAW_DATE)) * 10000 + 
                    EXTRACT(DAY FROM TO_TIMESTAMP(RAW_DATE)) * 100 + 
                    EXTRACT(HOUR FROM TO_TIMESTAMP(RAW_DATE)) AS date_id,
                    TO_DATE(RAW_DATE) AS date,
                    EXTRACT(DAY FROM TO_TIMESTAMP(RAW_DATE)) AS day,
                    EXTRACT(MONTH FROM TO_TIMESTAMP(RAW_DATE)) AS month,
                    EXTRACT(YEAR FROM TO_TIMESTAMP(RAW_DATE)) AS year,
                    DAYNAME(TO_TIMESTAMP(RAW_DATE)) AS weekday,
                    EXTRACT(HOUR FROM TO_TIMESTAMP(RAW_DATE)) AS hour
                FROM STAGING_MACRO_INDICATORS
            ) AS source
            ON target.date_id = source.date_id
            WHEN NOT MATCHED THEN
                INSERT (date_id, date, day, month, year, weekday, hour)
                VALUES (source.date_id, source.date, source.day, source.month, source.year, source.weekday, source.hour);
        """)
        print("Table dim_date peuplée")

        # dim_source
        conn.cursor().execute("""
            MERGE INTO dim_source AS target
            USING (
                SELECT * FROM (VALUES
                    (1, 'yfinance'),
                    (2, 'FRED')
                ) AS v (source_id, source_name)
            ) AS source
            ON target.source_id = source.source_id
            WHEN NOT MATCHED THEN
                INSERT (source_id, source_name)
                VALUES (source.source_id, source.source_name);
        """)
        print("Table dim_source peuplée")

        # dim_ticker
        conn.cursor().execute("""
            MERGE INTO dim_ticker AS target
            USING (
                SELECT * FROM (VALUES
                    (1, 'GC=F', 'Gold Futures')
                ) AS v (ticker_id, ticker_symbol, ticker_name)
            ) AS source
            ON target.ticker_id = source.ticker_id
            WHEN NOT MATCHED THEN
                INSERT (ticker_id, ticker_symbol, ticker_name)
                VALUES (source.ticker_id, source.ticker_symbol, source.ticker_name);
        """)
        print("Table dim_ticker peuplée")

        # dim_indicator
        conn.cursor().execute("""
            MERGE INTO dim_indicator AS target
            USING (
                SELECT * FROM (VALUES
                    (1, 'fed_funds_rate', 'Macro'),
                    (2, 'inflation_us', 'Macro'),
                    (3, 'dxy_index', 'Macro')
                ) AS v (indicator_id, indicator_name, category)
            ) AS source
            ON target.indicator_id = source.indicator_id
            WHEN NOT MATCHED THEN
                INSERT (indicator_id, indicator_name, category)
                VALUES (source.indicator_id, source.indicator_name, source.category);
        """)
        print("Table dim_indicator peuplée")
        
        # Peupler fact_gold_prices
        conn.cursor().execute("""
            MERGE INTO fact_gold_prices AS target
            USING (
                SELECT
                    d.date_id,
                    s.source_id,
                    t.ticker_id,
                    r.CLOSE_PRICE AS gold_price,
                    r.OPEN_PRICE AS open_price,
                    r.CLOSE_PRICE AS close_price,
                    r.VOLUME,
                    r.INGESTION_TIMESTAMP
                FROM STAGING_GOLD_PRICES r
                JOIN dim_date d ON d.date = r.RAW_DATE::DATE
                JOIN dim_source s ON TRIM(LOWER(s.source_name)) = TRIM(LOWER(r.SOURCE_NAME))
                JOIN dim_ticker t ON TRIM(LOWER(t.ticker_symbol)) = TRIM(LOWER(r.TICKER_SYMBOL))
            ) AS source
            ON target.date_id = source.date_id AND target.ticker_id = source.ticker_id
            WHEN MATCHED THEN
                UPDATE SET
                    source_id = source.source_id,
                    gold_price = source.gold_price,
                    open_price = source.open_price,
                    close_price = source.close_price,
                    volume = source.volume,
                    ingestion_timestamp = source.ingestion_timestamp
            WHEN NOT MATCHED THEN
                INSERT (date_id, source_id, ticker_id, gold_price, open_price, close_price, volume, ingestion_timestamp)
                VALUES (source.date_id, source.source_id, source.ticker_id, source.gold_price, source.open_price, source.close_price, source.volume, source.ingestion_timestamp);
        """)
        print("Table fact_gold_prices peuplée")

        # Peupler fact_macro_indicators
        conn.cursor().execute("""
            MERGE INTO fact_macro_indicators AS target
            USING (
                SELECT
                    d.date_id,
                    s.source_id,
                    i.indicator_id,
                    r.VALUE AS macro_value,
                    r.INGESTION_TIMESTAMP
                FROM STAGING_MACRO_INDICATORS r
                JOIN dim_date d ON d.date = r.RAW_DATE::DATE
                JOIN dim_source s ON TRIM(LOWER(s.source_name)) = TRIM(LOWER(r.SOURCE_NAME))
                JOIN dim_indicator i ON TRIM(LOWER(i.indicator_name)) = TRIM(LOWER(r.INDICATOR_NAME))
            ) AS source
            ON target.date_id = source.date_id AND target.indicator_id = source.indicator_id
            WHEN MATCHED THEN
                UPDATE SET
                    source_id = source.source_id,
                    macro_value = source.macro_value,
                    ingestion_timestamp = source.ingestion_timestamp
            WHEN NOT MATCHED THEN
                INSERT (date_id, source_id, indicator_id, macro_value, ingestion_timestamp)
                VALUES (source.date_id, source.source_id, source.indicator_id, source.macro_value, source.ingestion_timestamp);
        """)
        print("Table fact_macro_indicators peuplée")
        
        # Nettoyer les tables de staging
        conn.cursor().execute("TRUNCATE TABLE STAGING_GOLD_PRICES")
        conn.cursor().execute("TRUNCATE TABLE STAGING_MACRO_INDICATORS")
        print("Tables de staging nettoyées")
    except Exception as e:
        print(f"Erreur lors de la transformation/chargement : {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    transform_load_data()