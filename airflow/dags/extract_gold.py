

# Configurations codées en dur



def extract_gold_data():
    import pandas as pd
    import requests
    import yfinance as yf
    from fredapi import Fred
    from hdfs import InsecureClient
    from datetime import datetime, timedelta
    import subprocess
    HDFS_CLIENT = InsecureClient("http://namenode:9870", user="hadoop")
    ALPHA_VANTAGE_KEY = "NZCIX5506VLLH09X"
    FRED_KEY = "552653b1665602249d879cccf7c6d22f"
    TICKER = "GC=F"
    hdfs_paths = [
    "/gold_datalake/raw/gold_prices",
    "/gold_datalake/raw/fed_rates",
    "/gold_datalake/raw/sp500",
    "/gold_datalake/processed"
]  
    for hdfs_dir in hdfs_paths:
    # Crée le dossier seulement s'il n'existe pas (équivalent à "if not exist")
      subprocess.run([
        "docker", "exec", "-u", "root", "namenode",
        "hdfs", "dfs", "-mkdir", "-p", hdfs_dir
    ], check=True)

    # Applique les permissions
      subprocess.run([
        "docker", "exec", "-u", "root", "namenode",
        "hdfs", "dfs", "-chmod", "-R", "777", hdfs_dir
    ], check=True)
    
    # 1. Prix de l’or (yfinance, 4H)
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    data_list = []
    
    # Récupérer les données via yfinance
    try:
        etf = yf.Ticker(TICKER)
        hist = etf.history(start=start_date, end=end_date, interval="4h")  # Granularité 4 heures
        if hist.empty:
            print(f"Aucune donnée récupérée pour {TICKER} entre {start_date} et {end_date}")
            return
        
        for date, row in hist.iterrows():
            data_list.append({
                "raw_date": date,  # Keep full datetime with time component
                "source_name": "yfinance",
                "ticker_symbol": TICKER,
                "open_price": row["Open"],
                "close_price": row["Close"],
                "volume": int(row["Volume"]) if "Volume" in row and pd.notnull(row["Volume"]) else 0,
                "ingestion_timestamp": datetime.now().isoformat()
            })
    except Exception as e:
        print(f"Erreur lors de la récupération des données pour {TICKER} : {e}")
        raise
    
    # Créer un DataFrame et écrire dans HDFS
    df = pd.DataFrame(data_list)
    if df.empty:
        print("Aucune donnée à écrire dans HDFS")
        return
    
    print("Aperçu des données récupérées :")
    print(df.head())
    gold_df = df

    # 2. Taux d’intérêt (FRED, mensuel)
    fred = Fred(api_key=FRED_KEY)
    taux_fed = fred.get_series("FEDFUNDS")
    taux_df = pd.DataFrame({"date": taux_fed.index, "taux_fed": taux_fed}).reset_index()
    taux_df["date"] = pd.to_datetime(taux_df["date"])

    # 3. S&P 500 (yfinance, 1H, à agréger en 4H)
    sp500 = yf.Ticker("^GSPC")
    sp500_data = sp500.history(period="1mo", interval="1h")
    sp500_df = sp500_data[["Close", "Volume"]].reset_index()
    sp500_df.columns = ["date", "prix_sp500", "volume_sp500"]
    sp500_df["date"] = pd.to_datetime(sp500_df["date"])

    # Sauvegarde dans HDFS (zone brute)
    today = datetime.now().strftime("%Y-%m-%d")

    for x, path in [
        (gold_df, f"/gold_datalake/raw/gold_prices/{today}.csv"),
        (taux_df, f"/gold_datalake/raw/fed_rates/{today}.csv"),
        (sp500_df, f"/gold_datalake/raw/sp500/{today}.csv")
    ]:
        with HDFS_CLIENT.write(path) as writer:
            x.to_csv(writer, index=False)

    return gold_df, taux_df, sp500_df