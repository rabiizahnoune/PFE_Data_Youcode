
from datetime import datetime
import os
import tempfile


def transform_gold_data():
    from hdfs import InsecureClient

    HDFS_CLIENT = InsecureClient("http://namenode:9870", user="hadoop")
    import pandas as pd
    import pyarrow.parquet as pq
    import pyarrow as pa
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Lire et valider le CSV des prix de l'or
    with HDFS_CLIENT.read(f"/gold_datalake/raw/gold_prices/{today}.csv") as file:
        gold_df = pd.read_csv(file)
    if "raw_date" not in gold_df.columns:
        raise ValueError(f"'raw_date' column not found in gold_prices CSV. Available columns: {gold_df.columns.tolist()}")
    gold_df = gold_df.rename(columns={
        "raw_date": "date",
        "close_price": "prix_or",
        "volume": "volume_or"
    })
    
    # Lire et valider le CSV des taux fédéraux
    with HDFS_CLIENT.read(f"/gold_datalake/raw/fed_rates/{today}.csv") as file:
        taux_df = pd.read_csv(file)
    if "date" not in taux_df.columns:
        raise ValueError(f"'date' column not found in fed_rates CSV. Available columns: {taux_df.columns.tolist()}")
    
    # Lire et valider le CSV S&P 500
    with HDFS_CLIENT.read(f"/gold_datalake/raw/sp500/{today}.csv") as file:
        sp500_df = pd.read_csv(file)
    if "date" not in sp500_df.columns:
        raise ValueError(f"'date' column not found in sp500 CSV. Available columns: {sp500_df.columns.tolist()}")
    
    # Convertir les dates et supprimer les informations de fuseau horaire
    gold_df["date"] = pd.to_datetime(gold_df["date"]).dt.tz_localize(None).dt.floor("4H")
    taux_df["date"] = pd.to_datetime(taux_df["date"]).dt.tz_localize(None).dt.floor("D")
    sp500_df["date"] = pd.to_datetime(sp500_df["date"]).dt.tz_localize(None)

    # Débogage : Afficher les dates uniques
    print(f"Dates uniques dans gold_df: {gold_df['date'].unique()[:5]}")
    print(f"Dates uniques dans sp500_df: {sp500_df['date'].unique()[:5]}")

    # Agréger S&P 500 en 4H (moyenne des prix, somme des volumes)
    sp500_df["date_4h"] = sp500_df["date"].dt.floor("4H")
    sp500_4h = sp500_df.groupby("date_4h").agg({
        "prix_sp500": "mean",
        "volume_sp500": "sum"
    }).reset_index()
    sp500_4h.columns = ["date", "prix_sp500", "volume_sp500"]
    sp500_4h["date"] = pd.to_datetime(sp500_4h["date"]).dt.tz_localize(None)

    # Débogage : Afficher les dates uniques dans sp500_4h
    print(f"Dates uniques dans sp500_4h: {sp500_4h['date'].unique()[:5]}")

    # Créer un calendrier 4H pour le mois
    start_date = (datetime.now() - pd.Timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = datetime.now().replace(hour=20, minute=0, second=0, microsecond=0)
    date_range = pd.date_range(start=start_date, end=end_date, freq="4H")
    calendar_df = pd.DataFrame({"date": date_range})

    # Débogage : Afficher les dates uniques dans calendar_df
    print(f"Dates uniques dans calendar_df: {calendar_df['date'].unique()[:5]}")

    # Fusionner avec taux_df en utilisant un mappage mensuel
    taux_df["date"] = taux_df["date"].dt.to_period("M").dt.to_timestamp()
    merged_df = calendar_df.merge(gold_df, on="date", how="left") \
                          .merge(sp500_4h, on="date", how="left")
    
    # Fusionner taux_df sur le début du mois
    merged_df["month_start"] = merged_df["date"].dt.to_period("M").dt.to_timestamp()
    merged_df = merged_df.merge(taux_df[["date", "taux_fed"]], left_on="month_start", right_on="date", how="left", suffixes=("", "_taux"))
    merged_df = merged_df.drop(columns=["date_taux", "month_start"])

    # Débogage : Inspecter merged_df
    print(f"Colonnes dans merged_df: {merged_df.columns.tolist()}")
    print(f"Taille de merged_df: {merged_df.shape}")
    print(f"Échantillon de merged_df:\n{merged_df.head().to_string()}")

    # Valider les colonnes requises
    required_columns = ["prix_or", "volume_or", "prix_sp500", "volume_sp500", "taux_fed"]
    missing_columns = [col for col in required_columns if col not in merged_df.columns]
    if missing_columns:
        raise ValueError(f"Colonnes manquantes dans merged_df: {missing_columns}. Colonnes disponibles: {merged_df.columns.tolist()}")

    # Harmoniser les données
    merged_df["taux_fed"] = merged_df["taux_fed"].fillna(method="ffill")
    merged_df["prix_or"] = merged_df["prix_or"].interpolate(method="linear")
    merged_df["prix_sp500"] = merged_df["prix_sp500"].interpolate(method="linear")
    merged_df["volume_or"] = merged_df["volume_or"].interpolate(method="linear")
    merged_df["volume_sp500"] = merged_df["volume_sp500"].interpolate(method="linear")

    # Débogage : Vérifier si merged_df est vide après interpolation
    if merged_df.empty:
        raise ValueError("merged_df est vide après interpolation. Vérifiez les données d'entrée et l'alignement de la fusion.")

    # Supprimer uniquement les lignes où prix_or est NaN
    merged_df = merged_df.dropna(subset=["prix_or"])

    # Débogage : Vérifier la taille après dropna
    print(f"Taille de merged_df après dropna: {merged_df.shape}")

    # Vérifier si prix_or est vide avant le calcul
    if merged_df["prix_or"].empty:
        raise ValueError("La colonne prix_or est vide après traitement. Impossible de calculer la variation de prix.")
    premier_prix = merged_df["prix_or"].iloc[0]
    merged_df["variation_or_pct"] = ((merged_df["prix_or"] - premier_prix) / premier_prix) * 100
    merged_df["variation_or_abs"] = merged_df["prix_or"] - premier_prix

    # Créer Dim_Date
    dim_date = pd.DataFrame({
        "date_id": merged_df["date"],
        "jour": merged_df["date"].dt.day,
        "mois": merged_df["date"].dt.month,
        "annee": merged_df["date"].dt.year,
        "heure": merged_df["date"].dt.hour,
        "trimestre": merged_df["date"].dt.to_period("Q").astype(str)
    })

    # Créer Fait_Prix_Or
    fait_prix_or = merged_df[["date", "prix_or", "volume_or", "prix_sp500", "taux_fed", "variation_or_pct", "variation_or_abs"]]
    fait_prix_or.columns = ["date_id", "prix_or", "volume_or", "prix_sp500", "taux_fed", "variation_or_pct", "variation_or_abs"]

    # Créer Dim_Marche
    dim_marche = merged_df[["date", "volume_sp500"]].copy()
    dim_marche["marche_id"] = range(1, len(dim_marche) + 1)
    dim_marche.columns = ["date_id", "volume_sp500", "marche_id"]

    # Sauvegarde dans HDFS (zone processed, Parquet)
    for df, path in [
        (fait_prix_or, f"/gold_datalake/processed/gold_prices_{today}.parquet"),
        (dim_date, f"/gold_datalake/processed/dim_date_{today}.parquet"),
        (dim_marche, f"/gold_datalake/processed/dim_marche_{today}.parquet")
    ]:
        # Créer un fichier temporaire local
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as temp_file:
            temp_path = temp_file.name
            table = pa.Table.from_pandas(df)
            pq.write_table(table, temp_path)
        
        # Copier le fichier temporaire vers HDFS
        with open(temp_path, "rb") as local_file:
            with HDFS_CLIENT.write(path) as hdfs_file:
                hdfs_file.write(local_file.read())
        
        # Supprimer le fichier temporaire
        os.remove(temp_path)
        print(f"Fichier écrit avec succès dans HDFS: {path}")

    return fait_prix_or, dim_date, dim_marche