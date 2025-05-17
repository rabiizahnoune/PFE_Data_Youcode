# import pandas as pd
# from hdfs import InsecureClient
# import pyarrow.parquet as pq
# import pyarrow as pa
# from datetime import datetime
# from io import BytesIO
# import subprocess
# from sqlalchemy import create_engine
# from snowflake.sqlalchemy import URL

# # Configurations codées en dur
# HDFS_CLIENT = InsecureClient("http://namenode:9870", user="hadoop")
# SNOWFLAKE_CONN = {
#     "account": "RRWRUAS-VI48008",
#     "user": "RABIIZAHNOUNE05",
#     "password": "CdFbMNyjc87vueV",
#     "database": "GOLD_ANALYSIS",
#     "schema": "MARCHE",
#     "warehouse": "COMPUTE_WH"
# }

# def load_gold_data():
#     today = datetime.now().strftime("%Y-%m-%d")
    
#     # Définir les permissions HDFS sur le répertoire processed
#     hdfs_dir = "/gold_datalake/processed"
#     try:
#         subprocess.run([
#             "docker", "exec", "-u", "root", "namenode",
#             "hdfs", "dfs", "-chmod", "-R", "777", hdfs_dir
#         ], check=True)
#         print(f"Permissions définies avec succès sur {hdfs_dir} (777)")
#     except subprocess.CalledProcessError as e:
#         print(f"Erreur lors de la définition des permissions sur {hdfs_dir}: {e}")
#         raise
    
#     # Liste des fichiers Parquet à lire
#     files = [
#         (f"/gold_datalake/processed/gold_prices_{today}.parquet", "fait_prix_or"),
#         (f"/gold_datalake/processed/dim_date_{today}.parquet", "dim_date"),
#         (f"/gold_datalake/processed/dim_marche_{today}.parquet", "dim_marche")
#     ]
    
#     # Lire les fichiers Parquet depuis HDFS
#     dataframes = {}
#     for path, name in files:
#         with HDFS_CLIENT.read(path) as reader:
#             # Lire le contenu dans un buffer
#             content = reader.read()
#             # Créer un BytesIO à partir du contenu
#             buffer = BytesIO(content)
#             # Lire le fichier Parquet depuis le buffer
#             table = pq.read_table(buffer)
#             # Convertir en DataFrame pandas
#             dataframes[name] = table.to_pandas()
#             print(f"Fichier Parquet lu avec succès depuis HDFS: {path}")

#     # Extraire les DataFrames
#     fait_prix_or = dataframes["fait_prix_or"]
#     dim_date = dataframes["dim_date"]
#     dim_marche = dataframes["dim_marche"]

#     # Créer une connexion SQLAlchemy pour Snowflake
#     engine = create_engine(URL(
#         account=SNOWFLAKE_CONN["account"],
#         user=SNOWFLAKE_CONN["user"],
#         password=SNOWFLAKE_CONN["password"],
#         database=SNOWFLAKE_CONN["database"],
#         schema=SNOWFLAKE_CONN["schema"],
#         warehouse=SNOWFLAKE_CONN["warehouse"]
#     ))

#     # Connexion à Snowflake
#     with engine.connect() as conn:
#         # Charger Dim_Date
#         conn.execute("TRUNCATE TABLE GOLD_ANALYSIS.MARCHE.DIM_DATE")
#         dim_date.to_sql("DIM_DATE", conn, schema="GOLD_ANALYSIS.MARCHE", index=False, if_exists="append", method="multi")
#         print("Table Dim_Date chargée avec succès dans Snowflake")

#         # Charger Dim_Marche
#         conn.execute("TRUNCATE TABLE GOLD_ANALYSIS.MARCHE.DIM_MARCHE")
#         dim_marche.to_sql("DIM_MARCHE", conn, schema="GOLD_ANALYSIS.MARCHE", index=False, if_exists="append", method="multi")
#         print("Table Dim_Marche chargée avec succès dans Snowflake")

#         # Charger Fait_Prix_Or
#         conn.execute("TRUNCATE TABLE GOLD_ANALYSIS.MARCHE.FAIT_PRIX_OR")
#         fait_prix_or.to_sql("FAIT_PRIX_OR", conn, schema="GOLD_ANALYSIS.MARCHE", index=False, if_exists="append", method="multi")
#         print("Table Fait_Prix_Or chargée avec succès dans Snowflake")

#     print("Connexion Snowflake fermée")





import pandas as pd
from hdfs import InsecureClient
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
from io import BytesIO
import subprocess
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

# Configurations codées en dur
HDFS_CLIENT = InsecureClient("http://namenode:9870", user="hadoop")
SNOWFLAKE_CONN = {
    "account": "RRWRUAS-VI48008",
    "user": "RABIIZAHNOUNE05",
    "password": "CdFbMNyjc87vueV",
    "database": "GOLD_ANALYSIS",
    "schema": "MARCHE",
    "warehouse": "COMPUTE_WH"
}

def load_gold_data():
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Définir les permissions HDFS sur le répertoire processed
    hdfs_dir = "/gold_datalake/processed"
    try:
        subprocess.run([
            "docker", "exec", "-u", "root", "namenode",
            "hdfs", "dfs", "-chmod", "-R", "777", hdfs_dir
        ], check=True)
        print(f"Permissions définies avec succès sur {hdfs_dir} (777)")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de la définition des permissions sur {hdfs_dir}: {e}")
        raise
    
    # Liste des fichiers Parquet à lire
    files = [
        (f"/gold_datalake/processed/gold_prices_{today}.parquet", "fait_prix_or"),
        (f"/gold_datalake/processed/dim_date_{today}.parquet", "dim_date"),
        (f"/gold_datalake/processed/dim_marche_{today}.parquet", "dim_marche")
    ]
    
    # Lire les fichiers Parquet depuis HDFS
    dataframes = {}
    for path, name in files:
        with HDFS_CLIENT.read(path) as reader:
            content = reader.read()
            buffer = BytesIO(content)
            table = pq.read_table(buffer)
            dataframes[name] = table.to_pandas()
            print(f"Fichier Parquet lu avec succès depuis HDFS: {path}")

    # Extraire les DataFrames
    fait_prix_or = dataframes["fait_prix_or"]
    dim_date = dataframes["dim_date"]
    dim_marche = dataframes["dim_marche"]

    # Créer une connexion SQLAlchemy pour Snowflake
    engine = create_engine(URL(
        account=SNOWFLAKE_CONN["account"],
        user=SNOWFLAKE_CONN["user"],
        password=SNOWFLAKE_CONN["password"],
        database=SNOWFLAKE_CONN["database"],
        schema=SNOWFLAKE_CONN["schema"],
        warehouse=SNOWFLAKE_CONN["warehouse"]
    ))

    # Connexion à Snowflake
    with engine.connect() as conn:
        # 1. Charger Dim_Date avec MERGE
        # Charger le DataFrame dans une table temporaire
        dim_date.to_sql("TEMP_DIM_DATE", conn, schema="GOLD_ANALYSIS.MARCHE", index=False, if_exists="replace", method="multi")
        merge_dim_date_query = """
        MERGE INTO GOLD_ANALYSIS.MARCHE.DIM_DATE AS target
        USING GOLD_ANALYSIS.MARCHE.TEMP_DIM_DATE AS source
        ON target.date_id = source.date_id
        WHEN MATCHED THEN
            UPDATE SET
                jour = source.jour,
                mois = source.mois,
                annee = source.annee,
                heure = source.heure,
                trimestre = source.trimestre
        WHEN NOT MATCHED THEN
            INSERT (date_id, jour, mois, annee, heure, trimestre)
            VALUES (source.date_id, source.jour, source.mois, source.annee, source.heure, source.trimestre);
        """
        conn.execute(merge_dim_date_query)
        conn.execute("DROP TABLE IF EXISTS GOLD_ANALYSIS.MARCHE.TEMP_DIM_DATE")  # Nettoyer la table temporaire
        print("Table Dim_Date mise à jour avec succès dans Snowflake (sans doublons)")

        # 2. Charger Dim_Marche avec MERGE
        dim_marche.to_sql("TEMP_DIM_MARCHE", conn, schema="GOLD_ANALYSIS.MARCHE", index=False, if_exists="replace", method="multi")
        merge_dim_marche_query = """
        MERGE INTO GOLD_ANALYSIS.MARCHE.DIM_MARCHE AS target
        USING GOLD_ANALYSIS.MARCHE.TEMP_DIM_MARCHE AS source
        ON target.marche_id = source.marche_id
        WHEN MATCHED THEN
            UPDATE SET
                date_id = source.date_id,
                volume_sp500 = source.volume_sp500
        WHEN NOT MATCHED THEN
            INSERT (marche_id, date_id, volume_sp500)
            VALUES (source.marche_id, source.date_id, source.volume_sp500);
        """
        conn.execute(merge_dim_marche_query)
        conn.execute("DROP TABLE IF EXISTS GOLD_ANALYSIS.MARCHE.TEMP_DIM_MARCHE")  # Nettoyer la table temporaire
        print("Table Dim_Marche mise à jour avec succès dans Snowflake (sans doublons)")

        # 3. Charger Fait_Prix_Or avec MERGE
        fait_prix_or.to_sql("TEMP_FAIT_PRIX_OR", conn, schema="GOLD_ANALYSIS.MARCHE", index=False, if_exists="replace", method="multi")
        merge_fait_prix_or_query = """
        MERGE INTO GOLD_ANALYSIS.MARCHE.FAIT_PRIX_OR AS target
        USING GOLD_ANALYSIS.MARCHE.TEMP_FAIT_PRIX_OR AS source
        ON target.date_id = source.date_id
        WHEN MATCHED THEN
            UPDATE SET
                prix_or = source.prix_or,
                volume_or = source.volume_or,
                prix_sp500 = source.prix_sp500,
                taux_fed = source.taux_fed,
                variation_or_pct = source.variation_or_pct,
                variation_or_abs = source.variation_or_abs
        WHEN NOT MATCHED THEN
            INSERT (date_id, prix_or, volume_or, prix_sp500, taux_fed, variation_or_pct, variation_or_abs)
            VALUES (source.date_id, source.prix_or, source.volume_or, source.prix_sp500, source.taux_fed, source.variation_or_pct, source.variation_or_abs);
        """
        conn.execute(merge_fait_prix_or_query)
        conn.execute("DROP TABLE IF EXISTS GOLD_ANALYSIS.MARCHE.TEMP_FAIT_PRIX_OR")  # Nettoyer la table temporaire
        print("Table Fait_Prix_Or mise à jour avec succès dans Snowflake (sans doublons)")

    print("Connexion Snowflake fermée")

if __name__ == "__main__":
    load_gold_data()