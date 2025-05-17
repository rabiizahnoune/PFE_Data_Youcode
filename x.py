import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

# Connexion à Snowflake
engine = create_engine(URL(
    account="RRWRUAS-VI48008",
    user="RABIIZAHNOUNE05",
    password="CdFbMNyjc87vueV",
    database="GOLD_ANALYSIS",
    schema="MARCHE",
    warehouse="COMPUTE_WH"
))

# Charger les données
query = """
SELECT fp.date_id, fp.prix_or, fp.prix_sp500, fp.taux_fed, fp.variation_or_pct
FROM GOLD_ANALYSIS.MARCHE.Fait_Prix_Or fp
JOIN GOLD_ANALYSIS.MARCHE.Dim_Date dd ON fp.date_id = dd.date_id
WHERE dd.annee = 2025
ORDER BY fp.date_id;
"""
df = pd.read_sql(query, engine)

# Visualisation
fig, ax1 = plt.subplots(figsize=(12, 6))

# Prix de l'or et S&P 500
ax1.plot(df["date_id"], df["prix_or"], label="Prix Or", color="gold")
ax1.plot(df["date_id"], df["prix_sp500"], label="Prix S&P 500", color="blue")
ax1.set_xlabel("Date")
ax1.set_ylabel("Prix (Or, S&P 500)", color="blue")
ax1.tick_params(axis="y", labelcolor="blue")
ax1.legend(loc="upper left")

# Taux fédéraux (échelle secondaire)
ax2 = ax1.twinx()
ax2.plot(df["date_id"], df["taux_fed"], label="Taux Fed", color="red")
ax2.set_ylabel("Taux Fédéral (%)", color="red")
ax2.tick_params(axis="y", labelcolor="red")
ax2.legend(loc="upper right")

# Variation % de l'or (échelle tertiaire)
ax3 = ax1.twinx()
ax3.spines["right"].set_position(("outward", 60))
ax3.plot(df["date_id"], df["variation_or_pct"], label="Variation Or (%)", color="green", linestyle="--")
ax3.set_ylabel("Variation Or (%)", color="green")
ax3.tick_params(axis="y", labelcolor="green")
ax3.legend(loc="lower right")

plt.title("Évolution des Prix de l'Or, S&P 500, Taux Fédéraux et Variation de l'Or")
plt.tight_layout()
plt.savefig("/app/prices_plot.png")  # Sauvegarder le graphique
plt.close()  # Fermer la figure