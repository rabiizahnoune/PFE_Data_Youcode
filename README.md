# Documentation du Projet Gold Price Analytics

## Introduction

Le projet **Gold Price Analytics** est une plateforme complète conçue pour analyser les données du marché de l'or, en intégrant des données financières, des actualités et des analyses prédictives. Cette solution utilise une architecture moderne basée sur des technologies Big Data et d'intelligence artificielle pour fournir des insights techniques, fondamentaux et interactifs via une interface utilisateur intuitive. Le projet est structuré pour gérer l'ingestion, le traitement, l'analyse et la visualisation des données, avec un déploiement orchestré via Docker.

---

## Table des Matières

1. [Objectifs du Projet](#objectifs-du-projet)
2. [Architecture du Projet](#architecture-du-projet)
3. [Structure des Dossiers et Fichiers](#structure-des-dossiers-et-fichiers)
4. [Technologies Utilisées](#technologies-utilisées)
5. [Configuration et Déploiement](#configuration-et-déploiement)
6. [Pipeline de Données](#pipeline-de-données)
7. [Modèles d'Analyse](#modèles-danalyse)
8. [Tableau de Bord Streamlit](#tableau-de-bord-streamlit)
9. [Surveillance et Logging](#surveillance-et-logging)
10. [Instructions d'Installation](#instructions-dinstallation)
11. [Utilisation](#utilisation)
12. [Contributions et Maintenance](#contributions-et-maintenance)

---

## Objectifs du Projet

Le projet vise à :
- **Analyser les données du marché de l'or** : Collecter et traiter les données historiques et en temps réel sur les prix de l'or, les indices S&P 500, les taux d'intérêt fédéraux et les actualités pertinentes.
- **Fournir des insights techniques et fondamentaux** : Offrir des analyses basées sur des indicateurs techniques (moyennes mobiles, variations de prix) et des analyses fondamentales via l'intégration d'actualités.
- **Prédire les tendances** : Utiliser des modèles d'apprentissage automatique pour anticiper les mouvements des prix de l'or.
- **Interagir avec les utilisateurs** : Proposer une interface utilisateur interactive via Streamlit et un module de chat AI basé sur Gemini pour répondre aux questions des utilisateurs.

---

## Architecture du Projet

L'architecture est modulaire et basée sur des conteneurs Docker, orchestrés par `docker-compose.yml`. Elle comprend :

1. **Ingestion des Données** :
   - **Sources** : Données financières via yfinance et FRED, actualités via NewsAPI.
   - **Outils** : Apache Kafka pour le streaming, Airflow pour l'orchestration des tâches.
2. **Stockage** :
   - **HDFS** : Stockage des données brutes et traitées au format CSV et Parquet.
   - **Snowflake** : Base de données analytique pour les tables dimensionnelles et factuelles.
   - **Cassandra** : Stockage des actualités et des analyses d'impact.
3. **Traitement** :
   - **Spark** : Traitement des flux de données et analyse des actualités.
   - **MLflow** : Gestion des modèles d'apprentissage automatique.
4. **Visualisation** :
   - **Streamlit** : Tableau de bord interactif pour l'analyse technique, fondamentale et le chat AI.
5. **Surveillance** :
   - Airflow pour le monitoring des pipelines.
   - Discord pour les alertes en cas d'erreurs dans les logs Spark.

---

## Structure des Dossiers et Fichiers

La structure du projet est organisée comme suit :


C:\Users\Youcode\Documents\gold_price_project
├── .gitignore
├── data.sql.txt
├── docker-compose.yml
├── test.py
├── x.py
├── airflow/
│   ├── requirements.txt
│   └── dags/
│       ├── extract_gold.py
│       ├── load.py
│       ├── monitor_spark.py
│       ├── news_batch.py
│       ├── stream.py
│       ├── while.py
│       └── gold_trading_dashboard.py
├── docker/
│   ├── agent.dockerfile
│   ├── ai_trainer.dockerfile
│   ├── apache.dockerfile
│   ├── hadoop.dockerfile
│   └── requirements.txt
├── keys/
├── ML/
│   ├── commande.sh
│   ├── Dockerfile
│   ├── fine_tune_model.py
│   ├── mlflow.db
│   ├── requirements.txt
│   └── train_model.py
├── ollama_llm/
│   ├── Dockerfile
│   └── entrypoint.sh
├── postgres/
│   └── init.sql
├── scripts/
│   ├── batch/
│   └── mapreduce/
└── stream/
├── cassandra/
│   ├── cassandra-rackdc.properties
│   ├── cassandra-setup.cql
│   ├── cassandra.yaml
│   └── Dockerfile
├── kafka/
│   ├── Dockerfile
│   ├── kafka-setup-k8s.sh
│   ├── kafka.properties
│   ├── requirements.txt
│   └── scripts/
│       ├── gold_price.py
│       ├── news_producer.py
│       └── logs/
│           └── kafka.log
└── spark/
├── analyze_news.py
├── commande.sh
├── Dockerfile
├── gold_price_streaming.py
├── requirements.txt
├── spark_consume_test.py
├── store_news_to_hdfs.py
└── logs/
└── spark.log




### Description des Fichiers Principaux

- **.gitignore** : Ignore les environnements virtuels (`myenv`) et les artifacts MLflow (`ML/mlruns`).
- **data.sql.txt** : Définit le schéma Snowflake pour les tables `Dim_Date`, `Dim_Marche`, et `Fait_Prix_Or`.
- **docker-compose.yml** : Configure les services Docker (Airflow, HDFS, Kafka, Spark, MLflow, Cassandra, Streamlit, etc.).
- **test.py** : Script pour télécharger et lire les fichiers CSV/Parquet depuis HDFS, avec prévisualisation des données.
- **x.py** : Script utilitaire pour générer la structure du projet et gérer les interactions avec HDFS.
- **airflow/dags/** : Contient les DAGs Airflow pour l'extraction, le chargement, la surveillance, et l'ingestion des actualités.
  - **extract_gold.py** : Extrait les données de l'or, S&P 500, et taux fédéraux via yfinance et FRED.
  - **load.py** : Charge les données traitées dans Snowflake avec des opérations MERGE pour éviter les doublons.
  - **monitor_spark.py** : Surveille les logs Spark pour détecter les erreurs et envoie des alertes via Discord.
  - **news_batch.py** : Ingère les actualités via NewsAPI et les stocke dans HDFS.
  - **gold_trading_dashboard.py** : Tableau de bord Streamlit pour l'analyse technique, fondamentale et le chat AI.
- **ML/** : Contient les scripts pour l'entraînement et le fine-tuning des modèles d'apprentissage automatique.
  - **train_model.py** : Entraîne un modèle de régression linéaire pour prédire les prix de l'or.
  - **fine_tune_model.py** : Fine-tune un modèle existant avec de nouvelles données mensuelles.
- **stream/** : Gère le traitement en temps réel des données et actualités.
  - **kafka/** : Configure Kafka pour le streaming des données de prix et d'actualités.
  - **spark/** : Traite les flux de données et analyse les actualités avec Spark.
  - **cassandra/** : Stocke les actualités et leurs analyses d'impact.
- **ollama_llm/** : Configuration pour le modèle Mistral (commenté dans `docker-compose.yml`).

---

## Technologies Utilisées

- **Ingestion et Orchestration** :
  - Apache Airflow (2.7.1) : Orchestration des pipelines.
  - Apache Kafka (7.4.0) : Streaming des données en temps réel.
- **Stockage** :
  - Hadoop HDFS (3.2.1) : Stockage distribué.
  - Snowflake : Data warehouse pour les analyses.
  - Cassandra (4.1) : Base NoSQL pour les actualités.
  - PostgreSQL (13) : Métadonnées Airflow.
- **Traitement** :
  - Apache Spark (3.5.1) : Traitement des données en streaming et batch.
  - MLflow (2.17.2) : Gestion des modèles d'apprentissage automatique.
- **Visualisation** :
  - Streamlit : Tableau de bord interactif.
- **IA et Analyse** :
  - Gemini AI : Analyse des actualités et chat interactif.
  - scikit-learn : Modèles d'apprentissage automatique.
- **Conteneurisation** :
  - Docker : Conteneurisation des services.
  - Docker Compose : Orchestration des conteneurs.

---

## Configuration et Déploiement

### Pré-requis

- Docker et Docker Compose installés.
- Compte Snowflake avec les identifiants configurés dans `load.py` et `gold_trading_dashboard.py`.
- Clé API NewsAPI pour `news_batch.py` et `news_producer.py`.
- Clé API Gemini pour `gold_trading_dashboard.py` et `analyze_news.py`.
- Clé FRED pour `extract_gold.py`.

### Configuration

1. **Clés API** :
   - Configurez les clés API dans les fichiers respectifs :
     - `ALPHA_VANTAGE_KEY` et `FRED_KEY` dans `extract_gold.py`.
     - `API_KEY` dans `news_batch.py` et `news_producer.py`.
     - `GEMINI_API_KEY` dans `gold_trading_dashboard.py` et `analyze_news.py`.
   - Assurez-vous que les identifiants Snowflake sont corrects dans `load.py` et `gold_trading_dashboard.py`.

2. **HDFS et Permissions** :
   - Lescript (`test.py`) définit des permissions 777 sur les répertoires HDFS pour garantir l'accès.

3. **Docker Compose** :
   - Le fichier `docker-compose.yml` définit les services suivants :
     - **postgres** : Base de données pour Airflow.
     - **airflow-init**, **airflow-webserver**, **airflow-scheduler** : Composants Airflow.
     - **namenode**, **datanode1** : HDFS pour le stockage distribué.
     - **zookeeper**, **kafka**, **kafka-init** : Gestion du streaming.
     - **spark** : Traitement des données.
     - **mlflow-container** : Suivi des modèles.
     - **cassandra** : Stockage des actualités.
     - **streamlit** : Interface utilisateur.
     - **ollama** (commenté) : Pour le modèle Mistral.

---

## Pipeline de Données

### 1. Ingestion
- **extract_gold.py** : Extrait les données via yfinance (prix de l'or, S&P 500) et FRED (taux fédéraux), puis les stocke dans HDFS sous `/gold_datalake/raw`.
- **news_batch.py** : Ingère les actualités via NewsAPI, les sauvegarde en JSON, et les stocke dans HDFS.
- **gold_price.py** : Produit des données simulées de prix de l'or vers le topic Kafka `gold-price`.
- **news_producer.py** : Envoie les actualités vers le topic Kafka `gold-news`.

### 2. Traitement
- **gold_price_streaming.py** : Lit le flux Kafka `gold-price`, parse les données JSON, et les écrit dans HDFS sous `/gold_price/aggregates` au format Parquet, partitionné par année, mois et jour.
- **store_news_to_hdfs.py** : Convertit les fichiers JSON d'actualités en Parquet et les stocke dans HDFS.
- **analyze_news.py** : Analyse les actualités avec Gemini AI et stocke les résultats dans Cassandra.

### 3. Chargement
- **load.py** : Lit les fichiers Parquet depuis HDFS, les charge dans Snowflake avec des opérations MERGE pour éviter les doublons, et met à jour les tables `Dim_Date`, `Dim_Marche`, et `Fait_Prix_Or`.

### 4. Visualisation
- **gold_trading_dashboard.py** : Fournit une interface Streamlit avec trois modes :
  - **Analyse Technique** : Graphiques en chandeliers, moyennes mobiles, corrélations.
  - **Analyse Fondamentale** : Tableau des actualités avec impact, statistiques par source.
  - **Chat AI** : Interaction avec Gemini AI pour répondre aux questions.

---

## Modèles d'Analyse

### Entraînement Initial
- **train_model.py** :
  - Utilise un modèle de régression linéaire pour prédire les prix de l'or à t+30 minutes, basé sur les 30 minutes précédentes.
  - Les données sont lues depuis `/app/gold_price_historical.csv`.
  - Le modèle est loggé dans MLflow avec la métrique RMSE.

### Fine-Tuning
- **fine_tune_model.py** :
  - Fine-tune un modèle existant avec les données d'un mois spécifique, lues depuis HDFS.
  - Ajoute des features de décalage (lags) et enregistre le modèle mis à jour dans MLflow.

---

## Tableau de Bord Streamlit

Le tableau de bord (`gold_trading_dashboard.py`) offre :
- **Analyse Technique** :
  - Graphiques en chandeliers pour les prix de l'or.
  - Moyennes mobiles (7 et 30 jours).
  - Corrélations entre prix de l'or, S&P 500, et taux fédéraux.
  - Statistiques descriptives.
- **Analyse Fondamentale** :
  - Tableau des actualités avec liens cliquables et impacts (évalués via Gemini AI).
  - Statistiques sur les sources d'actualités.
- **Chat AI** :
  - Interface interactive pour poser des questions sur le marché de l'or, alimentée par Gemini AI.

---

## Surveillance et Logging

- **monitor_spark.py** : Un DAG Airflow vérifie les logs Spark (`spark.log`) pour détecter les erreurs et envoie des alertes via un webhook Discord.
- **Logging** : Les scripts Python utilisent le module `logging` pour journaliser les opérations, avec des niveaux INFO, WARNING, et ERROR.

---

## Instructions d'Installation

1. **Cloner le Répertoire** :
   ```bash
   git clone https://github.com/rabiizahnoune/PFE_Data_Youcode
   cd gold_price_project


2. **Lancer Docker Compose** :
   ```bash
   docker-compose up -d
   ```

3. **Accéder aux Services** :
   * Airflow : http://localhost:8080 (admin/admin)
   * HDFS NameNode : http://localhost:9870
   * MLflow : http://localhost:5000
   * Streamlit : http://localhost:8050

4. **Exécuter les DAGs Airflow** :
   * Activez les DAGs dans l'interface Airflow pour lancer les pipelines.

## Utilisation

1. **Extraction des Données** :
   * Exécutez le DAG `extract_gold` pour collecter les données financières.
   * Exécutez le DAG `news_batch` pour ingérer les actualités.

2. **Traitement et Chargement** :
   * Le DAG `load` charge les données dans Snowflake.
   * Les scripts Spark (`gold_price_streaming.py`, `store_news_to_hdfs.py`, `analyze_news.py`) traitent les flux de données.

3. **Visualisation** :
   * Accédez au tableau de bord Streamlit à http://localhost:8050.
   * Sélectionnez l'année, la période, et le type d'analyse (Technique, Fondamentale, Chat AI).

4. **Surveillance** :
   * Vérifiez les logs dans Airflow et les alertes Discord pour les erreurs.

## Contributions et Maintenance

* **Contributeurs** : Ajoutez des fonctionnalités via des pull requests.
* **Maintenance** :
   * Mettez à jour les clés API si elles expirent.
   * Vérifiez régulièrement les logs Spark et Airflow.
   * Fine-tunez les modèles périodiquement avec `fine_tune_model.py`.

---

**© 2024 Gold Analytics Pro** *Plateforme Professionnelle d'Analyse de Marché*
