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
