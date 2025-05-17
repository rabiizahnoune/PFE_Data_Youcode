# Utiliser une image de base Python plus complète (solution 5)
FROM python:3.9

# Définir le répertoire de travail
WORKDIR /ai_scripts

# Installer les dépendances système nécessaires (solution 4)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libblas-dev \
    liblapack-dev \
    libatlas-base-dev \
    && rm -rf /var/lib/apt/lists/*

# Copier le fichier requirements.txt et installer les dépendances
COPY requirements.txt .
# Augmenter le timeout de pip à 1000 secondes (solution 1)
RUN pip install --default-timeout=1000 -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

CMD ["bash", "-c", "export TF_ENABLE_ONEDNN_OPTS=0 && export TF_CPP_MIN_LOG_LEVEL=2 && mlflow server --host 0.0.0.0 --port 5000 --backend-store-uri /mlflow/mlruns --default-artifact-root /mlflow/artifacts"]
