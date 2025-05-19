# import json
# import random
# import time
# from kafka import KafkaProducer

# # Kafka configuration
# producer = KafkaProducer(
#     bootstrap_servers='kafka:9092',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# base_price = 1800

# # Envoi continu de données simulées
# while True:
#     price = base_price + random.uniform(-10, 10)
#     timestamp = int(time.time() * 1000)

#     message = {
#         'timestamp': timestamp,
#         'price': round(price, 2)
#     }

#     producer.send('gold-price', message)
#     print("Message envoyé :", message)

#     time.sleep(1)


import json
import random
import time
from kafka import KafkaProducer
from datetime import datetime

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Prix de base
base_price = 3240  # Ajusté pour correspondre à vos données

# Envoi continu de données simulées
while True:
    # Timestamp actuel au format requis (YYYY-MM-DD HH:MM:SS+00:00)
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S+00:00")

    # Générer le prix principal (Price)
    price = base_price + random.uniform(-2, 2)  # Variations plus petites pour plus de réalisme

    # Générer Open, High, Low, Close autour du prix
    open_price = price + random.uniform(-0.5, 0.5)  # Légère variation par rapport à Price
    high_price = max(price, open_price) + random.uniform(0, 0.7)  # High est légèrement supérieur
    low_price = min(price, open_price) - random.uniform(0, 0.7)  # Low est légèrement inférieur
    close_price = price + random.uniform(-0.5, 0.5)  # Close est proche de Price

    # Arrondir à 4 décimales pour correspondre au format
    price = round(price, 4)
    open_price = round(open_price, 4)
    high_price = round(high_price, 4)
    low_price = round(low_price, 4)
    close_price = round(close_price, 4)

    # Créer le message
    message = {
        "Datetime": timestamp,
        "Price": price,
        "Close": close_price,
        "High": high_price,
        "Low": low_price,
        "Open": open_price
    }

    # Envoyer le message au topic Kafka
    producer.send('gold-price', message)
    print("Message envoyé :", message)

    # Attendre 60 secondes pour simuler une fréquence de 1 minute
    time.sleep(60)