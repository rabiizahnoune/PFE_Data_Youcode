import streamlit as st
import json
from kafka import KafkaConsumer
import pandas as pd

st.title("Dashboard Temps Réel - Prix de l'Or")

# Configuration Kafka
consumer = KafkaConsumer(
    'gold-prices',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest'
)

# Initialisation des données
if 'prices' not in st.session_state:
    st.session_state.prices = pd.DataFrame(columns=['timestamp', 'price'])

# Création du graphique
chart = st.line_chart()

for message in consumer:
    data = json.loads(message.value.decode('utf-8'))
    new_row = pd.DataFrame([{
        'timestamp': pd.to_datetime(data['timestamp'], unit='ms'),
        'price': data['price']
    }])
    
    st.session_state.prices = pd.concat([st.session_state.prices, new_row])
    chart.add_rows(new_row[['price']])