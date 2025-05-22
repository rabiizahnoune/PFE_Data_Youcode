import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import plotly.express as px
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import networkx as nx
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import numpy as np
import requests

import google.generativeai as genai  # Ajouté pour Gemini

# Configuration de l'API Gemini
GEMINI_API_KEY = "AIzaSyBqnMwcPJZqySSrdrcqnG828KmW_f7KkfE"
try:
    genai.configure(api_key=GEMINI_API_KEY)
except Exception as e:
    st.error(f"Erreur de configuration de l'API Gemini : {str(e)}")
# Configuration de la page
st.set_page_config(
    page_title="Gold Analytics Pro",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Style CSS personnalisé
st.markdown("""
    <style>
        .main {background-color: #0E1117; padding: 2rem;}
        h1 {color: #FFD700; border-bottom: 2px solid #FFD700;}
        .sidebar .sidebar-content {background-color: #1A1D24;}
        .metric-card {padding: 1.5rem; border-radius: 10px; background-color: #1A1D24;}
        footer {color: #FFD700; text-align: center; padding: 1rem;}
        .stDataFrame {border-radius: 10px;}
        .news-card {
            background-color: #1A1D24;
            border-radius: 10px;
            padding: 1rem;
            margin-bottom: 1rem;
            border-left: 4px solid #FFD700;
        }
        .impact-high {color: #FF4B4B;}
        .impact-medium {color: #FFA500;}
        .impact-low {color: #2ECC71;}
        .sentiment-positive {color: #2ECC71;}
        .sentiment-negative {color: #FF4B4B;}
        .sentiment-neutral {color: #3498DB;}
    </style>
""", unsafe_allow_html=True)

# Connexion à Snowflake
@st.cache_resource
def create_db_connection():
    return create_engine(URL(
        account="LEKYCXI-ZO52842",
        user="ZAHNOUNE",
        password="CdFbMNyjc87vueV",
        database="GOLD_ANALYSIS",
        schema="MARCHE",
        warehouse="COMPUTE_WH"
    ))

engine = create_db_connection()

# Chargement des données
@st.cache_data
def load_data():
    query = """
    SELECT fp.date_id, fp.prix_or, fp.prix_sp500, fp.taux_fed, fp.variation_or_pct
    FROM GOLD_ANALYSIS.MARCHE.Fait_Prix_Or fp
    JOIN GOLD_ANALYSIS.MARCHE.Dim_Date dd ON fp.date_id = dd.date_id
    ORDER BY fp.date_id;
    """
    df = pd.read_sql(query, engine)
    df["date_id"] = pd.to_datetime(df["date_id"])
    df["ma7_or"] = df["prix_or"].rolling(window=7).mean()
    df["ma30_or"] = df["prix_or"].rolling(window=30).mean()
    return df

df = load_data()

# Sidebar
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3135/3135715.png", width=100)
    st.header("🔍 Paramètres d'Analyse")
    
    # Filtres dans des colonnes
    col1, col2 = st.columns(2)
    with col1:
        selected_year = st.selectbox("Année", options=sorted(df["date_id"].dt.year.unique(), reverse=True))
    with col2:
        analysis_type = st.selectbox("Type d'Analyse", ["Technique", "Fundamentale", "Chat AI"])
    
    date_range = st.date_input("Période", 
                             [df["date_id"].min(), df["date_id"].max()],
                             min_value=df["date_id"].min(),
                             max_value=df["date_id"].max())
    
    st.info("ℹ️ Sélectionnez les paramètres d'analyse et la période souhaitée")

if analysis_type == 'Technique':
    # Filtrage des données
    filtered_df = df[(df["date_id"].dt.year == selected_year) & 
                    (df["date_id"] >= pd.to_datetime(date_range[0])) & 
                    (df["date_id"] <= pd.to_datetime(date_range[1]))]

    # KPI Cards
    st.subheader(f"📊 Indicateurs Clés - {selected_year}")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Prix Actuel Or", f"${filtered_df['prix_or'].iloc[-1]:,.2f}", 
                f"{filtered_df['variation_or_pct'].iloc[-1]:.2f}%")
    with col2:
        st.metric("S&P 500", f"${filtered_df['prix_sp500'].iloc[-1]:,.2f}")
    with col3:
        st.metric("Taux Fed", f"{filtered_df['taux_fed'].iloc[-1]:.2f}%")
    with col4:
        ytd_change = ((filtered_df['prix_or'].iloc[-1] - filtered_df['prix_or'].iloc[0])/filtered_df['prix_or'].iloc[0])*100
        st.metric("YTD Or", f"{ytd_change:.2f}%")

    # Graphique principal
    fig1 = go.Figure()
    fig1.add_trace(go.Candlestick(x=filtered_df['date_id'],
                                open=filtered_df['prix_or'].shift(1),
                                high=filtered_df['prix_or'].rolling(5).max(),
                                low=filtered_df['prix_or'].rolling(5).min(),
                                close=filtered_df['prix_or'],
                                name="Cours Or",
                                increasing_line_color='#FFD700',
                                decreasing_line_color='#444'))

    fig1.add_trace(go.Scatter(x=filtered_df['date_id'], 
                            y=filtered_df['ma7_or'],
                            name='MA 7j',
                            line=dict(color='#00FF00', width=1)))

    fig1.add_trace(go.Scatter(x=filtered_df['date_id'], 
                            y=filtered_df['ma30_or'],
                            name='MA 30j',
                            line=dict(color='#FF0000', width=1)))

    fig1.update_layout(
        title=f'Analyse Technique du Cours de l\'Or - {selected_year}',
        xaxis_title='Date',
        yaxis_title='Prix (USD)',
        template='plotly_dark',
        hovermode="x unified",
        showlegend=True,
        height=600,
        xaxis_rangeslider_visible=False
    )

    # Graphique secondaire
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(x=filtered_df['date_id'], 
                        y=filtered_df['variation_or_pct'],
                        name='Variation Journalière',
                        marker_color='#1E90FF'))

    fig2.add_trace(go.Scatter(x=filtered_df['date_id'], 
                            y=filtered_df['taux_fed'],
                            name='Taux Fed',
                            line=dict(color='#FF1493', width=2),
                            yaxis='y2'))

    fig2.update_layout(
        title='Analyse Macroéconomique',
        xaxis_title='Date',
        yaxis_title='Variation (%)',
        yaxis2=dict(title='Taux (%)', overlaying='y', side='right'),
        template='plotly_dark',
        hovermode="x unified",
        height=400
    )

    # Affichage des graphiques
    st.plotly_chart(fig1, use_container_width=True)
    st.plotly_chart(fig2, use_container_width=True)

    # Section d'analyse
    st.subheader("🔍 Analyse des Données")
    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown("**Corrélations**")
        corr_matrix = filtered_df[['prix_or', 'prix_sp500', 'taux_fed']].corr()
        fig_corr = px.imshow(corr_matrix,
                            text_auto=True,
                            color_continuous_scale='Viridis',
                            labels=dict(x="Variables", y="Variables"))
        st.plotly_chart(fig_corr, use_container_width=True)

    with col2:
        st.markdown("**Statistiques Descriptives**")
        stats = filtered_df.describe().T.reset_index()
        stats.columns = ['Métrique', 'Count', 'Moyenne', 'Std', 'Min', '25%', '50%', '75%', 'Max']
        st.dataframe(stats.style.background_gradient(cmap='YlGnBu'),
                    use_container_width=True,
                    height=400)

    # Footer
    st.markdown("---")
    st.markdown("""
        <div style='text-align: center; padding: 1rem;'>
            <p style='color: #FFD700;'>© 2024 Gold Analytics Pro • Plateforme Professionnelle d'Analyse de Marché</p>
            <p style='font-size: 0.8rem; color: #888;'>Données mises à jour quotidiennement à 00:00 UTC</p>
        </div>
    """, unsafe_allow_html=True)
elif analysis_type == 'Fundamentale':
    # Connexion Cassandra (version Docker)
    @st.cache_resource
    def create_cassandra_connection():
        try:
            cluster = Cluster(
                ['cassandra'],
                port=9042,
                protocol_version=4
            )
            session = cluster.connect('gold_market', wait_for_all_pools=True)
            return session
        except Exception as e:
            st.error(f"Erreur de connexion Cassandra: {str(e)}")
            return None

    session = create_cassandra_connection()

    if session:
        # Chargement des données
        @st.cache_data(ttl=60)
        def load_news_impact_data():
            try:
                # Récupération des actualités
                news_rows = session.execute("SELECT title, source, published_at, url, impact FROM news_impact")
                news_df = pd.DataFrame(list(news_rows))
                
                if not news_df.empty:
                    # Convertir la date en datetime et gérer les timezones
                    news_df['published_at'] = pd.to_datetime(news_df['published_at']).dt.tz_localize(None)
                    # Trier par date (plus récent en premier)
                    news_df = news_df.sort_values('published_at', ascending=False)
                
                return news_df
            except Exception as e:
                st.error(f"Erreur de chargement: {str(e)}")
                return pd.DataFrame()

        news_df = load_news_impact_data()

        # Titre
        st.title("🔍 Analyse Fondamentale du Marché de l'Or")
        st.write("Liste des actualités avec leur impact et source")

        # Tableau des actualités
        if not news_df.empty:
            st.subheader("📰 Liste des Actualités")
            # Préparer le tableau pour l'affichage
            display_df = news_df[['title', 'source', 'published_at', 'url', 'impact']].copy()
            display_df['published_at'] = display_df['published_at'].dt.strftime('%d/%m/%Y %H:%M')
            display_df = display_df.rename(columns={
                'title': 'Titre',
                'source': 'Source',
                'published_at': 'Date',
                'url': 'Lien',
                'impact': 'Impact'
            })
            
            # Rendre les liens cliquables
            display_df['Lien'] = display_df['Lien'].apply(
                lambda x: f'<a href="{x}" target="_blank">Consulter</a>' if pd.notna(x) else ''
            )
            
            # Afficher le tableau
            st.markdown(
                display_df.to_html(escape=False, index=False),
                unsafe_allow_html=True
            )

            # Calculer le nombre d'actualités par source
            st.subheader("📊 Nombre d'Actualités par Source")
            source_counts = news_df['source'].value_counts().reset_index()
            source_counts.columns = ['Source', 'Nombre']
            
           
        # Footer avec actualisation
        st.markdown("---")
        st.markdown("🔄 Actualisation automatique toutes les 60 secondes")
        st.markdown("""
            <div style='text-align: center; padding: 1rem;'>
                <p style='color: #FFD700;'>© 2024 Gold Analytics Pro • Plateforme Professionnelle d'Analyse de Marché</p>
            </div>
        """, unsafe_allow_html=True)

elif analysis_type == 'Chat AI':
    st.title("🤖 Chat AI - Analyse Interactive avec Gemini")
    st.write("Posez vos questions sur le marché de l'or ou l'analyse financière. Gemini vous répondra en temps réel.")

    # Clé API Gemini
    GEMINI_API_KEY = "AIzaSyBqnMwcPJZqySSrdrcqnG828KmW_f7KkfE"
    API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

    # Initialiser l'historique du chat dans la session
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # Zone de saisie pour la question
    user_input = st.text_area("💬 Votre question :", height=100, placeholder="Ex. : Quels facteurs influencent le prix de l'or ?")

    # Bouton pour envoyer la question
    if st.button("Envoyer", key="send_chat"):
        if user_input:
            try:
                # Préparer la requête pour l'API Gemini
                headers = {
                    "Content-Type": "application/json"
                }
                payload = {
                    "contents": [
                        {
                            "parts": [
                                {"text": user_input}
                            ]
                        }
                    ]
                }
                response = requests.post(f"{API_URL}?key={GEMINI_API_KEY}", json=payload, headers=headers)

                # Vérifier si la requête a réussi
                if response.status_code == 200:
                    response_data = response.json()
                    # Extraire le texte de la réponse
                    answer = response_data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "Aucune réponse reçue.")
                    # Ajouter la question et la réponse à l'historique
                    st.session_state.chat_history.append({"role": "user", "content": user_input})
                    st.session_state.chat_history.append({"role": "assistant", "content": answer})
                else:
                    st.error(f"Erreur de l'API Gemini : {response.status_code} - {response.text}")

            except Exception as e:
                st.error(f"Erreur lors de la génération de la réponse : {str(e)}")
        else:
            st.warning("Veuillez entrer une question.")

    # Afficher l'historique du chat
    st.subheader("📜 Historique du Chat")
    for message in st.session_state.chat_history:
        if message["role"] == "user":
            st.markdown(f"""
                <div class='news-card' style='border-left: 4px solid #FFD700;'>
                    <strong>Vous :</strong> {message['content']}
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div class='news-card' style='border-left: 4px solid #3498DB;'>
                    <strong>Gemini :</strong> {message['content']}
                </div>
            """, unsafe_allow_html=True)

    # Bouton pour réinitialiser l'historique
    if st.button("Effacer l'historique", key="clear_chat"):
        st.session_state.chat_history = []
        st.success("Historique du chat effacé.")

    # Footer
    st.markdown("---")
    st.markdown("""
        <div style='text-align: center; padding: 1rem;'>
            <p style='color: #FFD700;'>© 2024 Gold Analytics Pro • Plateforme Professionnelle d'Analyse de Marché</p>
            <p style='font-size: 0.8rem; color: #888;'>Powered by Gemini AI</p>
        </div>
    """, unsafe_allow_html=True)