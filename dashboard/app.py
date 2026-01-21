"""
Social Network Analysis Dashboard
==================================
Dashboard interactif Streamlit pour l'analyse de réseau social.

Pour lancer: streamlit run app.py
"""

import streamlit as st
import sys
import os

# Ajouter le répertoire parent au path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from collections import Counter

# Imports des modules d'analyse
try:
    from social_network_analysis.graph_builder import build_network_graph
    from social_network_analysis.centrality_analyzer import (
        calculate_centrality_metrics, 
        get_top_influencers,
        identify_key_bridge_nodes
    )
    from social_network_analysis.community_detector import (
        detect_communities,
        calculate_modularity,
        get_community_statistics,
        get_nodes_with_communities
    )
    from social_network_analysis.data_loader import load_sentiment_data
except ImportError as e:
    st.error(f"Erreur d'import: {e}")
    st.info("Assurez-vous d'être dans le bon répertoire")

# Imports des composants
from components.network_viz import (
    create_network_figure,
    create_community_visualization,
    create_centrality_network
)
from components.charts import (
    create_sentiment_pie_chart,
    create_centrality_bar_chart,
    create_community_bar_chart,
    create_degree_distribution
)
from components.metrics_cards import (
    render_network_stats,
    render_centrality_summary,
    render_community_summary,
    render_sentiment_summary
)


# Configuration de la page
st.set_page_config(
    page_title="Social Network Analysis Dashboard",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 1rem 0;
    }
    .stMetric {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        padding: 15px;
        border-radius: 10px;
    }
    .css-1d391kg {
        padding: 1rem;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=300)
def load_data():
    """Charge et met en cache les données."""
    df = load_sentiment_data()
    return df


@st.cache_resource(ttl=300)
def build_graph(_df):
    """Construit et met en cache le graphe."""
    return build_network_graph(_df)


@st.cache_data(ttl=300)
def compute_centrality(_G):
    """Calcule et met en cache les métriques de centralité."""
    return calculate_centrality_metrics(_G)


@st.cache_data(ttl=300)
def compute_communities(_G, method):
    """Détecte et met en cache les communautés."""
    return detect_communities(_G, method=method)


def main():
    # Sidebar
    st.sidebar.markdown("# 🌐 Navigation")
    
    page = st.sidebar.radio(
        "Choisir une page",
        ["🏠 Accueil", "📊 Réseau", "🎯 Centralité", "🏘️ Communautés", "😊 Sentiments"]
    )
    
    st.sidebar.markdown("---")
    
    # Options de rafraîchissement
    if st.sidebar.button("🔄 Rafraîchir les données"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
    
    # Auto-refresh
    auto_refresh = st.sidebar.checkbox("Auto-refresh (5 min)", value=False)
    if auto_refresh:
        st.sidebar.info("Les données seront rafraîchies automatiquement")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ⚙️ Paramètres")
    
    # Charger les données
    with st.spinner("Chargement des données..."):
        df = load_data()
    
    if df.empty:
        st.error("❌ Aucune donnée disponible. Vérifiez la connexion MongoDB ou les fichiers CSV.")
        return
    
    # Construire le graphe
    with st.spinner("Construction du graphe..."):
        G = build_graph(df)
    
    # Router vers les pages
    if page == "🏠 Accueil":
        render_home_page(G, df)
    elif page == "📊 Réseau":
        render_network_page(G, df)
    elif page == "🎯 Centralité":
        render_centrality_page(G)
    elif page == "🏘️ Communautés":
        render_community_page(G)
    elif page == "😊 Sentiments":
        render_sentiment_page(df, G)


def render_home_page(G, df):
    """Page d'accueil avec résumé."""
    st.markdown('<h1 class="main-header">🌐 Social Network Analysis Dashboard</h1>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Statistiques du réseau
    st.subheader("📊 Vue d'ensemble du réseau")
    
    import networkx as nx
    stats = {
        'nodes': G.number_of_nodes(),
        'edges': G.number_of_edges(),
        'density': nx.density(G) if G.number_of_nodes() > 0 else 0,
        'components': nx.number_connected_components(G) if G.number_of_nodes() > 0 else 0
    }
    render_network_stats(stats)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔗 Aperçu du réseau")
        fig = create_network_figure(G, title="Réseau complet")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📈 Distribution des degrés")
        fig = create_degree_distribution(G)
        st.plotly_chart(fig, use_container_width=True)
    
    # Résumé des sentiments
    if 'label' in df.columns:
        st.markdown("---")
        st.subheader("😊 Résumé des sentiments")
        sentiment_counts = df['label'].value_counts().to_dict()
        render_sentiment_summary(sentiment_counts)


def render_network_page(G, df):
    """Page de visualisation du réseau."""
    st.markdown("## 📊 Visualisation du Réseau")
    
    # Options
    col1, col2 = st.columns([3, 1])
    
    with col2:
        show_communities = st.checkbox("Afficher les communautés", value=True)
        community_method = st.selectbox(
            "Méthode de détection",
            ["louvain", "label_propagation", "greedy"],
            disabled=not show_communities
        )
    
    # Construire la visualisation
    if show_communities:
        partition = compute_communities(G, community_method)
        fig = create_community_visualization(G, partition, title="Réseau avec communautés")
    else:
        fig = create_network_figure(G, title="Réseau social")
    
    st.plotly_chart(fig, use_container_width=True, height=600)
    
    # Statistiques
    st.markdown("---")
    st.subheader("📊 Statistiques du réseau")
    
    import networkx as nx
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Nœuds", G.number_of_nodes())
    with col2:
        st.metric("Arêtes", G.number_of_edges())
    with col3:
        st.metric("Degré moyen", f"{sum(d for n, d in G.degree()) / G.number_of_nodes():.2f}" if G.number_of_nodes() > 0 else "0")
    with col4:
        st.metric("Densité", f"{nx.density(G):.4f}")


def render_centrality_page(G):
    """Page d'analyse de centralité."""
    st.markdown("## 🎯 Analyse de Centralité")
    
    # Calculer les métriques
    with st.spinner("Calcul des métriques..."):
        df_metrics = compute_centrality(G)
    
    if df_metrics.empty:
        st.warning("Aucune donnée de centralité disponible")
        return
    
    # Sélection de la métrique
    metric = st.selectbox(
        "Métrique de centralité",
        ["pagerank", "degree_centrality", "betweenness_centrality", "closeness_centrality"]
    )
    
    # Afficher le top influenceur
    top = df_metrics.nlargest(1, metric).iloc[0]
    render_centrality_summary(top['node'], top[metric], metric)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader(f"🏆 Top 10 - {metric}")
        fig = create_centrality_bar_chart(df_metrics, metric=metric, top_n=10)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🌐 Visualisation")
        scores = df_metrics.set_index('node')[metric].to_dict()
        fig = create_centrality_network(G, scores, title=f"Réseau coloré par {metric}")
        st.plotly_chart(fig, use_container_width=True)
    
    # Tableau des métriques
    st.markdown("---")
    st.subheader("📋 Tableau des métriques")
    st.dataframe(
        df_metrics[['node', 'degree', 'pagerank', 'betweenness_centrality', 'closeness_centrality', 'sentiment']].head(20),
        use_container_width=True
    )


def render_community_page(G):
    """Page de détection de communautés."""
    st.markdown("## 🏘️ Détection de Communautés")
    
    # Sélection de la méthode
    method = st.selectbox(
        "Algorithme de détection",
        ["louvain", "label_propagation", "greedy"]
    )
    
    # Détecter les communautés
    with st.spinner("Détection des communautés..."):
        partition = compute_communities(G, method)
    
    if not partition:
        st.warning("Impossible de détecter les communautés")
        return
    
    # Calculs
    modularity = calculate_modularity(G, partition)
    community_stats = get_community_statistics(G, partition)
    n_communities = len(set(partition.values()))
    
    # Résumé
    render_community_summary(n_communities, modularity)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🌐 Visualisation des communautés")
        fig = create_community_visualization(G, partition)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📊 Taille des communautés")
        fig = create_community_bar_chart(community_stats)
        st.plotly_chart(fig, use_container_width=True)
    
    # Détails des communautés
    st.markdown("---")
    st.subheader("📋 Détails des communautés")
    
    display_stats = community_stats[['community_id', 'size', 'density', 'dominant_sentiment']].copy()
    display_stats.columns = ['ID', 'Taille', 'Densité', 'Sentiment dominant']
    st.dataframe(display_stats, use_container_width=True)


def render_sentiment_page(df, G):
    """Page d'analyse des sentiments."""
    st.markdown("## 😊 Analyse des Sentiments")
    
    if 'label' not in df.columns:
        st.warning("Pas de données de sentiment disponibles")
        return
    
    # Distribution des sentiments
    sentiment_counts = df['label'].value_counts().to_dict()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Distribution")
        fig = create_sentiment_pie_chart(sentiment_counts)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📈 Comptage")
        render_sentiment_summary(sentiment_counts)
        
        st.markdown("---")
        
        # Statistiques par sentiment
        for sentiment, count in sentiment_counts.items():
            pct = count / len(df) * 100
            st.progress(pct / 100, text=f"{sentiment}: {count} ({pct:.1f}%)")
    
    # Réseau par sentiment
    st.markdown("---")
    st.subheader("🌐 Réseau coloré par sentiment dominant")
    
    # Créer une partition basée sur le sentiment des nœuds
    sentiment_partition = {}
    sentiment_map = {'neutral': 0, 'agreed': 1, 'against': 2}
    
    for node in G.nodes():
        node_sentiment = G.nodes[node].get('sentiment', 'unknown')
        sentiment_partition[node] = sentiment_map.get(node_sentiment, 3)
    
    fig = create_network_figure(G, partition=sentiment_partition, title="Réseau par sentiment")
    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
