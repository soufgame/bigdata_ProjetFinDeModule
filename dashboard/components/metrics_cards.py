"""
Metrics Cards Component
========================
Crée des cartes de métriques KPI pour le dashboard.
"""

import streamlit as st
from typing import Dict, Optional


def render_metric_card(
    label: str,
    value: str,
    delta: Optional[str] = None,
    delta_color: str = "normal"
):
    """
    Affiche une carte de métrique dans Streamlit.
    
    Args:
        label: Libellé de la métrique
        value: Valeur principale
        delta: Changement (optionnel)
        delta_color: Couleur du delta ("normal", "inverse", "off")
    """
    st.metric(label=label, value=value, delta=delta, delta_color=delta_color)


def render_kpi_row(metrics: Dict[str, str]):
    """
    Affiche une rangée de KPIs.
    
    Args:
        metrics: Dict avec {label: value}
    """
    cols = st.columns(len(metrics))
    for col, (label, value) in zip(cols, metrics.items()):
        with col:
            st.metric(label=label, value=value)


def render_network_stats(stats: Dict):
    """
    Affiche les statistiques du réseau.
    
    Args:
        stats: Dict avec les statistiques
    """
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Nœuds", stats.get('nodes', 0))
    with col2:
        st.metric("🔗 Arêtes", stats.get('edges', 0))
    with col3:
        st.metric("📈 Densité", f"{stats.get('density', 0):.4f}")
    with col4:
        st.metric("🌐 Composantes", stats.get('components', 0))


def render_centrality_summary(top_node: str, top_score: float, metric_name: str):
    """
    Affiche un résumé de centralité.
    
    Args:
        top_node: Nœud le plus central
        top_score: Score de centralité
        metric_name: Nom de la métrique
    """
    st.markdown(f"""
    <div style="padding: 15px; border-radius: 10px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white;">
        <h4 style="margin: 0; color: white;">🏆 Top {metric_name}</h4>
        <h2 style="margin: 5px 0; color: white;">{top_node}</h2>
        <p style="margin: 0; opacity: 0.9;">Score: {top_score:.4f}</p>
    </div>
    """, unsafe_allow_html=True)


def render_community_summary(n_communities: int, modularity: float):
    """
    Affiche un résumé des communautés.
    
    Args:
        n_communities: Nombre de communautés
        modularity: Score de modularité
    """
    quality = "Excellente" if modularity > 0.5 else "Bonne" if modularity > 0.3 else "Modérée"
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("🏘️ Communautés", n_communities)
    with col2:
        st.metric("📊 Modularité", f"{modularity:.4f}", quality)


def render_sentiment_summary(sentiment_counts: Dict[str, int]):
    """
    Affiche un résumé des sentiments.
    
    Args:
        sentiment_counts: Dict avec les comptes par sentiment
    """
    total = sum(sentiment_counts.values())
    
    cols = st.columns(len(sentiment_counts))
    
    icons = {
        'neutral': '😐',
        'agreed': '👍',
        'against': '👎',
        'positive': '😊',
        'negative': '😞',
        'unknown': '❓'
    }
    
    for col, (sentiment, count) in zip(cols, sentiment_counts.items()):
        with col:
            pct = (count / total * 100) if total > 0 else 0
            icon = icons.get(sentiment, '📊')
            st.metric(f"{icon} {sentiment.capitalize()}", f"{count}", f"{pct:.1f}%")
