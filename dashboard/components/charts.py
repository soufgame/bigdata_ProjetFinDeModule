"""
Charts Component
=================
Crée des graphiques Plotly pour le dashboard.
"""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from typing import Dict, List, Optional


def create_sentiment_pie_chart(sentiment_counts: Dict[str, int]) -> go.Figure:
    """
    Crée un graphique en camembert pour la distribution des sentiments.
    
    Args:
        sentiment_counts: Dict avec les comptes par sentiment
        
    Returns:
        go.Figure: Graphique Plotly
    """
    colors = {
        'neutral': '#95A5A6',
        'agreed': '#27AE60',
        'against': '#E74C3C',
        'positive': '#2ECC71',
        'negative': '#C0392B',
        'unknown': '#BDC3C7'
    }
    
    labels = list(sentiment_counts.keys())
    values = list(sentiment_counts.values())
    chart_colors = [colors.get(label, '#3498DB') for label in labels]
    
    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=values,
        hole=0.4,
        marker_colors=chart_colors,
        textinfo='label+percent',
        textposition='outside'
    )])
    
    fig.update_layout(
        title="Distribution des Sentiments",
        showlegend=True,
        margin=dict(t=50, b=20, l=20, r=20),
        paper_bgcolor='rgba(0,0,0,0)',
    )
    
    return fig


def create_centrality_bar_chart(df: pd.DataFrame, metric: str = 'pagerank', top_n: int = 10) -> go.Figure:
    """
    Crée un graphique à barres horizontales pour les métriques de centralité.
    
    Args:
        df: DataFrame avec les métriques
        metric: Nom de la métrique à afficher
        top_n: Nombre d'éléments à afficher
        
    Returns:
        go.Figure: Graphique Plotly
    """
    if df.empty or metric not in df.columns:
        fig = go.Figure()
        fig.add_annotation(text="No data available", showarrow=False, font=dict(size=16))
        return fig
    
    top_df = df.nlargest(top_n, metric)
    
    colors = {
        'neutral': '#95A5A6',
        'agreed': '#27AE60',
        'against': '#E74C3C',
    }
    bar_colors = [colors.get(str(s), '#3498DB') for s in top_df.get('sentiment', ['unknown'] * len(top_df))]
    
    fig = go.Figure(go.Bar(
        x=top_df[metric],
        y=top_df['node'],
        orientation='h',
        marker_color=bar_colors,
        text=[f"{v:.4f}" for v in top_df[metric]],
        textposition='outside'
    ))
    
    metric_labels = {
        'pagerank': 'PageRank',
        'degree_centrality': 'Degree Centrality',
        'betweenness_centrality': 'Betweenness Centrality',
        'closeness_centrality': 'Closeness Centrality',
        'eigenvector_centrality': 'Eigenvector Centrality'
    }
    
    fig.update_layout(
        title=f"Top {top_n} - {metric_labels.get(metric, metric)}",
        xaxis_title=metric_labels.get(metric, metric),
        yaxis_title="",
        yaxis=dict(autorange="reversed"),
        margin=dict(t=50, b=50, l=150, r=50),
        paper_bgcolor='rgba(0,0,0,0)',
    )
    
    return fig


def create_community_bar_chart(community_stats: pd.DataFrame) -> go.Figure:
    """
    Crée un graphique à barres pour les tailles des communautés.
    
    Args:
        community_stats: DataFrame avec les statistiques des communautés
        
    Returns:
        go.Figure: Graphique Plotly
    """
    if community_stats.empty:
        fig = go.Figure()
        fig.add_annotation(text="No communities detected", showarrow=False, font=dict(size=16))
        return fig
    
    fig = go.Figure(go.Bar(
        x=[f"C{c}" for c in community_stats['community_id']],
        y=community_stats['size'],
        marker_color=px.colors.qualitative.Set3[:len(community_stats)],
        text=community_stats['size'],
        textposition='outside'
    ))
    
    fig.update_layout(
        title="Taille des Communautés",
        xaxis_title="Communauté",
        yaxis_title="Nombre de nœuds",
        margin=dict(t=50, b=50, l=50, r=20),
        paper_bgcolor='rgba(0,0,0,0)',
    )
    
    return fig


def create_degree_distribution(G) -> go.Figure:
    """
    Crée un histogramme de la distribution des degrés.
    
    Args:
        G: Graphe NetworkX
        
    Returns:
        go.Figure: Graphique Plotly
    """
    if G.number_of_nodes() == 0:
        fig = go.Figure()
        fig.add_annotation(text="No data available", showarrow=False, font=dict(size=16))
        return fig
    
    degrees = [d for n, d in G.degree()]
    
    fig = go.Figure(go.Histogram(
        x=degrees,
        nbinsx=20,
        marker_color='#3498DB',
    ))
    
    fig.update_layout(
        title="Distribution des Degrés",
        xaxis_title="Degré",
        yaxis_title="Fréquence",
        margin=dict(t=50, b=50, l=50, r=20),
        paper_bgcolor='rgba(0,0,0,0)',
    )
    
    return fig


def create_metrics_comparison(df: pd.DataFrame, nodes: List[str]) -> go.Figure:
    """
    Crée un graphique radar pour comparer les métriques de plusieurs nœuds.
    
    Args:
        df: DataFrame avec les métriques
        nodes: Liste des nœuds à comparer
        
    Returns:
        go.Figure: Graphique Plotly
    """
    if df.empty or not nodes:
        fig = go.Figure()
        fig.add_annotation(text="No data available", showarrow=False, font=dict(size=16))
        return fig
    
    metrics = ['degree_centrality', 'betweenness_centrality', 'closeness_centrality', 'pagerank']
    available_metrics = [m for m in metrics if m in df.columns]
    
    if not available_metrics:
        fig = go.Figure()
        fig.add_annotation(text="No metrics available", showarrow=False, font=dict(size=16))
        return fig
    
    fig = go.Figure()
    
    colors = px.colors.qualitative.Set1
    
    for i, node in enumerate(nodes[:5]):  # Max 5 nœuds
        node_data = df[df['node'] == node]
        if node_data.empty:
            continue
            
        values = [node_data[m].values[0] for m in available_metrics]
        # Normaliser les valeurs
        max_vals = [df[m].max() for m in available_metrics]
        normalized = [v / mv if mv > 0 else 0 for v, mv in zip(values, max_vals)]
        
        fig.add_trace(go.Scatterpolar(
            r=normalized + [normalized[0]],  # Fermer le polygone
            theta=available_metrics + [available_metrics[0]],
            fill='toself',
            name=node,
            line_color=colors[i % len(colors)],
            opacity=0.7
        ))
    
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
        title="Comparaison des Métriques",
        margin=dict(t=80, b=50, l=50, r=50),
        paper_bgcolor='rgba(0,0,0,0)',
    )
    
    return fig
