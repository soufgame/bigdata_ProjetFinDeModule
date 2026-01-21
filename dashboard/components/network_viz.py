"""
Network Visualization Component
================================
Crée des visualisations de graphe interactives avec Plotly.
"""

import plotly.graph_objects as go
import networkx as nx
from typing import Dict, Optional, List
import random


def generate_color_palette(n_colors: int) -> List[str]:
    """Génère une palette de couleurs distinctes."""
    colors = [
        '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7',
        '#DDA0DD', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9',
        '#F8B500', '#00CED1', '#FF69B4', '#32CD32', '#FFD700',
        '#8A2BE2', '#00FA9A', '#DC143C', '#00BFFF', '#FF4500'
    ]
    return colors[:n_colors] if n_colors <= len(colors) else colors * (n_colors // len(colors) + 1)


def create_network_figure(
    G: nx.Graph,
    partition: Optional[Dict[str, int]] = None,
    highlight_nodes: Optional[List[str]] = None,
    title: str = "Network Graph"
) -> go.Figure:
    """
    Crée une figure Plotly du graphe de réseau.
    
    Args:
        G: Graphe NetworkX
        partition: Dict optionnel avec les communautés
        highlight_nodes: Liste optionnelle de nœuds à mettre en évidence
        title: Titre du graphe
        
    Returns:
        go.Figure: Figure Plotly interactive
    """
    if G.number_of_nodes() == 0:
        fig = go.Figure()
        fig.add_annotation(text="No data available", showarrow=False, font=dict(size=20))
        return fig
    
    # Calculer le layout
    if G.number_of_nodes() <= 100:
        pos = nx.spring_layout(G, k=2, iterations=50, seed=42)
    else:
        pos = nx.spring_layout(G, k=1, iterations=30, seed=42)
    
    # Préparer les couleurs des communautés
    if partition:
        n_communities = len(set(partition.values()))
        colors = generate_color_palette(n_communities)
        node_colors = [colors[partition.get(node, 0)] for node in G.nodes()]
    else:
        node_colors = ['#4ECDC4'] * G.number_of_nodes()
    
    # Créer les arêtes
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
    
    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines',
        name='Connections'
    )
    
    # Créer les nœuds
    node_x = [pos[node][0] for node in G.nodes()]
    node_y = [pos[node][1] for node in G.nodes()]
    
    # Taille des nœuds basée sur le degré
    degrees = [G.degree(node) for node in G.nodes()]
    max_degree = max(degrees) if degrees else 1
    if max_degree == 0:
        max_degree = 1
    node_sizes = [10 + (d / max_degree) * 30 for d in degrees]
    
    # Texte au survol
    node_text = []
    for node in G.nodes():
        node_data = G.nodes[node]
        text = f"<b>{node}</b><br>"
        text += f"Degree: {G.degree(node)}<br>"
        if 'sentiment' in node_data:
            text += f"Sentiment: {node_data['sentiment']}<br>"
        if 'article_count' in node_data:
            text += f"Articles: {node_data['article_count']}"
        if partition and node in partition:
            text += f"<br>Community: {partition[node]}"
        node_text.append(text)
    
    # Bordure pour les nœuds mis en évidence
    node_line_widths = []
    node_line_colors = []
    for node in G.nodes():
        if highlight_nodes and node in highlight_nodes:
            node_line_widths.append(3)
            node_line_colors.append('#FF0000')
        else:
            node_line_widths.append(1)
            node_line_colors.append('#fff')
    
    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers',
        hoverinfo='text',
        text=node_text,
        marker=dict(
            showscale=False,
            color=node_colors,
            size=node_sizes,
            line=dict(width=node_line_widths, color=node_line_colors)
        ),
        name='Nodes'
    )
    
    # Créer la figure
    fig = go.Figure(
        data=[edge_trace, node_trace],
        layout=go.Layout(
            title=dict(text=title, font=dict(size=16)),
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20, l=5, r=5, t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
        )
    )
    
    return fig


def create_community_visualization(
    G: nx.Graph,
    partition: Dict[str, int],
    title: str = "Community Detection"
) -> go.Figure:
    """
    Crée une visualisation des communautés avec des couleurs distinctes.
    """
    return create_network_figure(G, partition=partition, title=title)


def create_centrality_network(
    G: nx.Graph,
    centrality_scores: Dict[str, float],
    title: str = "Centrality Visualization"
) -> go.Figure:
    """
    Crée une visualisation où la taille des nœuds reflète leur centralité.
    """
    if G.number_of_nodes() == 0:
        fig = go.Figure()
        fig.add_annotation(text="No data available", showarrow=False, font=dict(size=20))
        return fig
    
    pos = nx.spring_layout(G, k=2, iterations=50, seed=42)
    
    # Arêtes
    edge_x, edge_y = [], []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
    
    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines'
    )
    
    # Nœuds avec taille basée sur la centralité
    node_x = [pos[node][0] for node in G.nodes()]
    node_y = [pos[node][1] for node in G.nodes()]
    
    scores = [centrality_scores.get(node, 0) for node in G.nodes()]
    max_score = max(scores) if scores and max(scores) > 0 else 1
    node_sizes = [15 + (s / max_score) * 40 for s in scores]
    
    # Couleur basée sur le score
    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers',
        hoverinfo='text',
        text=[f"<b>{n}</b><br>Score: {centrality_scores.get(n, 0):.4f}" for n in G.nodes()],
        marker=dict(
            showscale=True,
            colorscale='Viridis',
            color=scores,
            size=node_sizes,
            colorbar=dict(title="Centrality"),
            line=dict(width=1, color='#fff')
        )
    )
    
    fig = go.Figure(
        data=[edge_trace, node_trace],
        layout=go.Layout(
            title=dict(text=title, font=dict(size=16)),
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20, l=5, r=5, t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
        )
    )
    
    return fig
