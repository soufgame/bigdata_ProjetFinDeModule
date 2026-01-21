"""
Centrality Analyzer - Calcul des métriques de centralité
=========================================================
Calcule différentes métriques de centralité pour le graphe de réseau.
"""

import networkx as nx
import pandas as pd
from typing import Dict, Optional

from .graph_builder import build_network_graph


def calculate_centrality_metrics(G: Optional[nx.Graph] = None) -> pd.DataFrame:
    """
    Calcule toutes les métriques de centralité pour le graphe.
    
    Args:
        G: Graphe NetworkX (optionnel, sinon construit depuis les données)
        
    Returns:
        DataFrame avec les métriques de centralité par nœud
    """
    if G is None:
        G = build_network_graph()
    
    if G.number_of_nodes() == 0:
        print("❌ Graphe vide, impossible de calculer les centralités")
        return pd.DataFrame()
    
    print(f"📊 Calcul des métriques de centralité pour {G.number_of_nodes()} nœuds...")
    
    metrics = {}
    
    # Degree Centrality - Nombre de connexions normalisé
    print("   • Calcul de la centralité de degré...")
    degree_centrality = nx.degree_centrality(G)
    
    # Betweenness Centrality - Importance comme pont entre groupes
    print("   • Calcul de la centralité d'intermédiarité...")
    betweenness_centrality = nx.betweenness_centrality(G)
    
    # Closeness Centrality - Proximité moyenne aux autres nœuds
    print("   • Calcul de la centralité de proximité...")
    closeness_centrality = nx.closeness_centrality(G)
    
    # PageRank - Importance basée sur les liens
    print("   • Calcul du PageRank...")
    try:
        pagerank = nx.pagerank(G, max_iter=100)
    except:
        pagerank = {node: 0 for node in G.nodes()}
    
    # Eigenvector Centrality - Influence des voisins
    print("   • Calcul de la centralité eigenvector...")
    try:
        eigenvector_centrality = nx.eigenvector_centrality(G, max_iter=100)
    except:
        eigenvector_centrality = {node: 0 for node in G.nodes()}
    
    # Construire le DataFrame
    for node in G.nodes():
        node_data = G.nodes[node]
        metrics[node] = {
            'node': node,
            'degree': G.degree(node),
            'degree_centrality': degree_centrality.get(node, 0),
            'betweenness_centrality': betweenness_centrality.get(node, 0),
            'closeness_centrality': closeness_centrality.get(node, 0),
            'pagerank': pagerank.get(node, 0),
            'eigenvector_centrality': eigenvector_centrality.get(node, 0),
            'article_count': node_data.get('article_count', 0),
            'sentiment': node_data.get('sentiment', 'unknown'),
        }
    
    df = pd.DataFrame.from_dict(metrics, orient='index')
    df = df.sort_values('pagerank', ascending=False).reset_index(drop=True)
    
    print(f"✅ Métriques calculées pour {len(df)} nœuds")
    
    return df


def get_top_influencers(df: pd.DataFrame, metric: str = 'pagerank', top_n: int = 10) -> pd.DataFrame:
    """
    Retourne les top influenceurs selon une métrique.
    
    Args:
        df: DataFrame avec les métriques de centralité
        metric: Métrique à utiliser pour le classement
        top_n: Nombre de top influenceurs à retourner
        
    Returns:
        DataFrame avec les top influenceurs
    """
    if df.empty:
        return pd.DataFrame()
    
    return df.nlargest(top_n, metric)[['node', 'degree', metric, 'sentiment', 'article_count']]


def get_centrality_statistics(df: pd.DataFrame) -> Dict:
    """
    Calcule les statistiques descriptives des métriques.
    
    Args:
        df: DataFrame avec les métriques de centralité
        
    Returns:
        Dict avec les statistiques
    """
    if df.empty:
        return {}
    
    metrics = ['degree_centrality', 'betweenness_centrality', 'closeness_centrality', 'pagerank']
    stats = {}
    
    for metric in metrics:
        if metric in df.columns:
            stats[metric] = {
                'mean': df[metric].mean(),
                'std': df[metric].std(),
                'min': df[metric].min(),
                'max': df[metric].max(),
                'median': df[metric].median(),
            }
    
    return stats


def identify_key_bridge_nodes(G: nx.Graph, top_n: int = 5) -> pd.DataFrame:
    """
    Identifie les nœuds qui servent de ponts entre communautés.
    
    Args:
        G: Graphe NetworkX
        top_n: Nombre de nœuds à retourner
        
    Returns:
        DataFrame avec les nœuds ponts
    """
    if G.number_of_nodes() == 0:
        return pd.DataFrame()
    
    betweenness = nx.betweenness_centrality(G)
    sorted_nodes = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)[:top_n]
    
    data = []
    for node, score in sorted_nodes:
        data.append({
            'node': node,
            'betweenness_score': score,
            'degree': G.degree(node),
            'neighbors': list(G.neighbors(node))[:5]  # Top 5 voisins
        })
    
    return pd.DataFrame(data)


if __name__ == "__main__":
    print("=" * 50)
    print("Analyse de Centralité du Réseau")
    print("=" * 50)
    
    # Construire le graphe
    G = build_network_graph()
    
    if G.number_of_nodes() > 0:
        # Calculer les métriques
        df_metrics = calculate_centrality_metrics(G)
        
        print("\n🔝 Top 10 Influenceurs (PageRank):")
        print(get_top_influencers(df_metrics, 'pagerank', 10).to_string(index=False))
        
        print("\n🌉 Nœuds Ponts (Betweenness):")
        print(identify_key_bridge_nodes(G, 5).to_string(index=False))
        
        print("\n📊 Statistiques:")
        stats = get_centrality_statistics(df_metrics)
        for metric, values in stats.items():
            print(f"\n   {metric}:")
            print(f"      Moyenne: {values['mean']:.4f}")
            print(f"      Max: {values['max']:.4f}")
