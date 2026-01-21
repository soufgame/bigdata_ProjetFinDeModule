"""
Community Detector - Détection de communautés
==============================================
Implémente des algorithmes de clustering pour détecter les communautés.
"""

import networkx as nx
import pandas as pd
from typing import Dict, List, Optional, Tuple
from collections import Counter

try:
    import community as community_louvain
    LOUVAIN_AVAILABLE = True
except ImportError:
    LOUVAIN_AVAILABLE = False
    print("⚠️ Module python-louvain non installé. Utilisation de l'algorithme alternatif.")

from .graph_builder import build_network_graph


def detect_communities_louvain(G: nx.Graph) -> Dict[str, int]:
    """
    Détecte les communautés avec l'algorithme de Louvain.
    
    Args:
        G: Graphe NetworkX
        
    Returns:
        Dict mappant chaque nœud à son ID de communauté
    """
    if not LOUVAIN_AVAILABLE:
        return detect_communities_label_propagation(G)
    
    print("🔍 Détection de communautés avec Louvain...")
    partition = community_louvain.best_partition(G)
    
    n_communities = len(set(partition.values()))
    print(f"✅ {n_communities} communautés détectées")
    
    return partition


def detect_communities_label_propagation(G: nx.Graph) -> Dict[str, int]:
    """
    Détecte les communautés avec Label Propagation.
    
    Args:
        G: Graphe NetworkX
        
    Returns:
        Dict mappant chaque nœud à son ID de communauté
    """
    print("🔍 Détection de communautés avec Label Propagation...")
    
    communities = nx.community.label_propagation_communities(G)
    
    partition = {}
    for idx, community in enumerate(communities):
        for node in community:
            partition[node] = idx
    
    n_communities = len(set(partition.values()))
    print(f"✅ {n_communities} communautés détectées")
    
    return partition


def detect_communities_greedy(G: nx.Graph) -> Dict[str, int]:
    """
    Détecte les communautés avec l'algorithme Greedy Modularity.
    
    Args:
        G: Graphe NetworkX
        
    Returns:
        Dict mappant chaque nœud à son ID de communauté
    """
    print("🔍 Détection de communautés avec Greedy Modularity...")
    
    communities = nx.community.greedy_modularity_communities(G)
    
    partition = {}
    for idx, community in enumerate(communities):
        for node in community:
            partition[node] = idx
    
    n_communities = len(set(partition.values()))
    print(f"✅ {n_communities} communautés détectées")
    
    return partition


def detect_communities(G: Optional[nx.Graph] = None, method: str = "louvain") -> Dict[str, int]:
    """
    Détecte les communautés avec la méthode spécifiée.
    
    Args:
        G: Graphe NetworkX (optionnel)
        method: Méthode de détection ("louvain", "label_propagation", "greedy")
        
    Returns:
        Dict mappant chaque nœud à son ID de communauté
    """
    if G is None:
        G = build_network_graph()
    
    if G.number_of_nodes() == 0:
        print("❌ Graphe vide, impossible de détecter les communautés")
        return {}
    
    methods = {
        "louvain": detect_communities_louvain,
        "label_propagation": detect_communities_label_propagation,
        "greedy": detect_communities_greedy,
    }
    
    detector = methods.get(method, detect_communities_louvain)
    return detector(G)


def calculate_modularity(G: nx.Graph, partition: Dict[str, int]) -> float:
    """
    Calcule le score de modularité de la partition.
    
    Args:
        G: Graphe NetworkX
        partition: Dict mappant chaque nœud à sa communauté
        
    Returns:
        Score de modularité (0-1)
    """
    if not partition:
        return 0.0
    
    # Convertir en liste de sets pour NetworkX
    communities_dict: Dict[int, set] = {}
    for node, comm_id in partition.items():
        if comm_id not in communities_dict:
            communities_dict[comm_id] = set()
        communities_dict[comm_id].add(node)
    
    communities = list(communities_dict.values())
    
    try:
        modularity = nx.community.modularity(G, communities)
        return modularity
    except:
        return 0.0


def get_community_statistics(G: nx.Graph, partition: Dict[str, int]) -> pd.DataFrame:
    """
    Calcule les statistiques par communauté.
    
    Args:
        G: Graphe NetworkX
        partition: Dict mappant chaque nœud à sa communauté
        
    Returns:
        DataFrame avec les statistiques par communauté
    """
    if not partition:
        return pd.DataFrame()
    
    # Grouper les nœuds par communauté
    communities: Dict[int, List[str]] = {}
    for node, comm_id in partition.items():
        if comm_id not in communities:
            communities[comm_id] = []
        communities[comm_id].append(node)
    
    stats = []
    for comm_id, nodes in communities.items():
        # Sous-graphe de la communauté
        subgraph = G.subgraph(nodes)
        
        # Collecter les sentiments
        sentiments = [G.nodes[n].get('sentiment', 'unknown') for n in nodes]
        dominant_sentiment = Counter(sentiments).most_common(1)[0][0] if sentiments else 'unknown'
        
        stats.append({
            'community_id': comm_id,
            'size': len(nodes),
            'internal_edges': subgraph.number_of_edges(),
            'density': nx.density(subgraph) if len(nodes) > 1 else 0,
            'dominant_sentiment': dominant_sentiment,
            'top_nodes': nodes[:5],  # Top 5 nœuds
        })
    
    df = pd.DataFrame(stats)
    df = df.sort_values('size', ascending=False).reset_index(drop=True)
    
    return df


def get_nodes_with_communities(G: nx.Graph, partition: Dict[str, int]) -> pd.DataFrame:
    """
    Retourne un DataFrame avec les nœuds et leur communauté.
    
    Args:
        G: Graphe NetworkX
        partition: Dict mappant chaque nœud à sa communauté
        
    Returns:
        DataFrame avec nœud, communauté, et métriques
    """
    if not partition:
        return pd.DataFrame()
    
    data = []
    for node, comm_id in partition.items():
        node_data = G.nodes.get(node, {})
        data.append({
            'node': node,
            'community': comm_id,
            'degree': G.degree(node),
            'article_count': node_data.get('article_count', 0),
            'sentiment': node_data.get('sentiment', 'unknown'),
        })
    
    df = pd.DataFrame(data)
    df = df.sort_values(['community', 'degree'], ascending=[True, False]).reset_index(drop=True)
    
    return df


if __name__ == "__main__":
    print("=" * 50)
    print("Détection de Communautés")
    print("=" * 50)
    
    # Construire le graphe
    G = build_network_graph()
    
    if G.number_of_nodes() > 0:
        # Détecter les communautés
        partition = detect_communities(G, method="louvain")
        
        # Calculer la modularité
        modularity = calculate_modularity(G, partition)
        print(f"\n📊 Score de modularité: {modularity:.4f}")
        
        # Statistiques des communautés
        print("\n📊 Statistiques par communauté:")
        df_stats = get_community_statistics(G, partition)
        print(df_stats.to_string(index=False))
        
        # Nœuds avec leur communauté
        print("\n📊 Top nœuds par communauté:")
        df_nodes = get_nodes_with_communities(G, partition)
        for comm_id in df_nodes['community'].unique()[:5]:
            comm_nodes = df_nodes[df_nodes['community'] == comm_id].head(3)
            print(f"\n   Communauté {comm_id}:")
            for _, row in comm_nodes.iterrows():
                print(f"      • {row['node']} (degré: {row['degree']})")
