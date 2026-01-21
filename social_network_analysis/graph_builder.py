"""
Graph Builder - Construction du graphe de réseau social
========================================================
Construit un graphe NetworkX à partir des données d'articles.

Le graphe est construit avec:
- Nœuds: Sources/Auteurs des articles
- Arêtes: Relations basées sur les mots-clés partagés et la proximité temporelle
"""

import networkx as nx
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import ast

from .data_loader import load_articles_for_network, load_sentiment_data


def parse_keywords(keywords_str: str) -> List[str]:
    """Parse les mots-clés depuis une chaîne."""
    if pd.isna(keywords_str) or not keywords_str:
        return []
    
    # Si c'est une liste de tokens sous forme de string
    if keywords_str.startswith('['):
        try:
            return ast.literal_eval(keywords_str)
        except:
            pass
    
    # Sinon, séparer par virgule
    return [k.strip().lower() for k in str(keywords_str).split(',') if k.strip()]


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse une date depuis différents formats."""
    if pd.isna(date_str) or not date_str:
        return None
    
    formats = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str), fmt)
        except ValueError:
            continue
    return None


def get_node_identifier(row: pd.Series) -> str:
    """Extrait l'identifiant de nœud (auteur prioritaire sur source)."""
    # Essayer d'abord avec 'author'
    try:
        if 'author' in row.index:
            author_val = row['author']
            if pd.notna(author_val) and str(author_val).strip():
                author = str(author_val).strip()
                # Nettoyer les auteurs multiples (prendre le premier)
                if ',' in author:
                    author = author.split(',')[0].strip()
                if author and author.lower() not in ['', 'none', 'null', 'unknown', 'nan']:
                    return author
    except:
        pass
    
    # Fallback: utiliser la source si ce n'est pas "newsapi" générique
    try:
        if 'source' in row.index:
            source_val = row['source']
            if pd.notna(source_val):
                source = str(source_val).strip()
                if source and source.lower() not in ['newsapi', '', 'none', 'null', 'nan']:
                    return source
    except:
        pass
    
    # Dernier fallback: extraire du titre
    try:
        if 'title' in row.index:
            title_val = row['title']
            if pd.notna(title_val) and str(title_val).strip():
                title = str(title_val).strip()[:30]
                return f"Article: {title}..."
    except:
        pass
    
    return "Unknown"


def build_network_graph(df: Optional[pd.DataFrame] = None) -> nx.Graph:
    """
    Construit le graphe de réseau social à partir des articles.
    
    Args:
        df: DataFrame avec les articles (optionnel, sinon charge depuis MongoDB)
        
    Returns:
        nx.Graph: Graphe NetworkX avec nœuds et arêtes
    """
    if df is None:
        df = load_sentiment_data()
    
    if df.empty:
        print("❌ Aucune donnée disponible pour construire le graphe")
        return nx.Graph()
    
    print(f"🔨 Construction du graphe à partir de {len(df)} articles...")
    
    G = nx.Graph()
    
    # Index pour trouver les relations
    keyword_to_nodes: Dict[str, List[str]] = defaultdict(list)
    date_to_nodes: Dict[str, List[str]] = defaultdict(list)
    node_articles: Dict[str, List[int]] = defaultdict(list)
    node_sentiments: Dict[str, List[str]] = defaultdict(list)
    
    # Première passe: collecter les informations des nœuds
    for idx, row in df.iterrows():
        node_id = get_node_identifier(row)
        if node_id == "Unknown":
            continue
            
        node_articles[node_id].append(idx)
        
        # Collecter les mots-clés
        keywords_col = 'keywords' if 'keywords' in df.columns else 'processed_tokens'
        if keywords_col in df.columns:
            keywords = parse_keywords(row.get(keywords_col, ''))
            for kw in keywords:
                keyword_to_nodes[kw].append(node_id)
        
        # Collecter les dates
        date_col = 'published_at' if 'published_at' in df.columns else 'ingested_at'
        if date_col in df.columns:
            date = parse_date(row.get(date_col, ''))
            if date:
                date_key = date.strftime("%Y-%m-%d")
                date_to_nodes[date_key].append(node_id)
        
        # Collecter les sentiments
        if 'label' in df.columns and pd.notna(row.get('label')):
            node_sentiments[node_id].append(str(row['label']))
    
    # Ajouter les nœuds avec leurs attributs
    for node_id, article_indices in node_articles.items():
        # Calculer le sentiment dominant
        sentiments = node_sentiments.get(node_id, [])
        dominant_sentiment = max(set(sentiments), key=sentiments.count) if sentiments else "unknown"
        
        G.add_node(
            node_id,
            article_count=len(article_indices),
            sentiment=dominant_sentiment,
            type="source"
        )
    
    print(f"📊 {G.number_of_nodes()} nœuds créés")
    
    # Créer les arêtes basées sur les mots-clés partagés
    edge_weights: Dict[Tuple[str, str], int] = defaultdict(int)
    
    for keyword, nodes in keyword_to_nodes.items():
        unique_nodes = list(set(nodes))
        for i in range(len(unique_nodes)):
            for j in range(i + 1, len(unique_nodes)):
                edge = tuple(sorted([unique_nodes[i], unique_nodes[j]]))
                edge_weights[edge] += 1
    
    # Créer les arêtes basées sur la proximité temporelle
    for date_key, nodes in date_to_nodes.items():
        unique_nodes = list(set(nodes))
        for i in range(len(unique_nodes)):
            for j in range(i + 1, len(unique_nodes)):
                edge = tuple(sorted([unique_nodes[i], unique_nodes[j]]))
                edge_weights[edge] += 1
    
    # Ajouter les arêtes au graphe
    for (node1, node2), weight in edge_weights.items():
        if node1 in G.nodes and node2 in G.nodes:
            G.add_edge(node1, node2, weight=weight)
    
    print(f"📊 {G.number_of_edges()} arêtes créées")
    
    # Statistiques
    if G.number_of_nodes() > 0:
        density = nx.density(G)
        components = nx.number_connected_components(G)
        print(f"📊 Densité du graphe: {density:.4f}")
        print(f"📊 Composantes connexes: {components}")
    
    return G


def save_graph(G: nx.Graph, filepath: str):
    """Sauvegarde le graphe au format GraphML."""
    nx.write_graphml(G, filepath)
    print(f"💾 Graphe sauvegardé: {filepath}")


def load_graph(filepath: str) -> nx.Graph:
    """Charge un graphe depuis un fichier GraphML."""
    return nx.read_graphml(filepath)


if __name__ == "__main__":
    print("=" * 50)
    print("Construction du graphe de réseau social")
    print("=" * 50)
    
    G = build_network_graph()
    
    if G.number_of_nodes() > 0:
        print("\n🔝 Top 10 nœuds par degré:")
        degrees = sorted(G.degree(), key=lambda x: x[1], reverse=True)[:10]
        for node, degree in degrees:
            print(f"   {node}: {degree} connexions")
