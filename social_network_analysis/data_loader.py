"""
Data Loader pour l'analyse de réseau social
============================================
Charge les données depuis MongoDB pour la construction du graphe.
"""

import os
import pandas as pd
from pymongo import MongoClient
from typing import Optional

# Configuration MongoDB
MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
MONGO_DB = "news_db"


def get_mongo_client():
    """Crée et retourne une connexion MongoDB."""
    return MongoClient(MONGO_HOST, MONGO_PORT)


def load_articles_for_network(collection_name: str = "processed_articles") -> pd.DataFrame:
    """
    Charge les articles depuis MongoDB pour l'analyse de réseau.
    
    Args:
        collection_name: Nom de la collection MongoDB
        
    Returns:
        DataFrame avec les articles et leurs métadonnées
    """
    print(f"📡 Connexion à MongoDB ({MONGO_HOST}:{MONGO_PORT})...")
    
    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        collection = db[collection_name]
        
        data = list(collection.find())
        print(f"✅ {len(data)} articles chargés depuis MongoDB")
        
        if not data:
            print("⚠️ Aucune donnée trouvée, tentative avec le CSV de fallback...")
            return load_from_csv_fallback()
        
        df = pd.DataFrame(data)
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)
        
        client.close()
        return df
        
    except Exception as e:
        print(f"❌ Erreur MongoDB: {e}")
        print("📂 Tentative de chargement depuis CSV...")
        return load_from_csv_fallback()


def load_from_csv_fallback() -> pd.DataFrame:
    """Charge les données depuis le CSV en cas d'échec MongoDB."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_paths = [
        os.path.join(base_dir, "sentiment_analysis_model", "labeled_full.csv"),
        os.path.join(base_dir, "preprocessing", "processed_news.csv"),
        os.path.join(base_dir, "preprocessing", "ai_vs_human_news.csv"),
    ]
    
    for csv_path in csv_paths:
        if os.path.exists(csv_path):
            print(f"📂 Chargement depuis {csv_path}")
            df = pd.read_csv(csv_path)
            print(f"✅ {len(df)} articles chargés")
            return df
    
    print("❌ Aucun fichier de données trouvé")
    return pd.DataFrame()


def load_sentiment_data() -> pd.DataFrame:
    """Charge les données avec labels de sentiment."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(base_dir, "sentiment_analysis_model", "labeled_full.csv")
    
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        print(f"✅ {len(df)} articles avec sentiments chargés")
        return df
    
    return load_articles_for_network()


if __name__ == "__main__":
    print("=" * 50)
    print("Test du Data Loader pour Network Analysis")
    print("=" * 50)
    
    df = load_articles_for_network()
    if not df.empty:
        print(f"\n📊 Colonnes disponibles: {list(df.columns)}")
        print(f"📊 Nombre d'articles: {len(df)}")
        if 'source' in df.columns:
            print(f"📊 Sources uniques: {df['source'].nunique()}")
        if 'author' in df.columns:
            print(f"📊 Auteurs uniques: {df['author'].nunique()}")
