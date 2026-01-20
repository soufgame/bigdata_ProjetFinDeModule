"""
MongoDB Data Loader pour Sentiment Analysis
============================================
Ce module permet de charger les données depuis MongoDB 
pour l'entraînement et l'évaluation du modèle de sentiment.

Usage:
    from data_loader import load_training_data, load_raw_articles
    
    # Charger les données traitées
    df = load_training_data()
    
    # Charger les articles bruts
    df_raw = load_raw_articles()
"""

import os
import pandas as pd
from pymongo import MongoClient

# Configuration MongoDB
MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
MONGO_DB = "news_db"


def get_mongo_client():
    """Crée et retourne une connexion MongoDB."""
    return MongoClient(MONGO_HOST, MONGO_PORT)


def load_raw_articles():
    """
    Charge les articles bruts depuis MongoDB.
    
    Returns:
        pd.DataFrame: DataFrame contenant les articles bruts
    """
    print(f"📡 Connexion à MongoDB ({MONGO_HOST}:{MONGO_PORT})...")
    
    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        collection = db["raw_articles"]
        
        data = list(collection.find())
        print(f"✅ {len(data)} articles bruts chargés depuis MongoDB")
        
        if not data:
            print("⚠️ Aucune donnée trouvée dans raw_articles")
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)
        
        client.close()
        return df
        
    except Exception as e:
        print(f"❌ Erreur de connexion MongoDB: {e}")
        return pd.DataFrame()


def load_training_data():
    """
    Charge les données prétraitées (processed_articles) depuis MongoDB.
    Ces données sont prêtes pour l'entraînement ML.
    
    Returns:
        pd.DataFrame: DataFrame contenant les articles prétraités
    """
    print(f"📡 Connexion à MongoDB ({MONGO_HOST}:{MONGO_PORT})...")
    
    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        collection = db["processed_articles"]
        
        data = list(collection.find())
        print(f"✅ {len(data)} articles prétraités chargés depuis MongoDB")
        
        if not data:
            print("⚠️ Aucune donnée trouvée dans processed_articles")
            print("💡 Astuce: Exécutez d'abord le pipeline de prétraitement")
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)
        
        client.close()
        return df
        
    except Exception as e:
        print(f"❌ Erreur de connexion MongoDB: {e}")
        return pd.DataFrame()


def save_predictions_to_mongodb(df, collection_name="predictions"):
    """
    Sauvegarde les prédictions du modèle dans MongoDB.
    
    Args:
        df: DataFrame avec les prédictions
        collection_name: Nom de la collection de destination
    """
    print(f"💾 Sauvegarde des prédictions dans MongoDB ({collection_name})...")
    
    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        collection = db[collection_name]
        
        records = df.to_dict('records')
        collection.delete_many({})
        if records:
            collection.insert_many(records)
        
        print(f"✅ {len(records)} prédictions sauvegardées")
        client.close()
        
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde: {e}")


def check_mongodb_status():
    """Vérifie l'état de la connexion MongoDB et affiche les statistiques."""
    print(f"🔍 Vérification de MongoDB ({MONGO_HOST}:{MONGO_PORT})...")
    
    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        
        collections = db.list_collection_names()
        print(f"✅ Connexion réussie à la base '{MONGO_DB}'")
        print(f"📊 Collections disponibles: {collections}")
        
        for coll_name in collections:
            count = db[coll_name].count_documents({})
            print(f"   - {coll_name}: {count} documents")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ Erreur de connexion: {e}")
        return False


if __name__ == "__main__":
    # Test de connexion
    print("=" * 50)
    print("Test du Data Loader MongoDB")
    print("=" * 50)
    
    if check_mongodb_status():
        print("\n📥 Test de chargement des données brutes:")
        df_raw = load_raw_articles()
        if not df_raw.empty:
            print(f"   Colonnes: {list(df_raw.columns)}")
            print(f"   Aperçu:\n{df_raw.head(2)}")
        
        print("\n📥 Test de chargement des données traitées:")
        df_processed = load_training_data()
        if not df_processed.empty:
            print(f"   Colonnes: {list(df_processed.columns)}")
