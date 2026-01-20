import pandas as pd
import json
import os
import traceback
from cleaner import clean_text
from normalizer import normalize_text
from nlp_processor import process_nlp

# MongoDB Configuration
MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
MONGO_DB = "news_db"
MONGO_RAW_COLLECTION = "raw_articles"
MONGO_PROCESSED_COLLECTION = "processed_articles"


def get_mongo_client():
    """Crée et retourne une connexion MongoDB."""
    from pymongo import MongoClient
    return MongoClient(MONGO_HOST, MONGO_PORT)


def load_from_mongodb():
    """Charge les données brutes depuis MongoDB."""
    print(f"📡 Connexion à MongoDB ({MONGO_HOST}:{MONGO_PORT})...")
    client = get_mongo_client()
    db = client[MONGO_DB]
    collection = db[MONGO_RAW_COLLECTION]
    
    data = list(collection.find())
    print(f"✅ {len(data)} documents chargés depuis MongoDB")
    
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    # Supprimer l'_id MongoDB pour éviter les problèmes
    if '_id' in df.columns:
        df = df.drop('_id', axis=1)
    
    client.close()
    return df


def save_to_mongodb(df):
    """Sauvegarde les données traitées dans MongoDB."""
    print(f"💾 Sauvegarde dans MongoDB collection '{MONGO_PROCESSED_COLLECTION}'...")
    client = get_mongo_client()
    db = client[MONGO_DB]
    collection = db[MONGO_PROCESSED_COLLECTION]
    
    # Convertir DataFrame en liste de dictionnaires
    records = df.to_dict('records')
    
    # Supprimer les anciens documents et insérer les nouveaux
    collection.delete_many({})
    if records:
        collection.insert_many(records)
    
    print(f"✅ {len(records)} documents sauvegardés dans MongoDB")
    client.close()


def preprocess_row(text):
    if not isinstance(text, str):
        return []
    
    cleaned = clean_text(text)
    normalized = normalize_text(cleaned)
    tokens = process_nlp(normalized)
    return tokens


def main(input_path, output_path, use_mongodb=False):
    """
    Pipeline principal de prétraitement.
    
    Args:
        input_path: Chemin du fichier CSV d'entrée (ignoré si use_mongodb=True)
        output_path: Chemin du fichier CSV de sortie
        use_mongodb: Si True, lit depuis MongoDB au lieu du CSV
    """
    
    try:
        # Charger les données depuis MongoDB ou CSV
        if use_mongodb:
            print("🔄 Mode MongoDB activé")
            df = load_from_mongodb()
            if df.empty:
                print("⚠️ Aucune donnée trouvée dans MongoDB, tentative avec CSV...")
                df = pd.read_csv(input_path)
        else:
            print(f"Loading data from {input_path}...")
            df = pd.read_csv(input_path)
        
        # Trouver la colonne de texte
        text_col = None
        possible_cols = ['article', 'text', 'content', 'body', 'News']
        
        for col in df.columns:
            if col in possible_cols:
                text_col = col
                break
        
        if not text_col:
            print(f"Columns found: {df.columns}")
            print("Could not identify text column automatically. Using the first column.")
            text_col = df.columns[0]

        print(f"Processing column: {text_col}")
        
        # Appliquer le prétraitement
        df['processed_tokens'] = df[text_col].apply(preprocess_row)
        
        # Sauvegarder les résultats
        print(f"Saving processed data to {output_path}...")
        df.to_csv(output_path, index=False)
        
        # Sauvegarder aussi dans MongoDB si le mode est activé
        if use_mongodb:
            save_to_mongodb(df)
        
        print("✅ Prétraitement terminé avec succès!")

    except Exception as e:
        print(f"Error processing file: {e}")
        traceback.print_exc()

import sys

if __name__ == "__main__":
    # Default paths for testing
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Vérifier si le mode MongoDB est demandé
    use_mongodb = "--mongodb" in sys.argv
    
    # Retirer le flag de la liste des arguments
    args = [arg for arg in sys.argv[1:] if arg != "--mongodb"]
    
    if len(args) >= 2:
        INPUT_FILE = args[0]
        OUTPUT_FILE = args[1]
    else:
        INPUT_FILE = os.path.join(base_dir, "preprocessing", "ai_vs_human_news.csv")
        OUTPUT_FILE = os.path.join(base_dir, "preprocessing", "processed_news.csv")
    
    print(f"Resolved Input Path: {INPUT_FILE}")
    print(f"Mode MongoDB: {'Activé' if use_mongodb else 'Désactivé'}")
    
    if not use_mongodb and not os.path.exists(INPUT_FILE):
        print("⚠️ Fichier CSV non trouvé. Activation du mode MongoDB...")
        use_mongodb = True
    
    main(INPUT_FILE, OUTPUT_FILE, use_mongodb=use_mongodb)

