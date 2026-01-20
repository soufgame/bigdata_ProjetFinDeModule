"""
DAG de Pipeline Big Data Complet
================================
Ce DAG orchestre le pipeline complet :
1. Collecte des données (scraping)
2. Stockage en CSV
3. Nettoyage et prétraitement NLP

"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

# Configuration des chemins
PREPROCESSING_PATH = '/opt/airflow/preprocessing'
INPUT_FILE = '/opt/airflow/preprocessing/ai_vs_human_news.csv'
OUTPUT_FILE = '/opt/airflow/preprocessing/processed_news.csv'

sys.path.insert(0, PREPROCESSING_PATH)


# ===================== TÂCHE 1: COLLECTE DES DONNÉES =====================
def collect_data(**kwargs):
    """
    Collecte les nouvelles données depuis une source externe (NewsAPI simulation).
    En production, cela pourrait être une API réelle.
    """
    from datetime import datetime
    import pandas as pd
    
    print("📡 Collecte des données en cours...")
    
    # Charger les données depuis le fichier source (simulant une API)
    csv_path = '/opt/airflow/preprocessing/ai_vs_human_news.csv'
    
    try:
        df = pd.read_csv(csv_path)
        print(f"✅ {len(df)} articles collectés depuis la source")
        
        # Convertir en liste de dictionnaires pour XCom
        articles = df.to_dict('records')
        
        # Ajouter un timestamp d'ingestion
        for article in articles:
            article['ingested_at'] = datetime.now().isoformat()
        
        print(f"✅ Données collectées avec timestamp d'ingestion")
        return articles
        
    except Exception as e:
        print(f"❌ Erreur lors de la collecte: {e}")
        return []


# ===================== TÂCHE 2: STOCKAGE DANS MONGODB =====================
def store_to_mongodb(**kwargs):
    """
    Stocke les données collectées dans MongoDB (collection raw_articles).
    """
    from pymongo import MongoClient
    from datetime import datetime
    
    ti = kwargs['ti']
    articles = ti.xcom_pull(task_ids='collect_data')
    
    if not articles:
        print("⚠️ Aucun article à stocker")
        return 0
    
    # Configuration MongoDB
    MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb")
    MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
    MONGO_DB = "news_db"
    
    print(f"💾 Stockage de {len(articles)} articles dans MongoDB...")
    print(f"📡 Connexion à MongoDB ({MONGO_HOST}:{MONGO_PORT})...")
    
    try:
        client = MongoClient(MONGO_HOST, MONGO_PORT)
        db = client[MONGO_DB]
        collection = db["raw_articles"]
        
        # Insérer les nouveaux articles
        # Option: remplacer tous les documents ou ajouter (ici on remplace pour éviter les doublons)
        collection.delete_many({})
        result = collection.insert_many(articles)
        
        print(f"✅ {len(result.inserted_ids)} articles stockés dans MongoDB (raw_articles)")
        client.close()
        
        return len(result.inserted_ids)
        
    except Exception as e:
        print(f"❌ Erreur MongoDB: {e}")
        return 0


# ===================== TÂCHE 3: PRÉTRAITEMENT =====================
def run_preprocessing(**kwargs):
    """
    Exécute le pipeline de nettoyage avec MongoDB.
    """
    import pandas as pd
    from pymongo import MongoClient
    
    sys.path.insert(0, '/opt/airflow/preprocessing')
    
    from cleaner import clean_text
    from normalizer import normalize_text
    from nlp_processor import process_nlp
    
    # Configuration MongoDB
    MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb")
    MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
    MONGO_DB = "news_db"
    
    output_file = '/opt/airflow/preprocessing/processed_news.csv'
    
    print("🧹 Prétraitement en cours...")
    print(f"📡 Connexion à MongoDB ({MONGO_HOST}:{MONGO_PORT})...")
    
    # Charger depuis MongoDB
    try:
        client = MongoClient(MONGO_HOST, MONGO_PORT)
        db = client[MONGO_DB]
        raw_collection = db["raw_articles"]
        
        data = list(raw_collection.find())
        print(f"✅ {len(data)} documents chargés depuis MongoDB")
        
        if data:
            df = pd.DataFrame(data)
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
        else:
            # Fallback vers CSV si MongoDB est vide
            print("⚠️ MongoDB vide, chargement depuis CSV...")
            csv_path = '/opt/airflow/preprocessing/ai_vs_human_news.csv'
            df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"⚠️ Erreur MongoDB: {e}, chargement depuis CSV...")
        csv_path = '/opt/airflow/preprocessing/ai_vs_human_news.csv'
        df = pd.read_csv(csv_path)
        client = None
    
    # Trouver la colonne de texte
    text_col = None
    for col in ['content', 'article', 'text', 'News']:
        if col in df.columns:
            text_col = col
            break
    
    if text_col is None:
        text_col = df.columns[0]
    
    print(f"📝 Traitement de la colonne: {text_col}")
    
    def preprocess(text):
        if not isinstance(text, str):
            return []
        cleaned = clean_text(text)
        normalized = normalize_text(cleaned)
        tokens = process_nlp(normalized)
        return tokens
    
    df['processed_tokens'] = df[text_col].apply(preprocess)
    
    # Sauvegarder en CSV
    df.to_csv(output_file, index=False)
    
    # Sauvegarder dans MongoDB
    try:
        if client is None:
            client = MongoClient(MONGO_HOST, MONGO_PORT)
            db = client[MONGO_DB]
        
        processed_collection = db["processed_articles"]
        records = df.to_dict('records')
        processed_collection.delete_many({})
        if records:
            processed_collection.insert_many(records)
        print(f"✅ {len(records)} documents sauvegardés dans MongoDB (processed_articles)")
        client.close()
    except Exception as e:
        print(f"⚠️ Erreur lors de la sauvegarde MongoDB: {e}")
    
    print(f"✅ Prétraitement terminé! {len(df)} lignes traitées")
    print(f"📁 Fichier sauvegardé: {output_file}")


# ===================== CONFIGURATION DU DAG =====================
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id='bigdata_pipeline',
    default_args=default_args,
    description='Pipeline Big Data: Collecte → Stockage → Prétraitement',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['bigdata', 'scraping', 'preprocessing', 'nlp'],
) as dag:
    
    # Tâche 1: Collecte
    t1_collect = PythonOperator(
        task_id='collect_data',
        python_callable=collect_data,
    )
    
    # Tâche 2: Stockage MongoDB
    t2_store = PythonOperator(
        task_id='store_to_mongodb',
        python_callable=store_to_mongodb,
    )
    
    # Tâche 3: Prétraitement
    t3_preprocess = PythonOperator(
        task_id='run_preprocessing',
        python_callable=run_preprocessing,
    )
    
    # Ordre d'exécution
    t1_collect >> t2_store >> t3_preprocess
