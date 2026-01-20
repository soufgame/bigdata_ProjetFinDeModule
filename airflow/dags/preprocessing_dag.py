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
    Simule la collecte de nouvelles données.
    """
    from datetime import datetime
    
    print("📡 Collecte des données en cours...")
    
    # Simulation de données collectées
    data = {
        'source': 'newsapi',
        'title': f'Article collecté le {datetime.now().strftime("%Y-%m-%d %H:%M")}',
        'content': 'This is sample article content with URLs https://example.com and emojis 😊 The CATS are running!!!',
        'published_at': datetime.now().isoformat()
    }
    
    print(f"✅ Données collectées: {data['title']}")
    return data


# ===================== TÂCHE 2: STOCKAGE EN CSV =====================
def store_to_csv(**kwargs):
    """
    Stocke les données dans un fichier CSV.
    """
    import pandas as pd
    
    ti = kwargs['ti']
    new_data = ti.xcom_pull(task_ids='collect_data')
    
    print("💾 Stockage des données...")
    
    # Utiliser le fichier existant
    csv_path = '/opt/airflow/preprocessing/ai_vs_human_news.csv'
    
    # Lire le fichier existant
    try:
        df = pd.read_csv(csv_path)
        print(f"📊 Fichier existant chargé: {len(df)} lignes")
    except:
        df = pd.DataFrame()
        print("📄 Création d'un nouveau fichier")
    
    print(f"✅ Données prêtes pour le prétraitement")
    return csv_path


# ===================== TÂCHE 3: PRÉTRAITEMENT =====================
def run_preprocessing(**kwargs):
    """
    Exécute le pipeline de nettoyage.
    """
    import pandas as pd
    sys.path.insert(0, '/opt/airflow/preprocessing')
    
    from cleaner import clean_text
    from normalizer import normalize_text
    from nlp_processor import process_nlp
    
    ti = kwargs['ti']
    input_file = ti.xcom_pull(task_ids='store_to_csv')
    output_file = '/opt/airflow/preprocessing/processed_news.csv'
    
    print("🧹 Prétraitement en cours...")
    
    df = pd.read_csv(input_file)
    
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
    df.to_csv(output_file, index=False)
    
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
    
    # Tâche 2: Stockage
    t2_store = PythonOperator(
        task_id='store_to_csv',
        python_callable=store_to_csv,
    )
    
    # Tâche 3: Prétraitement
    t3_preprocess = PythonOperator(
        task_id='run_preprocessing',
        python_callable=run_preprocessing,
    )
    
    # Ordre d'exécution
    t1_collect >> t2_store >> t3_preprocess
