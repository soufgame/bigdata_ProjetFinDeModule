"""
DAG de Prétraitement des Données Textuelles
============================================
Ce DAG orchestre le pipeline complet de nettoyage et prétraitement
des données textuelles pour le projet Big Data.

Auteur: Data Engineering Team
Date: Janvier 2026
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

# Configuration des chemins pour l'environnement Docker
PREPROCESSING_PATH = '/opt/airflow/preprocessing'
INPUT_FILE = '/opt/airflow/kafka_project/ai_vs_human_news.csv'
OUTPUT_FILE = '/opt/airflow/preprocessing/processed_news.csv'

# Ajouter le module preprocessing au path
sys.path.insert(0, PREPROCESSING_PATH)


def run_preprocessing_pipeline(**kwargs):
    """
    Exécute le pipeline complet de prétraitement.
    
    Étapes:
    1. Chargement des données brutes
    2. Nettoyage (URLs, HTML, emojis)
    3. Normalisation (minuscules, ponctuation, chiffres)
    4. Traitement NLP (tokenisation, stopwords, lemmatisation)
    5. Export du dataset final
    """
    from pipeline import main as pipeline_main
    
    input_file = kwargs.get('input_file', INPUT_FILE)
    output_file = kwargs.get('output_file', OUTPUT_FILE)
    
    print(f"📂 Fichier d'entrée: {input_file}")
    print(f"📁 Fichier de sortie: {output_file}")
    
    pipeline_main(input_file, output_file)
    
    print("✅ Pipeline de prétraitement terminé avec succès!")


# Configuration par défaut du DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Définition du DAG
with DAG(
    dag_id='preprocessing_pipeline',
    default_args=default_args,
    description='Pipeline de nettoyage et prétraitement des données textuelles pour le projet Big Data',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['bigdata', 'preprocessing', 'nlp', 'text-processing'],
) as dag:
    
    # Documentation du DAG
    dag.doc_md = """
    ## Pipeline de Prétraitement des Données Textuelles
    
    ### Description
    Ce DAG exécute automatiquement le pipeline de prétraitement des données textuelles.
    
    ### Étapes du Pipeline
    1. **Nettoyage** : Suppression des URLs, balises HTML, emojis
    2. **Normalisation** : Conversion en minuscules, suppression ponctuation/chiffres
    3. **Traitement NLP** : Tokenisation, suppression stopwords, lemmatisation
    
    ### Fichiers
    - **Entrée** : `ai_vs_human_news.csv`
    - **Sortie** : `processed_news.csv`
    
    ### Owner
    Data Engineering Team
    """
    
    # Tâche principale de prétraitement
    run_preprocessing = PythonOperator(
        task_id='run_preprocessing',
        python_callable=run_preprocessing_pipeline,
        op_kwargs={
            'input_file': INPUT_FILE,
            'output_file': OUTPUT_FILE
        },
        doc_md="""
        ### Tâche: run_preprocessing
        Exécute le pipeline complet de prétraitement sur les données brutes.
        
        **Modules utilisés:**
        - `cleaner.py` : Nettoyage des textes
        - `normalizer.py` : Normalisation des textes
        - `nlp_processor.py` : Traitement NLP avec NLTK
        """
    )
    
    run_preprocessing
