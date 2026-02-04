"""
DAG principal pour le pipeline Big Data Buy & Rent
Collecte, transforme et charge les données immobilières
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import des modules métier
from data_collectors.listings_collector import collect_listings
from data_collectors.georisks_collector import collect_georisks
from data_collectors.rates_collector import collect_rates
from transformers.enrichment import enrich_listings
from loaders.postgres_loader import load_to_postgres

default_args = {
    'owner': 'bigdata',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'buy_and_rent_pipeline',
    default_args=default_args,
    description='Pipeline Big Data pour analyse immobilière',
    schedule_interval=timedelta(hours=1),  # Exécution toutes les heures
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigdata', 'immobilier'],
) as dag:

    # Task 1: Collecter les annonces immobilières
    task_collect_listings = PythonOperator(
        task_id='collect_listings',
        python_callable=collect_listings,
    )

    # Task 2: Collecter les données de risques géographiques
    task_collect_georisks = PythonOperator(
        task_id='collect_georisks',
        python_callable=collect_georisks,
    )

    # Task 3: Collecter les taux financiers
    task_collect_rates = PythonOperator(
        task_id='collect_rates',
        python_callable=collect_rates,
    )

    # Task 4: Enrichir et transformer les données
    task_enrich = PythonOperator(
        task_id='enrich_listings',
        python_callable=enrich_listings,
    )

    # Task 5: Charger dans PostgreSQL
    task_load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    # Définition des dépendances
    # Les 3 collecteurs s'exécutent en parallèle, puis enrichissement, puis chargement
    [task_collect_listings, task_collect_georisks, task_collect_rates] >> task_enrich >> task_load
