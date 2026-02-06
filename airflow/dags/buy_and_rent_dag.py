"""
DAG principal pour le pipeline Big Data Buy & Rent
Architecture complète avec Kafka pour l'ingestion et Spark pour le traitement

Pipeline:
1. Collecte des données (annonces, risques, taux)
2. Ingestion vers Kafka (streaming)
3. Traitement avec Spark (batch)
4. Chargement dans PostgreSQL
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Import des modules métier
from data_collectors.listings_collector import collect_listings
from data_collectors.georisks_collector import collect_georisks
from data_collectors.rates_collector import collect_rates
from kafka_utils.producer import send_listings_to_kafka, send_georisks_to_kafka, send_rates_to_kafka
from kafka_utils.consumer import consume_and_save
from loaders.postgres_loader import load_to_postgres

# Import pour le fallback Python (si Spark indisponible)
from transformers.enrichment import enrich_listings


def prepare_spark_data(**context):
    """
    Prépare les données pour le traitement Spark.
    Récupère les données des XCom et les sauvegarde en fichiers JSON.

    Note: Airflow monte ./data sur /opt/airflow/data
          Spark monte ./data sur /opt/spark/data
    """
    import json
    import os
    from datetime import datetime

    ti = context['ti']
    execution_date = context.get('execution_date')
    batch_id = execution_date.strftime('%Y%m%d_%H%M%S') if execution_date else datetime.now().strftime('%Y%m%d_%H%M%S')

    # Répertoire de données (côté Airflow)
    airflow_data_dir = '/opt/airflow/data'
    batch_subdir = f'batch_{batch_id}'
    batch_dir = os.path.join(airflow_data_dir, batch_subdir)
    os.makedirs(batch_dir, exist_ok=True)

    # Récupère les données des collecteurs
    listings = ti.xcom_pull(task_ids='collect_listings', key='raw_listings') or []
    georisks = ti.xcom_pull(task_ids='collect_georisks', key='georisks') or []
    rates = ti.xcom_pull(task_ids='collect_rates', key='rates') or {}

    # Sauvegarde les annonces (format JSON Lines pour Spark)
    listings_path = os.path.join(batch_dir, 'listings.json')
    with open(listings_path, 'w') as f:
        for listing in listings:
            f.write(json.dumps(listing) + '\n')

    # Sauvegarde les risques
    georisks_path = os.path.join(batch_dir, 'georisks.json')
    with open(georisks_path, 'w') as f:
        for risk in georisks:
            f.write(json.dumps(risk) + '\n')

    # Sauvegarde les taux
    rates_path = os.path.join(batch_dir, 'rates.json')
    with open(rates_path, 'w') as f:
        json.dump(rates, f)

    # Chemins pour Spark (côté Spark: /opt/spark/data)
    spark_data_dir = '/opt/spark/data'
    spark_batch_dir = os.path.join(spark_data_dir, batch_subdir)

    # Stocke les chemins SPARK dans XCom (pas les chemins Airflow)
    ti.xcom_push(key='spark_listings_path', value=os.path.join(spark_batch_dir, 'listings.json'))
    ti.xcom_push(key='spark_georisks_path', value=os.path.join(spark_batch_dir, 'georisks.json'))
    ti.xcom_push(key='spark_rates_path', value=os.path.join(spark_batch_dir, 'rates.json'))
    ti.xcom_push(key='spark_output_path', value=os.path.join(spark_batch_dir, 'enriched'))
    ti.xcom_push(key='spark_batch_dir', value=spark_batch_dir)
    # Garde aussi le chemin Airflow pour load_spark_results
    ti.xcom_push(key='airflow_batch_dir', value=batch_dir)

    print(f"✅ Données préparées pour Spark dans: {batch_dir}")
    print(f"   - Annonces: {len(listings)}")
    print(f"   - Risques: {len(georisks)}")
    print(f"   - Taux: {rates}")

    return batch_dir


def load_spark_results(**context):
    """
    Charge les résultats du traitement Spark et les prépare pour PostgreSQL.
    Utilise les chemins Airflow (pas Spark) pour lire les fichiers.
    """
    import json
    import os
    import glob

    ti = context['ti']
    # Utilise le chemin Airflow, pas le chemin Spark
    batch_dir = ti.xcom_pull(task_ids='prepare_spark_data', key='airflow_batch_dir')
    output_path = os.path.join(batch_dir, 'enriched')

    enriched_listings = []

    # Lecture des fichiers JSON produits par Spark
    json_files = glob.glob(os.path.join(output_path, '*.json'))

    for json_file in json_files:
        with open(json_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        listing = json.loads(line)
                        enriched_listings.append(listing)
                    except json.JSONDecodeError:
                        continue

    print(f"✅ {len(enriched_listings)} annonces enrichies chargées depuis Spark")

    # Stocke dans XCom pour le loader PostgreSQL
    ti.xcom_push(key='enriched_listings', value=enriched_listings)

    return enriched_listings


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
    description='Pipeline Big Data avec Kafka (ingestion) et Spark (traitement)',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigdata', 'immobilier', 'kafka', 'spark'],
) as dag:

    # ============================================
    # PHASE 1: COLLECTE DES DONNÉES
    # ============================================

    task_collect_listings = PythonOperator(
        task_id='collect_listings',
        python_callable=collect_listings,
        doc='Collecte les annonces immobilières (simulées)',
    )

    task_collect_georisks = PythonOperator(
        task_id='collect_georisks',
        python_callable=collect_georisks,
        doc='Collecte les risques géographiques via API Georisques',
    )

    task_collect_rates = PythonOperator(
        task_id='collect_rates',
        python_callable=collect_rates,
        doc='Collecte les taux financiers via scraping',
    )

    # ============================================
    # PHASE 2: INGESTION KAFKA (Streaming)
    # ============================================

    task_send_listings_kafka = PythonOperator(
        task_id='send_listings_to_kafka',
        python_callable=send_listings_to_kafka,
        op_kwargs={'listings': None},  # Récupère via XCom
        doc='Envoie les annonces vers le topic Kafka raw-listings',
    )

    task_send_georisks_kafka = PythonOperator(
        task_id='send_georisks_to_kafka',
        python_callable=send_georisks_to_kafka,
        op_kwargs={'georisks': None},
        doc='Envoie les risques vers le topic Kafka georisks',
    )

    task_send_rates_kafka = PythonOperator(
        task_id='send_rates_to_kafka',
        python_callable=send_rates_to_kafka,
        op_kwargs={'rates': None},
        doc='Envoie les taux vers le topic Kafka rates',
    )

    # ============================================
    # PHASE 3: PRÉPARATION POUR SPARK
    # ============================================

    task_prepare_spark = PythonOperator(
        task_id='prepare_spark_data',
        python_callable=prepare_spark_data,
        doc='Prépare les données en fichiers JSON pour Spark',
    )

    # ============================================
    # PHASE 4: TRAITEMENT SPARK (Batch)
    # ============================================

    # Job Spark pour l'enrichissement des données
    task_spark_transform = BashOperator(
        task_id='spark_transform',
        bash_command="""
        docker exec big-data-spark-master-1 /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/spark/jobs/transform_listings.py \
            {{ ti.xcom_pull(task_ids='prepare_spark_data', key='spark_listings_path') }} \
            {{ ti.xcom_pull(task_ids='prepare_spark_data', key='spark_georisks_path') }} \
            {{ ti.xcom_pull(task_ids='prepare_spark_data', key='spark_rates_path') }} \
            {{ ti.xcom_pull(task_ids='prepare_spark_data', key='spark_output_path') }}
        """,
        doc='Exécute le job Spark de transformation des données',
    )

    # ============================================
    # PHASE 5: CHARGEMENT DES RÉSULTATS SPARK
    # ============================================

    task_load_spark_results = PythonOperator(
        task_id='load_spark_results',
        python_callable=load_spark_results,
        doc='Charge les résultats Spark pour PostgreSQL',
    )

    # ============================================
    # PHASE 6: CHARGEMENT POSTGRESQL
    # ============================================

    task_load_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        doc='Charge les données enrichies dans PostgreSQL',
    )

    # ============================================
    # DÉFINITION DES DÉPENDANCES
    # ============================================

    # Phase 1: Collecte en parallèle
    # Phase 2: Envoi vers Kafka (après collecte)
    task_collect_listings >> task_send_listings_kafka
    task_collect_georisks >> task_send_georisks_kafka
    task_collect_rates >> task_send_rates_kafka

    # Phase 3: Préparation Spark (après toutes les collectes)
    [task_collect_listings, task_collect_georisks, task_collect_rates] >> task_prepare_spark

    # Phase 4: Traitement Spark (après préparation et après Kafka)
    [task_prepare_spark, task_send_listings_kafka, task_send_georisks_kafka, task_send_rates_kafka] >> task_spark_transform

    # Phase 5 & 6: Chargement
    task_spark_transform >> task_load_spark_results >> task_load_postgres
