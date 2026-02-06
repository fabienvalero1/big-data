"""
Loader PostgreSQL
Charge les données enrichies dans la base de données
Compatible avec les sorties Python et Spark
"""
import json
import logging
import os
from datetime import datetime

import psycopg2

logger = logging.getLogger(__name__)


def get_db_connection():
    """Crée une connexion à la base PostgreSQL"""
    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'postgres'),
        database=os.environ.get('DB_NAME', 'airflow'),
        user=os.environ.get('DB_USER', 'airflow'),
        password=os.environ.get('DB_PASSWORD', 'airflow'),
    )


def load_to_postgres(**context):
    """
    Charge les annonces enrichies dans PostgreSQL.
    Compatible avec les données issues de:
    - enrichment.py (traitement Python)
    - transform_listings.py (traitement Spark)
    Compare le volume en entrée et le volume effectivement inséré.
    """
    ti = context['ti']
    execution_date = context.get("execution_date")
    # Convertit en datetime Python pur (évite le Proxy Airflow)
    if execution_date:
        batch_ts = datetime.fromisoformat(str(execution_date).replace('+00:00', ''))
    else:
        batch_ts = datetime.utcnow()

    logger.info("Debut du chargement dans PostgreSQL pour le batch %s", batch_ts)

    # Essaie d'abord de récupérer depuis Spark (load_spark_results)
    listings = ti.xcom_pull(task_ids='load_spark_results', key='enriched_listings')

    # Fallback sur le traitement Python classique
    if not listings:
        listings = ti.xcom_pull(task_ids='enrich_listings', key='enriched_listings')

    if not listings:
        logger.warning("[WARNING] Aucune donnée à charger dans PostgreSQL")
        return 0

    # Convertit TOUTE la liste en types Python purs (élimine les Proxy Airflow)
    listings = json.loads(json.dumps(list(listings), default=str))

    total_in = len(listings)
    logger.info("Nombre d'annonces en entrée pour chargement: %s", total_in)

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Insert les annonces
        insert_query = """
            INSERT INTO fact_listings 
            (listing_id, code_insee, prix_acquisition, loyer_estime, rentabilite_brute, cashflow, score_investissement, date_creation)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (listing_id) DO NOTHING
        """

        inserted = 0
        failed = 0

        for listing in listings:
            try:
                cursor.execute(
                    insert_query,
                    (
                        listing['id'],
                        listing['code_insee'],
                        listing['price'],
                        listing['loyer_estime'],
                        listing['rentabilite_brute'],
                        listing['cashflow'],
                        listing['score_investissement'],
                        batch_ts,
                    ),
                )
                # rowcount == 1 si insertion, 0 si conflit (ON CONFLICT DO NOTHING)
                if cursor.rowcount == 1:
                    inserted += 1
            except Exception as e:
                failed += 1
                logger.exception("[ERROR] Erreur insertion pour l'annonce %s: %s", listing.get('id'), e)

        conn.commit()

        logger.info(
            "Chargement terminé: %s en entrée, %s insérées (nouvelles), %s en erreur, %s ignorées (doublons)",
            total_in,
            inserted,
            failed,
            total_in - inserted - failed,
        )

        return inserted

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
