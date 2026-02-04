"""
Loader PostgreSQL
Charge les données enrichies dans la base de données
"""
import os
import psycopg2
from datetime import datetime

def get_db_connection():
    """Crée une connexion à la base PostgreSQL"""
    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'postgres'),
        database=os.environ.get('DB_NAME', 'airflow'),
        user=os.environ.get('DB_USER', 'airflow'),
        password=os.environ.get('DB_PASSWORD', 'airflow')
    )

def load_to_postgres(**context):
    """
    Charge les annonces enrichies dans PostgreSQL
    """
    print("💾 Début du chargement dans PostgreSQL...")
    
    # Récupère les données enrichies
    ti = context['ti']
    listings = ti.xcom_pull(task_ids='enrich_listings', key='enriched_listings')
    
    if not listings:
        print("⚠️ Aucune donnée à charger")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Insert les annonces
    insert_query = """
        INSERT INTO fact_listings 
        (listing_id, code_insee, prix_acquisition, loyer_estime, rentabilite_brute, cashflow, score_investissement, date_creation)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (listing_id) DO NOTHING
    """
    
    count = 0
    for listing in listings:
        try:
            cursor.execute(insert_query, (
                listing['id'],
                listing['code_insee'],
                listing['price'],
                listing['loyer_estime'],
                listing['rentabilite_brute'],
                listing['cashflow'],
                listing['score_investissement'],
                datetime.now()
            ))
            count += 1
        except Exception as e:
            print(f"❌ Erreur insertion: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ {count} annonces chargées dans PostgreSQL")
    
    return count
