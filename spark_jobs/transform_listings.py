"""
Job Spark pour l'enrichissement des données immobilières
Transforme les données brutes en données enrichies avec indicateurs financiers
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, round as spark_round, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, BooleanType, DoubleType
)
import json
import sys
import os

# Schéma pour les annonces
LISTINGS_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("timestamp", StringType(), True),
    StructField("batch_date", StringType(), True),
    StructField("code_insee", StringType(), False),
    StructField("ville", StringType(), True),
    StructField("surface", IntegerType(), True),
    StructField("nb_pieces", IntegerType(), True),
    StructField("price", DoubleType(), False),
    StructField("dpe", StringType(), True),
    StructField("description", StringType(), True),
])

# Schéma pour les risques géographiques
GEORISKS_SCHEMA = StructType([
    StructField("code_insee", StringType(), False),
    StructField("risque_inondation", BooleanType(), True),
    StructField("risque_industriel", BooleanType(), True),
    StructField("risque_sismique", IntegerType(), True),
])


def create_spark_session():
    """Crée une session Spark"""
    return SparkSession.builder \
        .appName("BuyAndRent_Enrichment") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()


def enrich_with_spark(listings_json: str, georisks_json: str, rates_json: str, output_path: str):
    """
    Enrichit les annonces immobilières avec Spark.

    Args:
        listings_json: Chemin vers le fichier JSON des annonces
        georisks_json: Chemin vers le fichier JSON des risques
        rates_json: Chemin vers le fichier JSON des taux
        output_path: Chemin de sortie pour les données enrichies
    """
    spark = create_spark_session()

    try:
        print("🚀 Démarrage du job Spark d'enrichissement...")

        # Charge les données depuis les fichiers JSON
        listings_df = spark.read.schema(LISTINGS_SCHEMA).json(listings_json)
        georisks_df = spark.read.schema(GEORISKS_SCHEMA).json(georisks_json)

        # Charge les taux depuis le fichier
        with open(rates_json, 'r') as f:
            rates = json.load(f)

        # Paramètres financiers
        interest_rate = float(rates.get('taux_nominal_20ans', 4.5)) / 100.0
        months = 240  # 20 ans
        monthly_rate = interest_rate / 12.0 if interest_rate > 0 else 0.0

        print(f"📊 Taux d'intérêt utilisé: {interest_rate * 100:.2f}%")
        print(f"📊 Nombre d'annonces à traiter: {listings_df.count()}")

        # Jointure avec les risques géographiques
        enriched_df = listings_df.join(
            georisks_df,
            on="code_insee",
            how="left"
        )

        # Remplit les valeurs nulles pour les risques
        enriched_df = enriched_df.fillna({
            "risque_inondation": False,
            "risque_industriel": False,
            "risque_sismique": 0
        })

        # Calcul de la mensualité (formule de prêt amortissable)
        # M = P * [r(1+r)^n] / [(1+r)^n - 1]
        if monthly_rate > 0:
            factor = monthly_rate * pow(1 + monthly_rate, months) / (pow(1 + monthly_rate, months) - 1)
        else:
            factor = 1.0 / months

        enriched_df = enriched_df.withColumn(
            "mensualite",
            spark_round(col("price") * lit(factor), 2)
        )

        # Loyer estimé (5% de rendement brut annuel)
        enriched_df = enriched_df.withColumn(
            "loyer_estime",
            spark_round(col("price") * lit(0.05) / lit(12), 2)
        )

        # Cashflow mensuel (loyer - mensualité - charges estimées 150€)
        enriched_df = enriched_df.withColumn(
            "cashflow",
            spark_round(col("loyer_estime") - col("mensualite") - lit(150), 2)
        )

        # Rentabilité brute en %
        enriched_df = enriched_df.withColumn(
            "rentabilite_brute",
            spark_round((col("loyer_estime") * lit(12) / col("price")) * lit(100), 2)
        )

        # Score d'investissement (0-10)
        enriched_df = enriched_df.withColumn(
            "score_investissement",
            when(col("cashflow") > 0, lit(8.0))
            .when(col("rentabilite_brute") > 6, lit(6.0))
            .otherwise(lit(4.0))
        )

        # Ajoute un flag indiquant si les risques sont complets
        enriched_df = enriched_df.withColumn(
            "risques_complets",
            col("risque_sismique").isNotNull()
        )

        # Ajoute timestamp de traitement
        enriched_df = enriched_df.withColumn(
            "spark_processed_at",
            current_timestamp()
        )

        # Filtre les annonces avec prix valide
        enriched_df = enriched_df.filter(col("price") > 0)

        # Affiche un aperçu
        print("✅ Aperçu des données enrichies:")
        enriched_df.select(
            "id", "ville", "price", "loyer_estime",
            "cashflow", "rentabilite_brute", "score_investissement"
        ).show(5, truncate=False)

        # Sauvegarde en JSON
        enriched_df.coalesce(1).write.mode("overwrite").json(output_path)

        print(f"✅ {enriched_df.count()} annonces enrichies sauvegardées dans {output_path}")

        return enriched_df.count()

    finally:
        spark.stop()


if __name__ == "__main__":
    """
    Usage: spark-submit transform_listings.py <listings_json> <georisks_json> <rates_json> <output_path>
    """
    if len(sys.argv) != 5:
        print("Usage: spark-submit transform_listings.py <listings_json> <georisks_json> <rates_json> <output_path>")
        sys.exit(1)

    listings_path = sys.argv[1]
    georisks_path = sys.argv[2]
    rates_path = sys.argv[3]
    output_path = sys.argv[4]

    enrich_with_spark(listings_path, georisks_path, rates_path, output_path)
