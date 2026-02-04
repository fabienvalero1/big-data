import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, when, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, BooleanType

# Configuration
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092') # Internal docker address
POSTGRES_URL = "jdbc:postgresql://postgres:5432/buyandrent"
POSTGRES_PROPERTIES = {
    "user": "admin",
    "password": "password",
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified"
}

def get_spark_session():
    return SparkSession.builder \
        .appName("BuyAndRentStreamProcessor") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

# Defines the schema for the listings coming from Kafka
def get_listing_schema():
    return StructType([
        StructField("id", StringType()),
        StructField("timestamp", StringType()),
        StructField("code_insee", StringType()),
        StructField("ville", StringType()),
        StructField("surface", FloatType()),
        StructField("nb_pieces", IntegerType()),
        StructField("price", FloatType()),
        StructField("dpe", StringType()),
        StructField("description", StringType())
        # Add more fields if needed
    ])

def process_batch(batch_df, batch_id):
    print(f"Processing Batch ID: {batch_id} with {batch_df.count()} records")
    
    # Write to Postgres (Data Warehouse / Serving Layer)
    batch_df.write \
        .jdbc(url=POSTGRES_URL, table="fact_listings", mode="append", properties=POSTGRES_PROPERTIES)

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Reading reference data from Postgres...")
    # Load Risk Data (Static or refreshed periodically in a real app)
    # Using the risks directly from the DB initialized via init.sql (or could be Kafka batch)
    # For now, let's assume init.sql populated some defaults or we rely on defaults
    # If the table is empty, this DF is empty.
    # Ideally, we would read the 'ref-georisques' topic as a table.
    try:
        df_risks = spark.read.jdbc(url=POSTGRES_URL, table="ref_georisques", properties=POSTGRES_PROPERTIES)
    except Exception as e:
        print(f"Warning: Could not load ref_georisques: {e}")
        # Create empty DF if table doesn't exist yet to avoid crash
        df_risks = spark.createDataFrame([], schema=StructType([
            StructField("code_insee", StringType()),
            StructField("risque_inondation", BooleanType())
        ]))

    # Load Financial Rates (Static for now)
    try:
        df_rates = spark.read.jdbc(url=POSTGRES_URL, table="ref_taux", properties=POSTGRES_PROPERTIES)
        # Take the most recent rate
        current_rate = df_rates.orderBy(col("date_valeur").desc()).limit(1).collect()
        if current_rate:
            interest_rate = current_rate[0]['taux_interet_moyen'] / 100.0
        else:
            interest_rate = 0.035 # Fallback 3.5%
    except:
         interest_rate = 0.035

    print(f"Using Interest Rate: {interest_rate}")

    # Read Stream
    print("Starting Stream from Kafka...")
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "real-estate-raw") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    listing_schema = get_listing_schema()
    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), listing_schema).alias("data")
    ).select("data.*")

    # Enrich and Calculate logic
    # 1. Join with Risks
    enriched = parsed_stream.join(df_risks, "code_insee", "left_outer")

    # 2. Financial Calcs
    # Simple Monthly Payment Formula: P * r/12 / (1 - (1+r/12)^-n)
    # Assume 20 years (240 months)
    months = 240
    monthly_rate = interest_rate / 12.0
    
    # Avoid division by zero if rate is 0
    if interest_rate == 0:
        processed = enriched.withColumn("mensualite", col("price") / months)
    else:
        # P * (r) / (1 - (1+r)^-n)
        factor = (monthly_rate) / (1 - (1 + monthly_rate) ** (-months))
        processed = enriched.withColumn("mensualite", col("price") * factor)

    # Rent Estimate (Naive: 5% gross yield default if not specified)
    # In a real app, join with market rental prices
    processed = processed.withColumn("loyer_estime", col("price") * 0.05 / 12)

    # Cashflow: Rent - Payment - Charges (estimated 100€) - Tax (estimated 50€)
    processed = processed.withColumn("cashflow", col("loyer_estime") - col("mensualite") - 150)

    # Rentabilite Brute: (Loyer * 12) / Price * 100
    processed = processed.withColumn("rentabilite_brute", (col("loyer_estime") * 12 / col("price")) * 100)

    # Investment Score (0-10)
    # Basic logic: High yield & positive cashflow -> good score
    processed = processed.withColumn("score_investissement", 
        when(col("cashflow") > 0, 8.0)
        .when(col("rentabilite_brute") > 6, 6.0)
        .otherwise(4.0)
    )

    # Prepare final columns for Postgres
    final_stream = processed.select(
        col("id").alias("listing_id"),
        col("code_insee"),
        col("price").alias("prix_acquisition"),
        col("loyer_estime"),
        col("rentabilite_brute"),
        col("cashflow"),
        col("score_investissement"),
        current_timestamp().alias("date_creation"),
        # Pack raw data into metadata jsonb if needed, here we just select cols
        # For 'metadata' JSONB column in PG, we can create a struct and to_json
        # to_json(struct(col("dpe"), col("description"))).alias("metadata")
    )

    # Start Query
    query = final_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/checkpoints") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
