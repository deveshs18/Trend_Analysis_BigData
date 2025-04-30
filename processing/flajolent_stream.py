from pyspark.sql import Row
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json
from pyspark.sql.types import StructType, StringType, TimestampType, ArrayType
from dotenv import load_dotenv
import os
import hashlib

# Set this up in PostgresSQL

# CREATE TABLE fm_estimates (
#     batch_id INTEGER,
#     timestamp TIMESTAMP,
#     fm_estimate INTEGER
# );


# Helper functions for Flajolet-Martin
def trailing_zeros(x):
    return len(bin(x)) - len(bin(x).rstrip('0'))

def flajolet_martin_estimate(values):
    max_zero = 0
    for v in values:
        h = int(hashlib.md5(v.encode('utf-8')).hexdigest(), 16)
        max_zero = max(max_zero, trailing_zeros(h))
    return 2 ** max_zero


load_dotenv()
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

spark = SparkSession.builder \
    .appName("TwitterSentimentAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.kafka.log.level", "DEBUG") \
    .getOrCreate()

schema = StructType() \
    .add("text", StringType()) \
    .add("created_at", TimestampType()) \
    .add("sentiment", StringType()) \
    .add("entities", ArrayType(StructType([])))  # Schema inside struct can be expanded later

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "twitter_sentiment") \
    .option("startingOffsets", "latest") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.text"),
        col("data.created_at").cast(TimestampType()),
        col("data.sentiment"),
        to_json(col("data.entities")).alias("entities")  # <- Serialize as JSON
    )

def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        print("Empty batch received - skipping")
        return

    # Debug print the incoming batch
    print(f"\n=== Processing Batch {batch_id} ===")
    print("Sample data from batch:")
    batch_df.show(5, truncate=False)
    
    # Get tweet texts as a list
    texts = batch_df.select("text").rdd.map(lambda row: row["text"]).collect()
    print(f"\nFirst 3 text samples: {texts[:3]}")
    
    est_unique_count = flajolet_martin_estimate(texts)
    print(f"Calculated Flajolet-Martin estimate: {est_unique_count}")

    # Prepare metrics data
    metrics_data = [Row(
        batch_id=batch_id,
        timestamp=datetime.utcnow(),
        set_name="tweets",
        estimated_cardinality=est_unique_count
    )]
    
    metrics_df = spark.createDataFrame(metrics_data)
    
    # Debug before writing to flajolent_stream
    print("\nMetrics DataFrame to be written to flajolent_stream:")
    metrics_df.show()
    
    print("\nAttempting to write to flajolent_stream table...")
    try:
        metrics_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}") \
            .option("dbtable", 'flajolent_stream') \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()
        print("Successfully wrote to flajolent_stream table")
    except Exception as e:
        print(f"Error writing to flajolent_stream: {str(e)}")
        # Write errors to a separate table
        error_df = spark.createDataFrame([(batch_id, str(e), datetime.now())], 
                                       ["batch_id", "error", "error_time"])
        error_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}") \
            .option("dbtable", 'processing_errors') \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()
    
    print(f"=== Finished processing Batch {batch_id} ===\n")


query = df_json.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()