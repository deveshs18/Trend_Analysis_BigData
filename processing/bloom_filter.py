from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, udf, lit
from pyspark.sql.types import StructType, StringType, TimestampType, ArrayType, IntegerType
from pyspark.sql import Row
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "analytics")

# Initialize Spark Session with PostgreSQL JDBC driver
spark = SparkSession.builder \
    .appName("TwitterSportsBloomFilter") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3") \
    .config("spark.kafka.log.level", "DEBUG") \
    .getOrCreate()

# Define schema
schema = StructType() \
    .add("text", StringType()) \
    .add("created_at", TimestampType()) \
    .add("sentiment", StringType()) \
    .add("entities", ArrayType(StructType([])))

# Kafka Stream setup
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "twitter_sentiment") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.text"),
        col("data.created_at"),
        col("data.sentiment")
    )

# Initialize sports keywords set
sports_terms = {
    "sports", "football", "soccer", "basketball", "cricket", "tennis", "baseball", "hockey", "golf",
    "olympics", "fifa", "nba", "ipl", "nfl", "wimbledon", "ufc", "boxing", "volleyball", "rugby"
}

player_names = {
    "ronaldo", "messi", "neymar", "mbappe", "virat", "dhoni", "rohit", "kohli", "federer",
    "nadal", "djokovic", "lebron", "jordan", "kobe", "steph", "kyrie", "giannis",
    "maxwell", "butler", "babar", "shami", "bryant", "raja", "hardik"
}

sports_keywords = sports_terms.union(player_names)

# UDF to check for sports keywords
def check_sports(text):
    if not text:
        return 0
    words = set(word.lower() for word in text.split())
    return 1 if words.intersection(sports_keywords) else 0

check_sports_udf = udf(check_sports, IntegerType())

# Apply sports keyword check
df_flagged = df_json.withColumn("is_sports", check_sports_udf(col("text")))

# Function to write to Postgres
def write_to_postgres(batch_df, batch_id):
    try:
        if batch_df.isEmpty():
            print(f"Batch {batch_id} is empty - skipping")
            return

        # Debug: Print schema and sample data
        print(f"\n=== Processing Batch {batch_id} ===")
        print("DataFrame Schema:")
        batch_df.printSchema()
        print("\nSample data:")
        batch_df.show(5, truncate=False)

        # Add processing metadata
        result_df = batch_df.withColumn("batch_id", lit(batch_id)) \
                           .withColumn("processing_time", lit(datetime.now()))

        # Debug final output
        print("Final DataFrame to be written:")
        result_df.show(5, truncate=False)

        # Write to PostgreSQL
        print(f"Writing batch {batch_id} to PostgreSQL...")
        result_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}") \
            .option("dbtable", "bloom_filter") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()
        
        print(f"Successfully wrote batch {batch_id} to PostgreSQL")

    except Exception as e:
        print(f"ERROR processing batch {batch_id}: {str(e)}")
        # Write errors to a separate table
        error_df = spark.createDataFrame([(batch_id, str(e), datetime.now())], 
                                       ["batch_id", "error", "error_time"])
        error_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}") \
            .option("dbtable", "processing_errors") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()

# Start streaming
print("Starting streaming query...")
query = df_flagged.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

print("Streaming query started. Waiting for data...")
query.awaitTermination()
