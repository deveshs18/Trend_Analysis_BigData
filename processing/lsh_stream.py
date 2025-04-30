from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json
from pyspark.sql.types import StructType, StringType, TimestampType, ArrayType
from pyspark.sql import Row
from datetime import datetime
from dotenv import load_dotenv
from datasketch import MinHash, MinHashLSH
import os
import random

# Load environment variables
load_dotenv()
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TwitterSentimentAnalysisWithLSH") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.kafka.log.level", "DEBUG") \
    .getOrCreate()

# Define schema for incoming Kafka data
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
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON and select fields
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.text"),
        col("data.created_at").cast(TimestampType()),
        col("data.sentiment"),
        to_json(col("data.entities")).alias("entities")
    )

# Initialize Global LSH
global_lsh = MinHashLSH(threshold=0.3, num_perm=128)

# MinHash helper
def get_minhash(text):
    m = MinHash(num_perm=128)
    for word in text.split():
        m.update(word.encode('utf8'))
    return m

# Add Laplace noise for Differential Privacy
def add_laplace_noise(count, epsilon=1.0):
    scale = 1.0 / epsilon
    noise = random.gauss(0, scale)
    return max(0, int(count + noise))

# Process each batch
def write_to_postgres_with_lsh(batch_df, batch_id):
    try:
        if batch_df.isEmpty():
            print("Empty batch received - skipping")
            return

        print(f"\n=== Processing Batch {batch_id} ===")
        batch_df.show(5, truncate=False)

        texts = batch_df.select("text").rdd.map(lambda row: row["text"]).collect()
        similar_pairs = []

        for i, text in enumerate(texts):
            m = get_minhash(text)
            tweet_id = f"tweet_{batch_id}_{i}"
            global_lsh.insert(tweet_id, m)
            similar = global_lsh.query(m)
            if len(similar) > 1:
                for sim in similar:
                    if sim != tweet_id:
                        similar_pairs.append((batch_id, tweet_id, sim, datetime.utcnow()))
                        print(f"Found similar tweets: {tweet_id} and {sim}")

        if similar_pairs:
            # Apply Differential Privacy to similarity frequency
            freq_dict = {}
            for _, tweet_id, sim, _ in similar_pairs:
                freq_dict[sim] = freq_dict.get(sim, 0) + 1

            noisy_similar_pairs = [
                (batch_id, tweet_id, sim, datetime.utcnow())
                for _, tweet_id, sim, _ in similar_pairs
                if add_laplace_noise(freq_dict[sim]) > 0
            ]

            similar_df = spark.createDataFrame(noisy_similar_pairs, ["batch_id", "tweet_id", "similar_to", "found_at"])
            print(f"\nWriting {len(noisy_similar_pairs)} Differentially Private Similar Tweets to Postgres...")

            similar_df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}") \
                .option("dbtable", "lsh_stream") \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .mode("append") \
                .save()
        else:
            print("\nNo similar tweets found in this batch")

        print(f"=== Finished processing Batch {batch_id} ===\n")

    except Exception as e:
        print(f"ERROR processing batch {batch_id}: {str(e)}")
        error_df = spark.createDataFrame([(batch_id, str(e), datetime.now())], ["batch_id", "error", "error_time"])
        error_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}") \
            .option("dbtable", 'processing_errors') \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()

# Start the streaming query
query = df_json.writeStream \
    .foreachBatch(write_to_postgres_with_lsh) \
    .outputMode("append") \
    .start()

query.awaitTermination()
