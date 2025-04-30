from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json
from pyspark.sql.types import StructType, StringType, TimestampType, ArrayType
from pyspark.sql import Row
from datetime import datetime
from dotenv import load_dotenv
import os
import hashlib

# Set this up in PostgresSQL
# CREATE TABLE cms_estimates (
#     batch_id INTEGER,
#     timestamp TIMESTAMP,
#     keyword TEXT,
#     estimated_count INTEGER
# );

# CMS (Count-Min Sketch) algorithm
class CountMinSketch:
    def __init__(self, width, depth):
        self.width = width
        self.depth = depth
        self.table = [[0] * width for _ in range(depth)]

    def _hash(self, item, i):
        """Simple hash function using MD5"""
        h = int(hashlib.md5((str(item) + str(i)).encode('utf-8')).hexdigest(), 16)
        return h % self.width

    def add(self, item):
        for i in range(self.depth):
            index = self._hash(item, i)
            self.table[i][index] += 1

    def count(self, item):
        min_count = float('inf')
        for i in range(self.depth):
            index = self._hash(item, i)
            min_count = min(min_count, self.table[i][index])
        return min_count

load_dotenv()
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TwitterSentimentAnalysisWithCMS") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.kafka.log.level", "DEBUG") \
    .getOrCreate()

# Define schema for incoming data
schema = StructType() \
    .add("text", StringType()) \
    .add("created_at", TimestampType()) \
    .add("sentiment", StringType()) \
    .add("entities", ArrayType(StructType([])))

# Define a simple Count-Min Sketch setup
cms = CountMinSketch(width=1000, depth=10)

# Kafka stream setup
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "twitter_sentiment") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON data and select relevant fields
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.text"),
        col("data.created_at").cast(TimestampType()),
        col("data.sentiment"),
        to_json(col("data.entities")).alias("entities")
    )

def write_to_postgres_with_cms(batch_df, batch_id):
    try:
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
        
        # Update CMS with keywords from this batch
        for text in texts:
            keywords = text.split()
            for keyword in keywords:
                cms.add(keyword)
        
        # Save CMS metrics for some example keywords
        example_keywords = ['sports', 'politics', 'tech', 'music', 'news']
        cms_data = [Row(
            batch_id=batch_id,
            timestamp=datetime.utcnow(),
            keyword=keyword,
            estimated_count=cms.count(keyword)
        ) for keyword in example_keywords]
        
        if cms_data:
            cms_metrics_df = spark.createDataFrame(cms_data)
            
            # Debug before writing to cms_stream
            print("\nMetrics DataFrame to be written to cms_stream:")
            cms_metrics_df.show()
            
            print("\nAttempting to write to cms_stream table...")
            cms_metrics_df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}") \
                .option("dbtable", 'cms_stream') \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .mode("append") \
                .save()
            print("Successfully wrote to cms_stream table")
        
        print(f"=== Finished processing Batch {batch_id} ===\n")

    except Exception as e:
        print(f"ERROR processing batch {batch_id}: {str(e)}")
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

# Set up the stream to write to PostgreSQL
query = df_json.writeStream \
    .foreachBatch(write_to_postgres_with_cms) \
    .outputMode("append") \
    .start()

query.awaitTermination()