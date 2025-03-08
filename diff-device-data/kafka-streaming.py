from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def main():
    # Create Spark Session
    spark = SparkSession \
        .builder \
        .appName("KafkaToConsoleStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("Spark session created")

    # Define schema for each type of data
    video_schema = StructType([
        StructField("video_id", StringType(), True),
        StructField("file_size", IntegerType(), True),
        StructField("duration", IntegerType(), True),
        StructField("format", StringType(), True),
        StructField("resolution", StringType(), True),
        StructField("upload_time", StringType(), True),
    ])

    photo_schema = StructType([
        StructField("photo_id", StringType(), True),
        StructField("file_size", IntegerType(), True),
        StructField("resolution", StringType(), True),
        StructField("format", StringType(), True),
        StructField("upload_time", StringType(), True),
        StructField("image_data", StringType(), True),  # Base64 encoded image
    ])

    iot_schema = StructType([
        StructField("device_id", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("status", StringType(), True),
        StructField("location", StringType(), True),
    ])

    # Read streaming data from Kafka for different topics
    print("Connecting to Kafka for video topic...")
    kafka_df_video = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "video_topic") \
        .option("startingOffsets", "earliest") \
        .load()
    print("Connected to Kafka video topic")

    print("Connecting to Kafka for photo topic...")
    kafka_df_photo = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "photo_topic") \
        .option("startingOffsets", "earliest") \
        .load()
    print("Connected to Kafka photo topic")

    print("Connecting to Kafka for IoT topic...")
    kafka_df_iot = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "iot_topic") \
        .option("startingOffsets", "earliest") \
        .load()
    print("Connected to Kafka IoT topic")

    # Parse and process each type of data
    print("Parsing video data...")
    parsed_video_df = kafka_df_video \
        .select(from_json(col("value").cast("string"), video_schema).alias("data")) \
        .select("data.*") \
        .withColumn("processing_time", current_timestamp())
    print("Video data parsed successfully")

    print("Parsing photo data...")
    parsed_photo_df = kafka_df_photo \
        .select(from_json(col("value").cast("string"), photo_schema).alias("data")) \
        .select("data.*") \
        .withColumn("processing_time", current_timestamp())
    print("Photo data parsed successfully")

    print("Parsing IoT data...")
    parsed_iot_df = kafka_df_iot \
        .select(from_json(col("value").cast("string"), iot_schema).alias("data")) \
        .select("data.*") \
        .withColumn("processing_time", current_timestamp())
    print("IoT data parsed successfully")

    # Write data to the console
    print("Starting video data stream to console...")
    video_stream_query = parsed_video_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()
    print("Video data stream to console started")

    print("Starting photo data stream to console...")
    photo_stream_query = parsed_photo_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()
    print("Photo data stream to console started")

    print("Starting IoT data stream to console...")
    iot_stream_query = parsed_iot_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()
    print("IoT data stream to console started")

    # Await termination
    print("Awaiting termination of streaming jobs...")
    video_stream_query.awaitTermination()
    photo_stream_query.awaitTermination()
    iot_stream_query.awaitTermination()
    print("Streaming jobs terminated.")

if __name__ == "__main__":
    main()
