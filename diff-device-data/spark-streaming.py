from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def main():
    try:
        # Create Spark Session
        spark = SparkSession \
            .builder \
            .appName("KafkaToS3Streaming") \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                    "org.apache.hadoop:hadoop-aws:3.3.1") \
            .config("spark.hadoop.fs.s3a.access.key", "*") \
            .config("spark.hadoop.fs.s3a.secret.key", "*") \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
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
            StructField("upload_time", StringType(), True)
        ])

        photo_schema = StructType([
            StructField("photo_id", StringType(), True),
            StructField("file_size", IntegerType(), True),
            StructField("resolution", StringType(), True),
            StructField("format", StringType(), True),
            StructField("upload_time", StringType(), True),
            StructField("image_data", StringType(), True)  # Base64 encoded image
        ])

        iot_schema = StructType([
            StructField("device_id", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("status", StringType(), True),
            StructField("location", StringType(), True)
        ])

        # Read streaming data from Kafka for different topics
        def read_kafka_topic(topic):
            print(f"Connecting to Kafka for topic: {topic}...")
            return spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", topic) \
                .option("startingOffsets", "earliest") \
                .load()

        kafka_df_video = read_kafka_topic("video_topic")
        kafka_df_photo = read_kafka_topic("photo_topic")
        kafka_df_iot = read_kafka_topic("iot_topic")

        # Parse and process each type of data
        def parse_kafka_data(kafka_df, schema):
            return kafka_df \
                .select(from_json(col("value").cast("string"), schema).alias("data")) \
                .select("data.*") \
                .withColumn("processing_time", current_timestamp())

        parsed_video_df = parse_kafka_data(kafka_df_video, video_schema)
        parsed_photo_df = parse_kafka_data(kafka_df_photo, photo_schema)
        parsed_iot_df = parse_kafka_data(kafka_df_iot, iot_schema)

        print("Data parsing completed successfully")

        # Define S3 output paths for different data types
        s3_paths = {
            "video": "s3a://apple-data2019/streamed_video/",
            "photo": "s3a://apple-data2019/streamed_photo/",
            "iot": "s3a://apple-data2019/streamed_iot/"
        }

        checkpoint_paths = {
            "video": "s3a://apple-data2019/checkpoints/video/",
            "photo": "s3a://apple-data2019/checkpoints/photo/",
            "iot": "s3a://apple-data2019/checkpoints/iot/"
        }

        # Write data to S3
        def write_to_s3(df, data_type):
            return df.writeStream \
                .format("parquet") \
                .option("path", s3_paths[data_type]) \
                .option("checkpointLocation", checkpoint_paths[data_type]) \
                .outputMode("append") \
                .start()

        video_s3_query = write_to_s3(parsed_video_df, "video")
        photo_s3_query = write_to_s3(parsed_photo_df, "photo")
        iot_s3_query = write_to_s3(parsed_iot_df, "iot")

        # Write data to the console (separate streams)
        def write_to_console(df, name):
            return df.writeStream \
                .format("console") \
                .outputMode("append") \
                .start()

        video_console_query = write_to_console(parsed_video_df, "video")
        photo_console_query = write_to_console(parsed_photo_df, "photo")
        iot_console_query = write_to_console(parsed_iot_df, "iot")

        # Await termination for all queries
        print("Awaiting termination of streaming jobs...")
        video_s3_query.awaitTermination()
        photo_s3_query.awaitTermination()
        iot_s3_query.awaitTermination()
        video_console_query.awaitTermination()
        photo_console_query.awaitTermination()
        iot_console_query.awaitTermination()

    except Exception as e:
        print(f"Error encountered: {e}")

if __name__ == "__main__":
    main()
