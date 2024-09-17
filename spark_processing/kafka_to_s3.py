from pyspark.sql import DataFrame
from pyspark.sql.functions import window, col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from utils.spark_utils import SparkJob

class KafkaToS3Job(SparkJob):
    def __init__(self, app_name: str, kafka_bootstrap_servers: str, kafka_topic: str, s3_output_path: str, s3_endpoint_url: str, s3_access_key: str, s3_secret_key: str):
        super().__init__(app_name)
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.s3_output_path = s3_output_path
        
        # Set Spark configurations for DigitalOcean Spaces (S3-compatible storage)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", s3_endpoint_url)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", s3_access_key)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", s3_secret_key)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")  # Use path-style access for S3 API compatibility
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.multipart.size", "104857600")  # Optional: Tune for large file sizes

    def read_from_kafka(self) -> DataFrame:
        """
        Reads data from a Kafka topic.

        :return: A DataFrame with the streamed Kafka data.
        """
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "earliest") \
            .option("checkpointLocation", self.s3_output_path + "/kafka_checkpoints") \
            .load()

    def process_data(self, df: DataFrame) -> DataFrame:
        """
        Process Kafka data, extracting the value field and adding a timestamp.

        :param df: Input DataFrame from Kafka
        :return: Processed DataFrame with extracted fields and a timestamp column
        """
        schema = StructType([
            StructField("channel", StringType(), True),
            StructField("message", StringType(), True)
        ])

        return df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)") \
            .withColumn("json_data", from_json(col("value"), schema)) \
            .select(col("json_data.channel"), col("json_data.message"), col("timestamp"))

    def write_to_s3(self, df: DataFrame):
        """
        Write DataFrame to DigitalOcean Spaces in Parquet format, partitioned by an hourly window.
        
        :param df: DataFrame containing Kafka data.
        """
        query = df.withColumn("window", window(df["timestamp"], "1 hour")) \
            .writeStream \
            .format("parquet") \
            .option("path", self.s3_output_path) \
            .option("checkpointLocation", self.s3_output_path + "/_checkpoints") \
            .trigger(processingTime='1 minute') \
            .partitionBy('window') \
            .start()

        query.awaitTermination()

    def run(self):
        """
        Main execution method for the job: reads from Kafka, processes, and writes to DigitalOcean Spaces.
        """
        kafka_data = self.read_from_kafka()
        processed_data = self.process_data(kafka_data)
        self.write_to_s3(processed_data)


if __name__ == "__main__":
    
    kafka_bootstrap_servers = "kafka:9092"
    kafka_topic = "twitch_chat"
    
    # s3 configuration
    s3_output_path = "s3a:"
    s3_endpoint_url = ""
    s3_access_key = ""
    s3_secret_key = ""

    # Initialize and run the Kafka to S3 job
    job = KafkaToS3Job(
        app_name="KafkaToS3Job",
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_topic=kafka_topic,
        s3_output_path=s3_output_path,
        s3_endpoint_url=s3_endpoint_url,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key
    )
    job.run()
