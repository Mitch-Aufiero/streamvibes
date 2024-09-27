import json
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import window, col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from utils.spark_utils import SparkJob

class KafkaToS3Job(SparkJob):
    def __init__(self, app_name: str, config: dict):
        super().__init__(app_name, config)
        self.kafka_bootstrap_servers = config["kafka"]["bootstrap_servers"]
        self.kafka_topic = config["kafka"]["topic"]
        
        
        self.s3_output_path = f"s3a://{config['s3']['raw_bucket']}"
        
        
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", config["s3"]["endpoint_url"])
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", config["s3"]["access_key"])
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config["s3"]["secret_key"])
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.multipart.size", "104857600")

    def read_from_kafka(self) -> DataFrame:
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("checkpointLocation", self.s3_output_path + "/kafka_checkpoints") \
            .load()

    def process_data(self, df: DataFrame) -> DataFrame:
        schema = StructType([
            StructField("channel", StringType(), True),
            StructField("message", StringType(), True)
        ])
        return df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)") \
            .withColumn("json_data", from_json(col("value"), schema)) \
            .select(col("json_data.channel"), col("json_data.message"), col("timestamp"))

    def write_to_s3(self, df: DataFrame):
        query = df.withColumn("window", window(df["timestamp"], "1 hour")) \
            .writeStream \
            .format("parquet") \
            .option("path", self.s3_output_path)  \
            .option("checkpointLocation", self.s3_output_path + "/_checkpoints") \
            .trigger(processingTime='10 seconds') \
            .partitionBy('window') \
            .start()

        query.awaitTermination()

    def run(self):
        kafka_data = self.read_from_kafka()
        processed_data = self.process_data(kafka_data)
        self.write_to_s3(processed_data)


if __name__ == "__main__":
    # Load config.json
    with open("/src/config.json", 'r') as config_file:
        config = json.load(config_file)

    print("Current working dir: ", os.getcwd())

    job = KafkaToS3Job(
        app_name="KafkaToS3Job",
        config=config
    )
    job.run()
