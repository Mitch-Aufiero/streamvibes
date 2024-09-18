import json
from pyspark.sql import SparkSession, DataFrame

class SparkJob:
    def __init__(self, app_name: str, config: dict):
        """
        Initialize the Spark session and load the S3 configuration from a file.

        Parameters:
        app_name (str): The name of the Spark application.
        config (dict): configuration dict that contains S3 and other settings.
        """
        self.app_name = app_name
        self.config = config
        self.spark = self.initialize_spark_session()

    def load_config(self, config_file: str) -> dict:
        """
        Load configuration from a JSON file.

        Parameters:
        config_file (str): Path to the JSON configuration file.

        Returns:
        dict: Configuration data loaded from the file.
        """
        with open(config_file, 'r') as f:
            return json.load(f)

    def initialize_spark_session(self) -> SparkSession:
        """
        Create and return a Spark session with S3 configuration.

        The S3 settings are loaded from the config file.

        Returns:
        SparkSession: A Spark session instance.
        """
        
        return SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.hadoop.fs.s3a.access.key", self.config['s3']['access_key']) \
            .config("spark.hadoop.fs.s3a.secret.key", self.config['s3']['secret_key']) \
            .config("spark.hadoop.fs.s3a.endpoint", self.config['s3']['endpoint_url']) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
            .getOrCreate()

    def load_s3_data(self, path: str) -> DataFrame:
        """
        Load data from a specified S3 path (Parquet format).

        Parameters:
        path (str): The S3 path where the Parquet file is stored.

        Returns:
        DataFrame: A Spark DataFrame containing the data from the specified S3 file path.
        """
        full_path = f"s3a://{self.config['s3']['bucket']}/{path}"
        return self.spark.read.parquet(full_path)

    def write_s3_data(self, df: DataFrame, path: str):
        """
        Write a Spark DataFrame to a specified S3 path in Parquet format.

        Parameters:
        df (DataFrame): The Spark DataFrame to write.
        path (str): The S3 path where the Parquet file should be stored.

        Returns:
        None
        """
        full_path = f"s3a://{self.config['s3']['bucket']}/{path}"
        df.write.parquet(full_path)

    def stop_spark(self) -> None:
        """
        Stop the Spark session.

        Returns:
        None
        """
        self.spark.stop()
