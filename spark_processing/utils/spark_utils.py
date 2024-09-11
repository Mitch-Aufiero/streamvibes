from pyspark.sql import SparkSession, DataFrame

class SparkJob:
    def __init__(self, app_name:str):
        """
        Initialize the Spark session with the given application name.

        Parameters:
        app_name (str): The name of the Spark application.
        """
        self.app_name = app_name
        self.spark = self.initialize_spark_session()

    def initialize_spark_session(self) -> SparkSession:
        """
        Create and return a Spark session.

        This method initializes a Spark session using the provided application name.
        You can further configure the Spark session here if needed (e.g., setting memory limits,
        enabling specific Spark configurations, etc.).

        Returns:
        SparkSession: A Spark session instance.
        """
        return SparkSession.builder \
            .appName(self.app_name) \
            .getOrCreate()

    def load_s3_data(self, path:str) -> DataFrame:
        """
        Load data from a specified S3 path.

        This method loads a Parquet file from an S3 path into a Spark DataFrame.

        Parameters:
        path (str): The S3 path where the Parquet file is stored.

        Returns:
        DataFrame: A Spark DataFrame containing the data from the specified S3 file path.
        """
        return self.spark.read.parquet(path)

    def write_s3_data(self, df:DataFrame, path:str):
        """
        Write a Spark DataFrame to a specified S3 path in Parquet format.

        Parameters:
        df (DataFrame): The Spark DataFrame to write.
        path (str): The S3 path where the Parquet file should be stored.

        Returns:
        None
        """
        df.write.parquet(path)

    def stop_spark(self) -> None:
        """
        Stop the Spark session.

        This method terminates the Spark session. It should be called after
        all processing is complete to free up resources.

        Returns:
        None
        """
        self.spark.stop()
