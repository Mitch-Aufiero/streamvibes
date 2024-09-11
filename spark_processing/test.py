from utils.spark_utils import SparkJob

class TestJob(SparkJob):
    def __init__(self):
        """Initialize with TestJob-specific configurations."""
        super().__init__("TestJob")

    def run(self):
        """Run a test Spark job."""
        # Example data processing logic
        data = self.spark.createDataFrame([("John", 25), ("Jane", 30)], ["name", "age"])
        print("Initial data:")
        data.show()

        # Save to S3 (simulated path)
        self.write_s3_data(data, "/opt/spark-data/output.parquet")


        # Stop the Spark session
        self.stop_spark()

# Simulate running the TestJob
if __name__ == "__main__":
    job = TestJob()
    job.run()
