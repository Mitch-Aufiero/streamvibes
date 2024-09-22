import json
from utils.spark_utils import SparkJob
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class SentimentAnalysisJob(SparkJob):
    def __init__(self, app_name: str, config: dict):
        super().__init__(app_name,config)
        self.s3_input_path = config['s3']['raw_bucket']
        self.s3_output_path = config['s3']['processed_bucket']

    def add_sentiment_column(self, df):
        analyzer = SentimentIntensityAnalyzer()

        def analyze_sentiment(message):
            scores = analyzer.polarity_scores(message)
            return float(scores['compound'])

        sentiment_udf = udf(analyze_sentiment, FloatType())

        return df.withColumn("sentiment_score", sentiment_udf(col("message")))

    def print_data(self, df):
        df.show(truncate=False)


    def run(self):
        original_data = self.load_s3_data(self.s3_input_path)

        processed_data = self.add_sentiment_column(original_data)

        self.print_data(processed_data)

        self.write_s3_data(processed_data, self.s3_output_path)
if __name__ == "__main__":
    # Load config.json
    with open("/src/config.json", 'r') as config_file:
        config = json.load(config_file)

    # Initialize and run the job
    job = SentimentAnalysisJob(
        'SentimentAnalysis',
        config
    )
    job.run()
