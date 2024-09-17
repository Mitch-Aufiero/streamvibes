FROM bitnami/spark:latest


USER root
RUN install_packages python3-pip


COPY ./spark/requirements.txt /opt/
RUN pip install --upgrade pip && \
    pip install -r /opt/requirements.txt

# add aws sdk for s3
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.1/hadoop-aws-3.2.1.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar /opt/bitnami/spark/jars/


# Add Kafka connector for Spark
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.2/spark-sql-kafka-0-10_2.12-3.3.2.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.7.0/kafka-clients-2.7.0.jar /opt/bitnami/spark/jars/


USER 1001
