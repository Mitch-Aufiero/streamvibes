FROM bitnami/spark:latest


USER root
RUN install_packages python3-pip


COPY requirements.txt /opt/
RUN pip install --upgrade pip && \
    pip install -r /opt/requirements.txt

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.1/hadoop-aws-3.2.1.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar /opt/bitnami/spark/jars/


USER 1001
