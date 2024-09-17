#setup network
docker network create shared_network


#get running containers
docker ps 

#run kafka to s3
docker exec -it spark_master /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 /src/kafka_to_s3.py

#verify network connection
docker exec -it spark_master /bin/bash
python /src/check-connect.py