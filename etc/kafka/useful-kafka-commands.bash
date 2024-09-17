#Get container name and exec into it
docker ps 

docker exec -it etc-kafka-1 /bin/bash 

#list topics and describe
kafka-topics.sh --list --bootstrap-server localhost:9092

kafka-topics.sh --describe --topic twitch_chat --bootstrap-server localhost:9092


#get kafka log file, check the size
cat /opt/kafka/config/server.properties | grep log.dirs

du -sh /kafka/kafka-logs-0dece368c453
