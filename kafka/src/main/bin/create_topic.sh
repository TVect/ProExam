
./bin/kafka-topics.sh --zookeeper zookeeper0:12181 --create --topic items --partitions 2 --replication-factor 2    

./bin/kafka-topics.sh --zookeeper zookeeper0:12181 --create --topic users --partitions 2 --replication-factor 2    

./bin/kafka-topics.sh --zookeeper zookeeper0:12181 --create --topic orders --partitions 2 --replication-factor 2
