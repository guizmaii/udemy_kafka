#!/bin/zsh

# create input topic with one partition to get full ordering
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bank-transactions

# create output log compacted topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bank-balance-exactly-once --config cleanup.policy=compact

# launch a Kafka consumer
kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic bank-balance-exactly-once \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
