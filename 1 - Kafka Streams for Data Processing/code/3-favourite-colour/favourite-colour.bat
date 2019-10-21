rem create input topic with one partition to get full ordering
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic favourite-colour-input

rem create intermediary log compacted topic
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic user-keys-and-colours --config cleanup.policy=compact

rem create output log compacted topic
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic favourite-colour-output --config cleanup.policy=compact


rem launch a Kafka consumer
bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 ^
    --topic favourite-colour-output ^
    --from-beginning ^
    --formatter kafka.tools.DefaultMessageFormatter ^
    --property print.key=true ^
    --property print.value=true ^
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer ^
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

rem launch the streams application

rem then produce data to it
bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic favourite-colour-input
rem
stephane,blue
john,green
stephane,red
alice,red


rem list all topics that we have in Kafka (so we can observe the internal topics)
bin\windows\kafka-topics.bat --list --zookeeper localhost:2181
