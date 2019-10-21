rem create input topic for user purchases
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic user-purchases

rem create table of user information - log compacted for optimisation
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic user-table --config cleanup.policy=compact

rem create out topic for user purchases enriched with user data (left join)
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic user-purchases-enriched-left-join

rem create out topic for user purchases enriched with user data (inner join)
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic user-purchases-enriched-inner-join

rem start a consumer on the output topic (left join)
bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 ^
    --topic user-purchases-enriched-left-join ^
    --from-beginning ^
    --formatter kafka.tools.DefaultMessageFormatter ^
    --property print.key=true ^
    --property print.value=true ^
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer ^
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer


rem start a consumer on the output topic (inner join)
bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 ^
    --topic user-purchases-enriched-inner-join ^
    --from-beginning ^
    --formatter kafka.tools.DefaultMessageFormatter ^
    --property print.key=true ^
    --property print.value=true ^
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer ^
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
