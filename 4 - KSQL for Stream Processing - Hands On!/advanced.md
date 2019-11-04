
# Headless
_Headless_ KSQL server cluster is *not* aware of anys streams or tables you defined in other (interactive) KSQL clusters.


```
confluent stop ksql-server 

/opt/confluent/bin/ksql-server-start /opt/confluent/etc/ksql/ksql-server.properties  --queries-file ./where-is-bob.ksql  

# show CLI does not work
ksql

# check if BOB topic exists
kafka-topics --zookeeper localhost:2181 --list --topic BOB

kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic BOB 
```

# Explain
```
create stream my_stream 
as select firstname 
from userprofile; 

show queries;

explain CSAS_MY_STREAM_1;


create table my_table 
as select firstname, count(*) as cnt 
from userprofile 
group by firstname;

show queries;

explain CTAS_MY_TABLE_0;
```

## Kafka Streams Topology Visualizer
_Converts an ASCII Kafka Topology description into a hand drawn diagram._

- See https://zz85.github.io/kafka-streams-viz/


# Multi Server
```
docker-compose  -f docker-compose-prod.yml up -d 
ksql-datagen schema=./datagen/userprofile.avro format=json topic=USERPROFILE key=userid maxInterval=1000 iterations=10000
```

In KSQL
```

CREATE STREAM userprofile (userid INT, firstname VARCHAR, lastname VARCHAR, countrycode VARCHAR, rating DOUBLE) 
  WITH (VALUE_FORMAT = 'JSON', KAFKA_TOPIC = 'USERPROFILE');  

create stream up_lastseen as 
SELECT TIMESTAMPTOSTRING(rowtime, 'dd/MMM HH:mm:ss') as createtime, firstname
from userprofile;  
```
```
kafka-console-consumer --bootstrap-server localhost:9092  --topic UP_LASTSEEN 


docker-compose -f docker-compose-prod.yml ps

# stop 1
docker-compose -f docker-compose-prod.yml stop ksql-server-1

# re-start 1
docker-compose -f docker-compose-prod.yml start ksql-server-1

# stop 2
docker-compose -f docker-compose-prod.yml stop ksql-server-2

# stop 1
docker-compose -f docker-compose-prod.yml stop ksql-server-1

# start 2
docker-compose -f docker-compose-prod.yml start ksql-server-1

# start 1
docker-compose -f docker-compose-prod.yml start ksql-server-1
```

# Admin

## Understanding settings

```
confluent stop
confluent destroy

cd /opt/confluent/etc/ksql
vi ksql-server.properties

# add this line anywhere in file
ksql.service.id=myservicename

confluent start ksql-server
```

Start KSQL 
```

LIST PROPERTIES;

 Property                                               | Default override | Effective Value
---------------------------------------------------------------------------------------------------
 ksql.schema.registry.url                               | SERVER           | http://localhost:8081
 ksql.streams.auto.offset.reset                         | SERVER           | latest
 ksql.service.id                                        | SERVER           | myservicename          <-- *** Note: this is the one we changed

SET 'auto.offset.reset'='earliest';

LIST PROPERTIES;


 Property                                               | Default override | Effective Value
----------------------------------------------------------------------------------------------------
 ksql.schema.registry.url                               | SERVER           | http://localhost:8081
 ksql.streams.auto.offset.reset                         | SESSION          | earliest                <-- *** Note both the override and Value cahnges
 ksql.service.id                                        | SERVER           | myservicename          
```

## State Stores

Start KSQL 
```
LIST PROPERTIES;

# Look for ksql.streams.state.dir

 Property                                               | Default override | Effective Value
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 ksql.streams.state.dir                                 | SERVER           | /var/folders/1p/3whlrkzx4bs3fkd55_600x4c0000gp/T/confluent.V2kB1p2N/ksql-server/data/kafka-streams
```

At UNIX
```
ksql-datagen schema=./datagen/userprofile.avro format=json topic=USERPROFILE key=userid maxInterval=5000 iterations=100
```

At KSQL
```
CREATE STREAM userprofile (userid INT, firstname VARCHAR, lastname VARCHAR, countrycode VARCHAR, rating DOUBLE) \
  WITH (VALUE_FORMAT = 'JSON', KAFKA_TOPIC = 'USERPROFILE');
```

At UNIX
```
# note: this will show nothing
find /var/folders/1p/3whlrkzx4bs3fkd55_600x4c0000gp/T/confluent.V2kB1p2N/ksql-server/data/kafka-streams -type f 
```

Run a stateful operation, which should require RocksDB to persist to disk

```
select countrycode, count(*) from userprofile group by countrycode;
```

At UNIX
```
# note: this will now show files
find /var/folders/1p/3whlrkzx4bs3fkd55_600x4c0000gp/T/confluent.V2kB1p2N/ksql-server/data/kafka-streams -type f 
```






# Complex

```
set 'ksql.sink.partitions' = '1';

kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic userrequests
kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic browsertype
kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic location
kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic cartype

create table browsertype (browser_code bigint, browsername varchar) with (kafka_topic='browsertype', value_format='json', key='browser_code');

create table location (location_code bigint, locationname varchar) with (kafka_topic='location', value_format='json', key='location_code');

create table cartype (car_code bigint, carname varchar) with (kafka_topic='cartype', value_format='json', key='car_code');


create stream userrequests (browser_code bigint, location_code bigint, car_code bigint, silly varchar) with (kafka_topic='userrequests', value_format='json');

create stream user_browser as select us.LOCATION_CODE, us.CAR_CODE, us.silly, bt.browsername from userrequests us left join browsertype bt on bt.browser_code=us.browser_code;


create stream user_browser_location as select ub.CAR_CODE, ub.silly, ub.browsername, l.locationname from user_browser ub left join location l on ub.location_code = l.location_code;

create stream user_browser_location_car as select ubl.silly, ubl.browsername, ubl.locationname, c.carname from user_browser_location ubl left join cartype c on ubl.CAR_CODE = c.car_code;
```