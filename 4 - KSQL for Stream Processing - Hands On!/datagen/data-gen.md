
# Usage
```
ksql-datagen schema=./riderequest-europe.avro  format=avro topic=riderequest-europe key=rideid maxInterval=5000 iterations=100

ksql-datagen schema=./riderequest-america.avro format=avro topic=riderequest-america key=rideid maxInterval=5000 iterations=10
```

- overview https://docs.confluent.io/current/ksql/docs/tutorials/generate-custom-test-data.html
- formats see https://github.com/confluentinc/avro-random-generator