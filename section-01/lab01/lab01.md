# ksql on ksqldb
kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --command-config client.properties --create --topic USERS

ksql> list topics;
ksql> create stream users_stream(name varchar, countrycode varchar) with (kafka_topic='USERS', value_format='DELIMITED');
ksql> select name,countrycode from users_stream;

ksql> set 'auto.offset.reset'='earliest';
ksql> select name,countrycode from users_stream limit 4;

ksql> 
SELECT
    countrycode,
    COUNT(*) AS user_count
FROM users_stream
GROUP BY countrycode
EMIT CHANGES;

**Pull Query와 Push Query 차이**

|구분	|Pull Query|Push Query|
|---|----|---|
|목적	|현재 상태 조회	|실시간 변경 데이터 조회|
|실행 방식	|한 번 실행 후 종료	|지속적으로 실행|
|EMIT CHANGES	|사용하지 않음	|사용|
|GROUP BY 직접 사용	|지원하지 않음	|지원|
|데이터 변경 감지	|불가능	|가능|
|대표 사용 사례	|사용자별 현재 상태 조회	|국가별 실시간 사용자 수 집계|

kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER \
--create --topic USERPROFILE \
--partitions 3 \
--replication-factor 3;


kafka-console-producer.sh   --bootstrap-server "$BOOTSTRAP_SERVER"   \
--producer.config client.properties   \
--topic USERPROFILE << EOF
> {"userid": 1000, "firstname": "Alice", "lastname": "Smith", "countrycode": "GB", "rating": 4.7}
> EOF

ksql> create stream userprofile (userid INT, firstname VARCHAR, lastname VARCHAR, countrycode VARCHAR, rating DOUBLE) \
WITH(VALUE_FORMAT = 'JSON', KAFKA_TOPIC = 'USERPROFILE');

ksql> list streams;
ksql> describe userprofile;

ksql> select firstname, lastname, countrycode, rating from userprofile;

ksql-datagen schema=/ksql/userprofile.avro format=json topic='USERPROFILE' key='userid'  iterations=100 bootstrap-server=pkc-921jm.us-east-2.aws.confluent.cloud:9092 propertiesFile=/ksql/client.properties msgRate=5


ksql> create table COUNTRYTABLE(countrycode VARCHAR primary key, countryname VARCHAR) with (KAFKA_TOPIC='COUNTRY-CSV', VALUE_FORMAT='DELIMITED');