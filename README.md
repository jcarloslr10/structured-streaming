# Sample project

## Kafka configuration

### Create Kafka topic to consume messages

Connect to Kafka container terminal:
`docker exec -it <kafka_container_id> bash`

Create the topic:
`kafka-topics.sh --create --replication-factor 1 --bootstrap-server localhost:9092 --topic test_topic`

List topics:
`kafka-topics.sh --list --bootstrap-server localhost:9092`

### Create Kafka console producer to write messages on previous topic

Connect to Kafka container terminal:
`docker exec -it <kafka_container_id> bash`

Create the topic:
`kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test_topic --property "parse.key=true" --property "key.separator=:"`

As of now, every line typed in the terminal will be sent as a message to the test topic. The character ":" is used to separate the message’s key and value (key:value).

## First streaming job consuming Kafka

### Run Spark streaming job

Connect to Spark container terminal:
`docker exec -it <spark_container_id> bash`

Run the job:
`spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /src/streaming/1_read_kafka_test_topic.py`

Go to Kafka container terminal and execute the following statements one per line:
`aaa:100`
`bbb:200`
`ccc:300`

## First streaming job consuming Parquet files

Files named `userdata[1-5].parquet` located in `data\userdata` are sample files containing data in PARQUET format.

These files have the following metadata:
* Number of rows in each file: 1000
* Schema details:

| column | column_name | hive_datatype |
|---|---|---|
| 1 | registration_dttm | timestamp |
| 2 | id | int |
| 3 | first_name | string |
| 4 | last_name | string |
| 5 | email | string |
| 6 | gender | string |
| 7 | ip_address | string |
| 8 | cc | string |
| 9 | country | string |
| 10 | birthdate | string |
| 11 | salary | double |
| 12 | title | string |
| 13 | comments | string |

### Run Spark batch job for exploratory purposes

Connect to Spark container terminal:
`docker exec -it <spark_container_id> bash`

Run the job:
`spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /src/batch/explore_parquet_files.py`

### Run Spark streaming job

Connect to Spark container terminal:
`docker exec -it <spark_container_id> bash`

Run the job:
`spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /src/streaming/2_read_parquet_files.py`

## Session 1

Based on the schema of the sample user data, below is a set of hands-on exercises to get started with Structured Streaming library.

### Exercise 1. Calculate how many people have registered by country in the last 24 hours

Some assumptions:
* Duplicated data (`id` is unique) may arrive from the data source (Kafka topic filled by us) due to implementation issues from other company teams.
* Old data (`registration_dttm` older than 24 hours) does not matter.
* If the process fails, the data must be processed from where the process left off

The result must be shown through the console every time a new batch is processed.

#### Data source
A Kafka topic will be used as a data source where we will publish json data to simulate the data coming into the system to test our job.

Connect to Kafka container terminal:
`docker exec -it <kafka_container_id> bash`

Create the topic:
`kafka-topics.sh --create --replication-factor 1 --bootstrap-server localhost:9092 --topic user_data_topic`

Purge the topic if necessary to drop all messages:
`kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic user_data_topic`

Publish data to the topic (one json object per line):
```
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic user_data_topic
{"registration_dttm":"2023-07-17T09:00:00.000Z","id":1,"first_name":"Amanda","last_name":"Jordan","email":"ajordan0@com.com","gender":"Female","ip_address":"1.197.201.2","cc":"6759521864920116","country":"Indonesia","birthdate":"3/8/1971","salary":49756.53,"title":"Internal Auditor","comments":"1E+02"}
{"registration_dttm":"2023-07-17T08:00:00.000Z","id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
{"registration_dttm":"2023-07-17T07:00:00.000Z","id":3,"first_name":"Evelyn","last_name":"Morgan","email":"emorgan2@altervista.org","gender":"Female","ip_address":"7.161.136.94","cc":"6767119071901597","country":"Russia","birthdate":"2/1/1960","salary":144972.51,"title":"Structural Engineer","comments":""}
{"registration_dttm":"2023-07-17T06:00:00.000Z","id":4,"first_name":"Denise","last_name":"Riley","email":"driley3@gmpg.org","gender":"Female","ip_address":"140.35.109.83","cc":"3576031598965625","country":"China","birthdate":"4/8/1997","salary":90263.05,"title":"Senior Cost Accountant","comments":""}
{"registration_dttm":"2023-07-17T05:00:00.000Z","id":5,"first_name":"Carlos","last_name":"Burns","email":"cburns4@miitbeian.gov.cn","gender":"","ip_address":"169.113.235.40","cc":"5602256255204850","country":"South Africa","birthdate":"","title":"","comments":""}
```

Connect to Spark container terminal:
`docker exec -it <spark_container_id> bash`

Run the job:
`spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /src/streaming/session_1/exercise_1.py`

### Exercise 2. Enrich sales information from stream with user information from Parquet files

Some assumptions:
* There will be no duplicate data.
* If the user does not exist, the sale information must be printed through the console.
* If the process fails, the data must be processed from where the process left off

The result must be shown through the console every time a new batch is processed.

#### Data source
A Kafka topic will be used as a data source where we will publish json data to simulate the data coming into the system to test our job.

Connect to Kafka container terminal:
`docker exec -it <kafka_container_id> bash`

Create the topic:
`kafka-topics.sh --create --replication-factor 1 --bootstrap-server localhost:9092 --topic sales_topic`

Purge the topic if necessary to drop all messages:
`kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic sales_topic`

Publish data to the topic (one json object per line):
```
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sales_topic
{"sale_dt":"2023-07-17T09:00:00.000Z","price":100,"user_id":1}
{"sale_dt":"2023-07-16T09:00:00.000Z","price":200,"user_id":2}
{"sale_dt":"2023-07-15T09:00:00.000Z","price":300,"user_id":3}
{"sale_dt":"2023-07-14T09:00:00.000Z","price":400,"user_id":99999999}
{"sale_dt":"2023-07-13T09:00:00.000Z","price":500,"user_id":-1}
```

Connect to Spark container terminal:
`docker exec -it <spark_container_id> bash`

Run the job:
`spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /src/streaming/session_1/exercise_2.py`

## Session 2

Based on the schema of the sample user data, below is a set of hands-on exercises to get started with Structured Streaming library and Delta Lake format.

### Exercise 3. Calculate how many people have registered by country in the last 24 hours (Delta Lake version)

Some assumptions:
* Duplicated data (`id` is unique) may arrive from the data source (Kafka topic filled by us) due to implementation issues from other company teams.
* Old data (`registration_dttm` older than 24 hours) does not matter.
* If the process fails, the data must be processed from where the process left off

The result must be stored in Delta format every time a new batch is processed.

**Now that you can use Delta, can you think of a way to make the process more efficient using ForeachBatch and something else?**

#### Data source
A Kafka topic will be used as a data source where we will publish json data to simulate the data coming into the system to test our job.

Connect to Kafka container terminal:
`docker exec -it <kafka_container_id> bash`

Create the topic:
`kafka-topics.sh --create --replication-factor 1 --bootstrap-server localhost:9092 --topic user_data_topic`

Purge the topic if necessary to drop all messages:
`kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic user_data_topic`

Publish data to the topic (one json object per line):
```
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic user_data_topic
{"registration_dttm":"2023-07-17T09:00:00.000Z","id":1,"first_name":"Amanda","last_name":"Jordan","email":"ajordan0@com.com","gender":"Female","ip_address":"1.197.201.2","cc":"6759521864920116","country":"Indonesia","birthdate":"3/8/1971","salary":49756.53,"title":"Internal Auditor","comments":"1E+02"}
{"registration_dttm":"2023-07-17T08:00:00.000Z","id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
{"registration_dttm":"2023-07-17T07:00:00.000Z","id":3,"first_name":"Evelyn","last_name":"Morgan","email":"emorgan2@altervista.org","gender":"Female","ip_address":"7.161.136.94","cc":"6767119071901597","country":"Russia","birthdate":"2/1/1960","salary":144972.51,"title":"Structural Engineer","comments":""}
{"registration_dttm":"2023-07-17T06:00:00.000Z","id":4,"first_name":"Denise","last_name":"Riley","email":"driley3@gmpg.org","gender":"Female","ip_address":"140.35.109.83","cc":"3576031598965625","country":"China","birthdate":"4/8/1997","salary":90263.05,"title":"Senior Cost Accountant","comments":""}
{"registration_dttm":"2023-07-17T05:00:00.000Z","id":5,"first_name":"Carlos","last_name":"Burns","email":"cburns4@miitbeian.gov.cn","gender":"","ip_address":"169.113.235.40","cc":"5602256255204850","country":"South Africa","birthdate":"","title":"","comments":""}
```

Connect to Spark container terminal:
`docker exec -it <spark_container_id> bash`

Run the job:
`spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,io.delta:delta-core_2.12:2.3.0 /src/streaming/session_2/exercise_3.py`