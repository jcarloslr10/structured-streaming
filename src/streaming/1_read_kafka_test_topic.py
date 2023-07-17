# The job reads key:value data from a Kafka topic called test_topic
# You have to access to the Kafka container to publish data to test_topic for this job to process
# and output the result through the console

from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "test_topic"

spark = SparkSession.builder.appName("1_read_test_topic").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load())

(df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination())

# Use checkpoints to check how not to process the data over and over again
