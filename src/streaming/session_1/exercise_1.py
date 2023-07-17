# Exercise 1. Calculate how many people have registered by country in the last 24 hours

# Some assumptions:
# * Duplicated data (`id` is unique) may arrive from the data source (Kafka topic filled by us) due to implementation issues from other company teams.
# * Old data (`registration_dttm` older than 24 hours) does not matter.
# * If the process fails, the data must be processed from where the process left off

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "user_data_topic"

SCHEMA = StructType([
    StructField("registration_dttm", TimestampType()),
    StructField("id", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("gender", StringType()),
    StructField("ip_address", StringType()),
    StructField("cc", StringType()),
    StructField("country", StringType()),
    StructField("birthdate", StringType()),
    StructField("salary", DoubleType()),
    StructField("title", StringType()),
    StructField("comments", StringType())
])

spark = SparkSession.builder.appName("exercise_1").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "earliest")
      .load())

# Solutions:

