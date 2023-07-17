# Exercise 2. Enrich sales information from stream with user information from Parquet files

# Some assumptions:
# * There will be no duplicate data.
# * If the user does not exist, the sale information must be printed through the console.
# * If the process fails, the data must be processed from where the process left off

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "sales_topic"
USERS_FILE_PATH = "/data/userdata/"

USERS_SCHEMA = StructType([
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

spark = SparkSession.builder.appName("exercise_2").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "earliest")
      .load())

# Solutions:

