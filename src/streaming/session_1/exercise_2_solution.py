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

# STREAM_SCHEMA = StructType([
#     StructField("sale_dt", StringType()),
#     StructField("price", IntegerType()),
#     StructField("user_id", IntegerType())
# ])


# def foreach_batch_function(batch_df, epoch_id):
#     batch_df.show()

#     sale_users_not_found = batch_df.filter(F.isnull(F.col("id")))
#     sale_users_not_found.show()


# users_df = (spark.read.format("parquet")
#             .schema(USERS_SCHEMA)
#             .load(USERS_FILE_PATH))

# (df.select(
#     F.from_json(
#         F.decode(F.col("value"), "iso-8859-1"),
#         STREAM_SCHEMA
#     ).alias("value")
# )
#     .select("value.*")
#     .withColumn("sale_ts", F.unix_timestamp(F.col('sale_dt'), "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
#     .join(users_df, df["user_id"] == users_df["id"], "left")
#     .writeStream
#     .outputMode("append")
#     .option('checkpointLocation', "/data/checkpoints/exercise_2")
#     .foreachBatch(foreach_batch_function)
#     .start()
#     .awaitTermination())
