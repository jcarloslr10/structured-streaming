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

# * Filter by `registration_dttm` + drop duplicates + aggregate + complete output mode.
# ----------------------------------------------------------------------------------------

# (df.select(
#     F.from_json(
#         F.decode(F.col("value"), "iso-8859-1"),
#         SCHEMA
#     ).alias("value")
# )
#     .select("value.*")
#     .withColumn("registration_ts", F.unix_timestamp(F.col('registration_dttm'), "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
#     .filter(F.col("registration_ts") >= (F.current_timestamp() - F.expr('INTERVAL 1 DAY')))
#     .dropDuplicates(["id"])
#     .groupBy("country")
#     .agg(F.count("*").alias("count_by_country"))
#     .select(F.col("country"), F.col("count_by_country"))
#     .writeStream
#     .outputMode("complete")
#     .format("console")
#     .start()
#     .awaitTermination())

# Why this solution is not correct? What does it happen with complete mode and data aggregated in the state?

# * Filter by `registration_dttm` + watermarking + foreachbatch + drop duplicates + aggregate + update output mode.
# ----------------------------------------------------------------------------------------

# AGG_SCHEMA = StructType([
#     StructField("country", StringType()),
#     StructField("count_by_country", DoubleType())
# ])


def foreach_batch_function(batch_df, epoch_id):
    batch_df.show()

    # agg_df = (spark.read.format("parquet")
    #           .schema(AGG_SCHEMA)
    #           .load("/data/exercise_1"))

    # appended_df = (agg_df.union(batch_df)
    #                .groupBy("country")
    #                .agg(F.max("count_by_country").alias("count_by_country"))
    #                .select(F.col("country"), F.col("count_by_country")))

    # appended_df.show()

    # (appended_df
    #  .write
    #  .mode("overwrite")
    #  .format("parquet")
    #  #  .option("partitionOverwriteMode", "dynamic")
    #  .save("/data/exercise_1"))

    # spark.catalog.refreshByPath("/data/exercise_1")


# (df.select(
#     F.from_json(
#         F.decode(F.col("value"), "iso-8859-1"),
#         SCHEMA
#     ).alias("value")
# )
#     .select("value.*")
#     .withColumn("registration_ts", F.unix_timestamp(F.col('registration_dttm'), "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
#     .withWatermark("registration_ts", "24 hours")
#     .dropDuplicates(["id"])
#     .groupBy(F.window("registration_ts", "24 hours"), "country")
#     .agg(F.count("*").alias("count_by_country"))
#     .select(F.col("country"), F.col("count_by_country"))
#     .writeStream
#     .outputMode("update")
#     .option('checkpointLocation', "/data/checkpoints/exercise_1")
#     .foreachBatch(foreach_batch_function)
#     .start()
#     .awaitTermination())
