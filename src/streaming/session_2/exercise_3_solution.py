# Exercise 3. Calculate how many people have registered by country in the last 24 hours (Delta Lake version)

# Some assumptions:
# * Duplicated data (`id` is unique) may arrive from the data source (Kafka topic filled by us) due to implementation issues from other company teams.
# * Old data (`registration_dttm` older than 24 hours) does not matter.
# * If the process fails, the data must be processed from where the process left off

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from delta.tables import DeltaTable

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

SINK_PATH = "/data/exercise_3"


spark = (SparkSession.builder.appName("exercise_3")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "earliest")
      .load())

# Solutions:

def process_batch(spark_session: SparkSession, new_data: DataFrame):
    new_data.show()

    if not DeltaTable.isDeltaTable(spark_session, SINK_PATH):
        new_data.write.format("delta").mode("errorifexists").save(SINK_PATH)
    else:
        old_data = DeltaTable.forPath(spark_session, SINK_PATH)
        (old_data.alias("old_data")
         .merge(
            new_data.alias("new_data"),
            'old_data.country = new_data.country')
         .whenNotMatchedInsertAll()
         .whenMatchedUpdateAll()
         .execute())
        
    all_data = DeltaTable.forPath(spark_session, SINK_PATH)
    all_data.toDF().show()


(df.select(
    F.from_json(
        F.decode(F.col("value"), "iso-8859-1"),
        SCHEMA
    ).alias("value")
)
    .select("value.*")
    .withColumn("registration_ts", F.unix_timestamp(F.col("registration_dttm"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
    .filter(F.col("registration_ts") >= (F.current_timestamp() - F.expr("INTERVAL 1 DAY")))
    .dropDuplicates(["id"])
    .groupBy("country")
    .agg(F.count("*").alias("count_by_country"))
    .select(F.col("country"), F.col("count_by_country"))
    .writeStream
    .outputMode("update")
    .option("checkpointLocation", "/data/checkpoints/exercise_3")
    .foreachBatch(lambda batch, epochId: process_batch(spark, batch))
    .trigger(processingTime="10 seconds")
    .start()
    .awaitTermination())
