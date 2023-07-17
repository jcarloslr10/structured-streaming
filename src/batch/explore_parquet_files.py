from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "users_topic"
FILE_PATH = "/data/userdata/"

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

spark = SparkSession.builder.appName("explore_parquet_files").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df1 = (spark.read.format("parquet")
    .schema(SCHEMA)
    .load(FILE_PATH))

df1.show(50)

(df1.coalesce(1)
    .write
    .format('json')
    .save('/data/output/userdata'))

df2 = (spark.read.format("parquet")
    .schema(SCHEMA)
    .load(FILE_PATH)
    .select(F.count("id").alias("count_id"),
            F.min("registration_dttm").alias("min_registration_dttm"),
            F.max("registration_dttm").alias("max_registration_dttm"),
            F.min("salary").alias("min_salary"),
            F.max("salary").alias("max_salary")))

df2.show()
