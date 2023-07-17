# The job reads data from multiple Parquet files in the `data/userdata` folder
# Theses files are read one by one (maxFilesPerTrigger=1) and starting from the earliest to the oldest (latestFirst=True)

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

spark = SparkSession.builder.appName("2_read_parquet_files").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = (spark.readStream.format("parquet")
    .schema(SCHEMA)
    .option("latestFirst", True)
    .option("maxFilesPerTrigger", "1")
    .load(FILE_PATH))
    
# maxFileAge ignored if latestFirst is set to true and maxFilesPerTrigger is set

(df.writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination())
