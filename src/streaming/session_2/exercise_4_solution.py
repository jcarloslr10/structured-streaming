# Exercise 4.
# Run the streaming job of the exercise 3.
# Run concurrently a streaming job recording the table changes of the exercise 3 using the CDF in streaming mode and print each change to the console.
# Enable CDF in the code of the exercise 3.

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

CDF_SOURCE_PATH = "/data/exercise_3"

spark = (SparkSession.builder.appName("exercise_4")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# Solutions:

(spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .load(CDF_SOURCE_PATH)
    .writeStream
    .format("console")
    .outputMode("complete")
    .option("checkpointLocation", "/data/checkpoints/exercise_4")
    .start()
    .awaitTermination())
