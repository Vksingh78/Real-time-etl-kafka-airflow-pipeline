from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("CDRStreamProcessor") \
    .config("spark.sql.streaming.checkpointLocation", "./checkpoint") \
    .getOrCreate()

schema = StructType([
    StructField("call_id", StringType()),
    StructField("timestamp", DoubleType()),
    StructField("duration", IntegerType()),
    StructField("cell_id", StringType()),
    StructField("roaming", BooleanType()),
    StructField("caller", StringType())
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "cdr-events") \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

query = parsed.writeStream.outputMode("append").format("console").trigger(processingTime="5 seconds").start()
query.awaitTermination()