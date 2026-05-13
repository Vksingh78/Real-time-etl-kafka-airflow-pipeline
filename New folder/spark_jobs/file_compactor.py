from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FileCompactor").getOrCreate()
df = spark.read.json("/topics/cdr-events/*/*.json")
df.write.mode("overwrite").parquet("/silver/cdr_compacted")
print("Compaction done")