from pyspark.sql import SparkSession

db = "mydb"
metrics_table = f"{db}.metrics_agg"

spark = SparkSession.builder.appName("job3").getOrCreate()

jdbc_table = "analytics.user_metrics"
df = spark.read.format("jdbc").option("url", "jdbc:postgresql://host:port/db").option("dbtable", jdbc_table).load()

df.write.saveAsTable(metrics_table)
