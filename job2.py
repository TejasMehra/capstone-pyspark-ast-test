from pyspark.sql import SparkSession

db = "mydb"
orders_table = f"{db}.orders"
orders_enriched_table = f"{db}.orders_enriched"

spark = SparkSession.builder.appName("job2").getOrCreate()

df = spark.table(orders_table)

df2 = df.withColumnRenamed("id", "order_id")

df2.write.insertInto(orders_enriched_table)
