from pyspark.sql import SparkSession

db = "mydb"
orders_table = f"{db}.orders"
orders_enriched_table = f"{db}.orders_enriched"

spark = SparkSession.builder.appName("job2").getOrCreate()

# Read table
df = spark.table(orders_table)

# Transform
df2 = df.withColumnRenamed("id", "order_id")

# Write enriched table
df2.write.insertInto(orders_enriched_table)
