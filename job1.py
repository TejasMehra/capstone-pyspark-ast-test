from pyspark.sql import SparkSession

db = "mydb"
orders_table = f"{db}.orders"
completed_summary_table = f"{db}.completed_summary"

spark = SparkSession.builder.appName("job1").getOrCreate()

# Read table
orders_df = spark.read.table(orders_table)

# Filter
completed_df = orders_df.filter("status = 'COMPLETED'")

# Write result
completed_df.write.saveAsTable(completed_summary_table)
