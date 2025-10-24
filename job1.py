from pyspark.sql import SparkSession

db = "mydb"
orders_table = f"{db}.orders"
completed_summary_table = f"{db}.completed_summary"

spark = SparkSession.builder.appName("job1").getOrCreate()

orders_df = spark.read.table(orders_table)

completed_df = orders_df.filter("status = 'COMPLETED'")

completed_df.write.saveAsTable(completed_summary_table)
