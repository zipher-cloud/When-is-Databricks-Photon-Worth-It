from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
users_tx = (spark.table("bronze.transactions_bronze")
    .join(spark.table("bronze.users_bronze"), "user_id")
    .groupBy("country", "user_id")
    .sum("amount")
    .withColumnRenamed("sum(amount)", "total_spending"))
window_spec = Window.partitionBy("country").orderBy(col("total_spending").desc())
df3 = users_tx.withColumn("spending_rank", rank().over(window_spec)).filter(col("spending_rank") <= 5)
df3.show()