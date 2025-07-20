from pyspark.sql.functions import col
df1 = (spark.table("bronze.transactions_bronze")
       .join(spark.table("bronze.users_bronze"), "user_id")
       .groupBy("country")
       .sum("amount")
       .withColumnRenamed("sum(amount)", "total_spending")
       .orderBy(col("total_spending").desc()))
df1.show()