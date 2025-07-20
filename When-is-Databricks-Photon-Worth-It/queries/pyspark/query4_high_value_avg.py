from pyspark.sql.functions import col
df4 = (spark.table("bronze.transactions_bronze")
       .filter(col("amount") > 500)
       .join(spark.table("bronze.products_bronze"), "product_id")
       .groupBy("category")
       .avg("amount")
       .withColumnRenamed("avg(amount)", "avg_high_value_amount")
       .orderBy(col("avg_high_value_amount").desc()))
df4.show()