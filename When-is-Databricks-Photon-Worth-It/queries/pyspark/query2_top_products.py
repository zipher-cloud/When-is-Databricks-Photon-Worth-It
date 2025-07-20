from pyspark.sql.functions import col
df2 = (spark.table("bronze.transactions_bronze")
       .join(spark.table("bronze.products_bronze"), "product_id")
       .groupBy("product_name")
       .sum("amount")
       .withColumnRenamed("sum(amount)", "total_sales")
       .orderBy(col("total_sales").desc())
       .limit(10))
df2.show()