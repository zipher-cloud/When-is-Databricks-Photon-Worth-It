# SCALE FACTOR C (~10M users, ~1M products, ~5B transactions)
# Users
users_bronze_scale_c = (
    spark.range(10_000_000)
    .withColumnRenamed("id", "user_id")
    .withColumn("user_name", expr("concat('user_', user_id)"))
    .withColumn("country", expr("""
        CASE WHEN user_id % 5 = 0 THEN 'FR'
             WHEN user_id % 5 = 1 THEN 'US'
             WHEN user_id % 5 = 2 THEN 'UK'
             WHEN user_id % 5 = 3 THEN 'DE'
             ELSE 'CA' END
    """))
)
users_bronze_scale_c.write.mode("overwrite").format("delta").saveAsTable("bronze.users_bronze_scale_c")

# Products
products_bronze_scale_c = (
    spark.range(1_000_000)
    .withColumnRenamed("id", "product_id")
    .withColumn("product_name", expr("concat('product_', product_id)"))
    .withColumn("category", expr("""
        CASE WHEN product_id % 4 = 0 THEN 'Electronics'
             WHEN product_id % 4 = 1 THEN 'Clothing'
             WHEN product_id % 4 = 2 THEN 'Home'
             ELSE 'Books' END
    """))
)
products_bronze_scale_c.write.mode("overwrite").format("delta").saveAsTable("bronze.products_bronze_scale_c")

# Transactions
transactions_bronze_scale_c = (
    spark.range(5_000_000_000)
    .withColumnRenamed("id", "transaction_id")
    .withColumn("user_id", (rand() * 10_000_000).cast("long"))
    .withColumn("product_id", (rand() * 1_000_000).cast("long"))
    .withColumn("amount", round(rand() * 1000, 2))
    .withColumn("transaction_date", expr("date_add('2022-01-01', cast(rand() * 1000 as int))"))
)
transactions_bronze_scale_c.write.mode("overwrite").format("delta").saveAsTable("bronze.transactions_bronze_scale_c")
