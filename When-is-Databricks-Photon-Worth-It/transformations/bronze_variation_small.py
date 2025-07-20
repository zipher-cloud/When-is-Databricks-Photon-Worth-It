from pyspark.sql.functions import expr

# Variation 1: Small (100K users, 1M transactions)

# Small Users
users_small_df = spark.range(100_000).withColumnRenamed("id", "user_id") \    .withColumn("name", expr("concat('user_', user_id)")) \    .withColumn("country", expr("CASE WHEN user_id % 3 = 0 THEN 'FR' WHEN user_id % 3 = 1 THEN 'US' ELSE 'UK' END"))

# Save to bronze
users_small_df.write.mode("overwrite").format("delta").saveAsTable("bronze.users_small")

# Small Transactions
transactions_small_df = spark.range(1_000_000).withColumnRenamed("id", "transaction_id") \    .withColumn("user_id", expr("cast(rand() * 100000 as int)")) \    .withColumn("amount", expr("round(rand() * 500, 2)")) \    .withColumn("date", expr("date_add('2024-01-01', cast(rand() * 365 as int))"))

# Save to bronze
transactions_small_df.write.mode("overwrite").format("delta").saveAsTable("bronze.transactions_small")
