from pyspark.sql.functions import expr

# Variation 2: Medium (1M users, 10M transactions)

# Medium Users
users_medium_df = spark.range(1_000_000).withColumnRenamed("id", "user_id") \    .withColumn("name", expr("concat('user_', user_id)")) \    .withColumn("country", expr("CASE WHEN user_id % 3 = 0 THEN 'FR' WHEN user_id % 3 = 1 THEN 'US' ELSE 'UK' END"))

users_medium_df.write.mode("overwrite").format("delta").saveAsTable("bronze.users_medium")

# Medium Transactions
transactions_medium_df = spark.range(10_000_000).withColumnRenamed("id", "transaction_id") \    .withColumn("user_id", expr("cast(rand() * 1000000 as int)")) \    .withColumn("amount", expr("round(rand() * 500, 2)")) \    .withColumn("date", expr("date_add('2024-01-01', cast(rand() * 365 as int))"))

transactions_medium_df.write.mode("overwrite").format("delta").saveAsTable("bronze.transactions_medium")
