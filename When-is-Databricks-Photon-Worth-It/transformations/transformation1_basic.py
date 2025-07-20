from pyspark.sql.functions import col, year, month

# Transformation 1: Basic enrichment and filtering

users = spark.table("bronze.users_<scale>")
transactions = spark.table("bronze.transactions_<scale>")

joined_df = transactions.join(users, on="user_id", how="left")

silver_df = joined_df \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .filter(col("amount") > 5)

silver_df.write.mode("overwrite").format("delta").saveAsTable("silver.transactions_<scale>_enriched_t1")
