from pyspark.sql.functions import col, year, month, when, lit, row_number, dense_rank, avg, sum as _sum, count
from pyspark.sql.window import Window

# Transformation 3: Full enrichment with rolling metrics and dense ranks

users = spark.table("bronze.users_<scale>")
transactions = spark.table("bronze.transactions_<scale>")

joined_df = transactions.join(users, on="user_id", how="left")

df = joined_df \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("amount_category", when(col("amount") < 50, "low")
                                     .when(col("amount") < 200, "medium")
                                     .otherwise("high")) \
    .withColumn("transaction_type", when(col("amount") > 400, lit("VIP"))
                                    .when(col("amount") > 200, lit("Regular"))
                                    .otherwise("Basic"))

user_win = Window.partitionBy("user_id").orderBy("date")
cat_win = Window.partitionBy("country", "amount_category").orderBy("date")

df = df \
    .withColumn("txn_count_per_user", count("*").over(user_win)) \
    .withColumn("rolling_avg_last_3", avg("amount").over(user_win.rowsBetween(-2, 0))) \
    .withColumn("rolling_sum_last_3", _sum("amount").over(user_win.rowsBetween(-2, 0))) \
    .withColumn("row_num_by_category", row_number().over(cat_win)) \
    .withColumn("dense_rank_by_category", dense_rank().over(cat_win)) \
    .filter((col("year") >= 2024) & (col("rolling_avg_last_3") > 100))

df.write.mode("overwrite").format("delta").saveAsTable("silver.transactions_<scale>_enriched_t3")
