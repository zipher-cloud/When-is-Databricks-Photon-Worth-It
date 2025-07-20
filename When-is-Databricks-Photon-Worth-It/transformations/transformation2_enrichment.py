from pyspark.sql.functions import col, year, month, when, avg, count, sum as _sum, rank
from pyspark.sql.window import Window

# Transformation 2: Enrichment with categorization and windowing

users = spark.table("bronze.users_<scale>")
transactions = spark.table("bronze.transactions_<scale>")

joined_df = transactions.join(users, on="user_id", how="left")

df = joined_df \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("amount_category", when(col("amount") < 50, "low")
                                     .when(col("amount") < 200, "medium")
                                     .otherwise("high"))

user_win = Window.partitionBy("user_id")
country_win = Window.partitionBy("country").orderBy(_sum("amount").desc())

df = df \
    .withColumn("avg_amount_per_user", avg("amount").over(user_win)) \
    .withColumn("total_transactions_per_user", count("*").over(user_win)) \
    .withColumn("rank_in_country_by_amount", rank().over(country_win)) \
    .filter(col("country").isin("US", "FR")) \
    .filter(col("total_transactions_per_user") > 3)

df.write.mode("overwrite").format("delta").saveAsTable("silver.transactions_<scale>_enriched_t2")
