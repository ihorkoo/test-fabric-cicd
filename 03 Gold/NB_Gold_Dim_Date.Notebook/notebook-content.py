# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "af7e0715-492e-442e-869f-1d7123b02a4f",
# META       "default_lakehouse_name": "DL_Gold_Lh",
# META       "default_lakehouse_workspace_id": "ed48cec8-2f0b-4e65-8253-a3592c231aa1",
# META       "known_lakehouses": [
# META         {
# META           "id": "af7e0715-492e-442e-869f-1d7123b02a4f"
# META         },
# META         {
# META           "id": "14441150-3ffa-42f7-9f82-2b3be2756089"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold – dim_date
# Full calendar years that cover all dates in silver.

# CELL ********************

from pyspark.sql import functions as F

dates = spark.sql("""
    SELECT explode(sequence(
        make_date(year(MIN(d)), 1, 1),
        make_date(year(MAX(d)), 12, 31),
        interval 1 day)) AS date
    FROM (
        SELECT order_date AS d FROM DL_Silver_Lh.dbo.silver_sales
        UNION ALL
        SELECT payment_date FROM DL_Silver_Lh.dbo.silver_payments
    )
""")

df = dates.select(
    F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
    "date",
    F.year("date").alias("year"),
    F.quarter("date").alias("quarter"),
    F.month("date").alias("month"),
    F.date_format("date", "MMMM").alias("month_name"),
    F.dayofmonth("date").alias("day"),
    F.dayofweek("date").alias("day_of_week"),
    F.dayofweek("date").isin(1, 7).alias("is_weekend"),
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(df.write.mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable("dbo.gold_dim_date"))

display(spark.read.table("dbo.gold_dim_date").limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
