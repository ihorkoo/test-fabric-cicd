# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "14441150-3ffa-42f7-9f82-2b3be2756089",
# META       "default_lakehouse_name": "DL_Silver_Lh",
# META       "default_lakehouse_workspace_id": "ed48cec8-2f0b-4e65-8253-a3592c231aa1",
# META       "known_lakehouses": [
# META         {
# META           "id": "14441150-3ffa-42f7-9f82-2b3be2756089"
# META         },
# META         {
# META           "id": "2ec94248-04b1-4b19-aff8-4538a71d156f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Silver – payments
# Clean, cast types, keep the latest version of each row, upsert.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

SOURCE = "DL_Bronze_Lh.dbo.bronze_payments"
TARGET = "dbo.silver_payments"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze = spark.read.table(SOURCE)

w = Window.partitionBy("payment_id").orderBy(F.col("_ingested_at").desc())

df = (bronze
      .withColumn("rn", F.row_number().over(w))
      .filter("rn = 1")
      .select(
          F.col("payment_id").cast("int"),
          F.col("order_id").cast("int"),
          F.to_date("payment_date").alias("payment_date"),
          F.col("customer_id").cast("int"),
          F.trim("customer_name").alias("customer_name"),
          F.trim("city").alias("city"),
          F.trim("method").alias("method"),
          F.col("amount").cast("decimal(12,2)"),
      )
      .withColumn("_updated_at", F.current_timestamp()))

display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not spark.catalog.tableExists(TARGET):
    df.write.saveAsTable(TARGET)
else:
    (DeltaTable.forName(spark, TARGET).alias("t")
        .merge(df.alias("s"), "t.payment_id = s.payment_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

print(TARGET, spark.read.table(TARGET).count(), "rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
