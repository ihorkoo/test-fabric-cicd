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

# # Gold – dim_product

# CELL ********************

from pyspark.sql import functions as F

df = (spark.read.table("DL_Silver_Lh.dbo.silver_sales")
      .select("product_id", "product_name", "category")
      .dropDuplicates(["product_id"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(df.write.mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable("dbo.gold_dim_product"))

display(spark.read.table("dbo.gold_dim_product").limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
