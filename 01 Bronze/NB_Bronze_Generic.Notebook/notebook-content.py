# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2ec94248-04b1-4b19-aff8-4538a71d156f",
# META       "default_lakehouse_name": "DL_Bronze_Lh",
# META       "default_lakehouse_workspace_id": "ed48cec8-2f0b-4e65-8253-a3592c231aa1",
# META       "known_lakehouses": [
# META         {
# META           "id": "2ec94248-04b1-4b19-aff8-4538a71d156f"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bronze – generic loader
# Reads one CSV from SharePoint shortcut and appends it to a bronze table.

# CELL ********************

from pyspark.sql import functions as F

SOURCE_FOLDER = "Files/e_commerce_demo"

def load_to_bronze(file_name, table_name):
    df = (spark.read
          .option("header", True)
          .option("encoding", "UTF-8")
          .csv(f"{SOURCE_FOLDER}/{file_name}"))

    df = (df
          .withColumn("_source_file", F.lit(file_name))
          .withColumn("_ingested_at", F.current_timestamp()))

    df.write.mode("append").option("mergeSchema", "true").saveAsTable(table_name)
    print(f"{table_name}: +{df.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Import Done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
