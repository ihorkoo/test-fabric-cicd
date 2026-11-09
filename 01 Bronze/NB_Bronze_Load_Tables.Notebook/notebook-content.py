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
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bronze – load all files
# Calls the generic function for every source file.

# CELL ********************

%run NB_Bronze_Generic

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sources = [
    {"file": "sales.csv",    "table": "bronze_sales"},
    {"file": "payments.csv", "table": "bronze_payments"},
]

for s in sources:
    load_to_bronze(s["file"], s["table"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("""
    SELECT _ingested_at, COUNT(*) AS rows
    FROM bronze_sales
    GROUP BY _ingested_at
    ORDER BY _ingested_at
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
