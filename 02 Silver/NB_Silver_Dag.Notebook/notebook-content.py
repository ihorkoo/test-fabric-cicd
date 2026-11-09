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

# # Silver – orchestration
# Runs bronze load first, then both silver notebooks in parallel.
# Each silver notebook cleans and casts data, keeps the latest row per key and upserts (MERGE) into a silver table.

# CELL ********************

dag = {
    "activities": [
        {
            "name": "silver_sales",
            "path": "NB_Silver_Sales",
            "timeoutPerCellInSeconds": 600,
            "retry": 1,
            "retryIntervalInSeconds": 30,
            "dependencies": []
        },
        {
            "name": "silver_payments",
            "path": "NB_Silver_Payments",
            "timeoutPerCellInSeconds": 600,
            "retry": 1,
            "retryIntervalInSeconds": 30,
            "dependencies": []
        }
    ],
    "timeoutInSeconds": 3600,
    "concurrency": 2
}

notebookutils.notebook.runMultiple(dag, {"displayDAGViaGraphviz": True})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
