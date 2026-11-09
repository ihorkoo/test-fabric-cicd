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

# # Gold – orchestration (star schema)
# Builds dimensions first (customer, product, date) in parallel, then fact tables.
# fact_sales: one row per product in an order. fact_payments: one row per payment.
# Tables are fully overwritten on each run.

# CELL ********************

dag = {
    "activities": [
        {
            "name": "dim_customer",
            "path": "NB_Gold_Dim_Customer",
            "timeoutPerCellInSeconds": 600,
            "dependencies": []
        },
        {
            "name": "dim_product",
            "path": "NB_Gold_Dim_Product",
            "timeoutPerCellInSeconds": 600,
            "dependencies": []
        },
        {
            "name": "dim_date",
            "path": "NB_Gold_Dim_Date",
            "timeoutPerCellInSeconds": 600,
            "dependencies": []
        },
        {
            "name": "fact_sales",
            "path": "NB_Gold_Fact_Sales",
            "timeoutPerCellInSeconds": 600,
            "retry": 1,
            "retryIntervalInSeconds": 30,
            "dependencies": ["dim_customer", "dim_product", "dim_date"]
        },
        {
            "name": "fact_payments",
            "path": "NB_Gold_Fact_Payments",
            "timeoutPerCellInSeconds": 600,
            "retry": 1,
            "retryIntervalInSeconds": 30,
            "dependencies": ["dim_customer", "dim_date"]
        }
    ],
    "timeoutInSeconds": 3600,
    "concurrency": 3
}

notebookutils.notebook.runMultiple(dag, {"displayDAGViaGraphviz": True})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
