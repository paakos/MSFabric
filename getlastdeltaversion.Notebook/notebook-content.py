# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c8c96811-35e9-4959-ab9d-cde494b191e6",
# META       "default_lakehouse_name": "lkhgold01",
# META       "default_lakehouse_workspace_id": "3c090656-62e2-47f4-8cf2-970e0bd9566c"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

table_name = "dataset1"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

# Path to your Delta table
table_path = "Tables/dbo/"+table_name

# Create a DeltaTable object
deltaTable = DeltaTable.forPath(spark, table_path)

# Get the full history of the Delta table
history_df = deltaTable.history()

# Get the latest version number
latest_version = history_df.select("version").orderBy("version", ascending=False).first()["version"]

# Print the latest version
print(f"The latest version is: {latest_version}")
mssparkutils.notebook.exit(str(latest_version))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
