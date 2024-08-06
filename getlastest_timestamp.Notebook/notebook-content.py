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
# META     },
# META     "environment": {}
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
from pyspark.sql import SparkSession

delta_table_path = "Tables/dbo/"+table_name

delta_table_path 

# Load the Delta table
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Query to get the latest modified timestamp
latest_timestamp_df = delta_table.history().select("timestamp").orderBy("timestamp", ascending=False).limit(1)


# Get the latest timestamp value
latest_timestamp = latest_timestamp_df.collect()[0]["timestamp"]

print(f"latest_timestamp={latest_timestamp}")

mssparkutils.notebook.exit(str(latest_timestamp))


# Update the Fabric data warehouse table
#update_query = f"""
#UPDATE dwhgoldhub01.audit.Control
#SET Lastloadedtime  = '{latest_timestamp}'
#WHERE TableName  = 'dataset1'
#"""



# Execute the update query
#spark.sql(update_query)

#print(update_query)
# Show the result
#latest_timestamp_df.show()

# Optionally, write the result to sql table  file
#output_path = "path/to/output/latest_timestamp.csv"
#latest_timestamp_df.write.csv(output_path, header=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
