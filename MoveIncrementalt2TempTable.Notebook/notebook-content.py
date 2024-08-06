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
# META       "default_lakehouse_workspace_id": "3c090656-62e2-47f4-8cf2-970e0bd9566c",
# META       "known_lakehouses": [
# META         {
# META           "id": "c8c96811-35e9-4959-ab9d-cde494b191e6"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# # Move to a temporal table just new rows 

# PARAMETERS CELL ********************

table_name = 'dataset1'
last_loadedwatermark = '2000-07-22 06:06:28.467518'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sparkquery = f"SELECT * FROM lkhgold01.dbo.{table_name} where load_timestamp > '{last_loadedwatermark}' "

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(sparkquery)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_incremental = spark.sql(sparkquery)
#display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write data to a Delta table to use a copy activity to move later 

temptablename = "temp"+table_name
#print(temptablename)
df_incremental.write.format("delta").mode("overwrite").saveAsTable(temptablename)
mssparkutils.notebook.exit(str(temptablename))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
