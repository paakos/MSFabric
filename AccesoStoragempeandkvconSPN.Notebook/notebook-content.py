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
# META     "environment": {
# META       "environmentId": "1cd4d241-fbab-49f7-9624-692cc11d652b",
# META       "workspaceId": "3c090656-62e2-47f4-8cf2-970e0bd9566c"
# META     }
# META   }
# META }

# CELL ********************

#reading the content of storage using a spn con el secret en un KV 
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import current_date, current_timestamp, lit
tenant_id = "fa5cfbbf-c9b7-4887-a40b-d42e9c7a6d6b"
#service_principal_id = "3f7b3618-fd19-47c5-9019-077b2f3ff42f"


# load spn secret 
kv_uri = "https://paakoskv01.vault.azure.net/"
#get pass
secret_name = "testsecret"
get_secret = mssparkutils.credentials.getSecret(kv_uri, secret_name)

#get_id
secret_name = "secretid"
get_id = mssparkutils.credentials.getSecret(kv_uri, secret_name)


#credentials = ClientSecretCredential(client_id=service_principal_id, client_secret=service_principal_password,tenant_id=tenant_id) 
#client = SecretClient(vault_url = kv_uri, credential = credentials)


print(get_secret)
print(get_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#reading the content of storage using a spn con el secret en un KV 


# spn
storage_account = "processedsa01"
container = "data"
sub_path1 = "/Dataset1/dataset1.snappy.parquet"
sub_path2 = "/Dataset2/dataset2.snappy.parquet"
sub_path3 = "/Dataset3/year=2024/month=7/day=1/*"
tenant_id = "fa5cfbbf-c9b7-4887-a40b-d42e9c7a6d6b"
service_principal_id =  get_id   # "3f7b3618-fd19-47c5-9019-077b2f3ff42f"
service_principal_password = get_secret



spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", service_principal_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_principal_password)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


df1 = spark.read.format("parquet").option("header", "true").load(f"abfss://{container}@{storage_account}.dfs.core.windows.net{sub_path1}")
df2 = spark.read.format("parquet").option("header", "true").load(f"abfss://{container}@{storage_account}.dfs.core.windows.net{sub_path2}")
df3 = spark.read.format("parquet").option("header", "true").load(f"abfss://{container}@{storage_account}.dfs.core.windows.net{sub_path3}")

df3.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#escritura optimizada al lakehouse  / o no  
spark.conf.set("sprk.sql.parquet.vorder.enabled", "true") # Enable Verti-Parquet write
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true") # Enable automatic delta optimized write

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get current user using SQL for AUDIT
current_user = spark.sql("SELECT current_user()").collect()[0][0]





#escritura a tabla Dataset1
Table_name1 = "Dataset1"
df1_audit = df1.withColumn("executed_by", lit(current_user)).withColumn("load_timestamp", current_timestamp())
df1_audit.write.format("delta").mode("append").saveAsTable(Table_name1)


#escritura a File Dataset2
Table_name2 = "Dataset2"
df2_audit = df2.withColumn("executed_by", lit(current_user)).withColumn("load_timestamp", current_timestamp())
df2_audit.write.format("delta").mode("append").saveAsTable(Table_name2)

#escritura a File Dataset3 ruta hardcodeada
Table_name3 = "Dataset3"
df3_audit = df3.withColumn("executed_by", lit(current_user)).withColumn("load_timestamp", current_timestamp())
df3_audit.write.format("delta").mode("append").saveAsTable(Table_name3)


#escritura a File Dataset3 full route with filter in sub_path
#Table_name2 = "Dataset3"
#df3.write.format("delta").mode("append").saveAsTable(Table_name3)
#year = 2016
#months = "7,8"
#wasbs_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net/{sub_path3}"


# Filter data by year and months si existen el las columnas si no componer la ruta en el sub_path
# filtered_df = df3.filter(f"puYear = {year} AND puMonth IN ({months})")


print(f"Spark df1_audit saved to delta table: {Table_name1}")
print(f"Spark df2_audit saved to delta table: {Table_name2}")
print(f"Spark df3_audit saved to delta table: {Table_name3}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_date, current_timestamp, lit

# Create a sample DataFrame
data = [("example",)]
df = spark.createDataFrame(data, ["column"])

# Add current date and timestamp
df = df.withColumn("current_date", current_date()).withColumn("current_timestamp", current_timestamp())

# Get current user using SQL
current_user = spark.sql("SELECT current_user()").collect()[0][0]

# Add current user to DataFrame using lit
df = df.withColumn("executed_by", lit(current_user))

df.show(truncate=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
