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

#recover secret and id 
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
tenant_id = "fa5cfbbf-c9b7-4887-a40b-d42e9c7a6d6b"

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

storage_account = "processedsa01"
container = "testlecturadbx"
sub_path = "/datatotestdbxreaders2"   #"/datatotestdbxreaders"

service_principal_id = get_id
service_principal_password = get_secret
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", service_principal_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_principal_password)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

#read dataset1  to modify it 
df_filtered = spark.sql("SELECT cod_pais_iso2,cod_city,cod_postal,val_average_income, CONCAT(cod_pais_iso2, '-', cod_city) as Cod_full, YEAR(CURRENT_DATE) as year,MONTH(CURRENT_DATE) as month, DAY(CURRENT_DATE) as day FROM lkhgold01.dbo.dataset1")
# display(df_filtered)

# write
#df.write.format("delta").mode("overwrite").save(f"abfss://{contanier}}@{storage_account}.dfs.core.windows.net{sub_path}")

# partitioned write
df_filtered.write.partitionBy("year", "month", "day").format("delta").mode("overwrite").save(f"abfss://{container}@{storage_account}.dfs.core.windows.net{sub_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
