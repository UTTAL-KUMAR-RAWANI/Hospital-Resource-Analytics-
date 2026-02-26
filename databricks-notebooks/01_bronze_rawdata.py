from pyspark.sql.functions import *

# Azure Event Hub Configuration
event_hub_namespace = "<<EVENT HUB NAMESPACE>>"
event_hub_name="<<EVENT HUB NAME>>"  
event_hub_conn_str = <<CONNECTION STRING>>
kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}
#Read from eventhub
raw_df = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
          )

#Cast data to json
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.<<StorageAccount_name>>.dfs.core.windows.net",
  "<<StorageAccount_access_Key>>"
)

bronze_path = "abfss://<<container>>@<<Storageaccount_name>>.dfs.core.windows.net/<<PATH>>"

#Write stream to bronze
(
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "abfss://<<container>>@<<Storageaccount_name>>.dfs.core.windows.net/_checkpoints/<<path>>")
    .start(bronze_path)
)