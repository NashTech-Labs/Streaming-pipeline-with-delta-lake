# Databricks notebook source
# DBTITLE 1,Authentication Credential to read events from PubSub.
client_id_secret = dbutils.secrets.get(scope = "gcp-pubsub", key = "client_id")
client_email_secret = dbutils.secrets.get(scope = "gcp-pubsub", key = "client_email")
private_key_secret = dbutils.secrets.get(scope = "gcp-pubsub", key = "private_key")
private_key_id_secret = dbutils.secrets.get(scope = "gcp-pubsub", key = "private_key_id")
authOptions = {"client_id": client_id_secret,
               "client_email": client_email_secret,
               "private_key": private_key_secret,
               "private_key_id": private_key_id_secret}

# COMMAND ----------

# DBTITLE 1, Spark structured streaming ingestion from PubSub topic vehicle.
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

vehicleInputDF = spark.readStream.format("pubsub") \
.option("subscriptionId", "your-subscription-Id") \
.option("topicId", "your-topic-Id") \
.option("projectId", "your-project-Id") \
.option("numFetchPartitions", "3") \
.options(**authOptions).load()

# COMMAND ----------

# DBTITLE 1,Schema for the vehicle events 
vehicleSchema = (
      StructType()
         .add("carId", "integer")
         .add("brand", "string")
         .add("model", "string")
         .add("year", "integer")
         .add("color", "string")
         .add("mileage", "float")
         .add("price", "integer")                    
         .add("quantity", "integer")
         .add("tax", "float")
     )

# COMMAND ----------

# DBTITLE 1,Spark data frame for the input vehicle event
vehicleDF = (
     vehicleInputDF.select(from_json(col("payload").cast("string"), vehicleSchema).alias("vehicledata"))
          .select(
               "vehicledata.carid",
               "vehicledata.brand",
               "vehicledata.model",
               "vehicledata.year",
               "vehicledata.color",
               "vehicledata.mileage",
               "vehicledata.price",
               "vehicledata.quantity",
               "vehicledata.tax"
               )
          )

# COMMAND ----------

# DBTITLE 1,Writing streaming raw vehicle data frame to the delta lake table(Bronze table)
(
vehicleDF.writeStream.format("delta") \
.outputMode("append") \
.option("checkpointLocation", "/dbfs/pubsub-vehicle-bronze-18/") \
.trigger(processingTime = '5 seconds') \
.table("main.car_demo_data_lake.vehicle_data_bronze")
)

# COMMAND ----------

# DBTITLE 1,Reading streaming vehicle events from bronze table, transformation to drop tax column
silverDF = spark.readStream \
.table("main.car_demo_data_lake.vehicle_data_bronze") \
    .drop('tax')


# COMMAND ----------

# DBTITLE 1,Merge operation if same carID already present in silver table update the row otherwise insert a new row
def writeUpdatedVehicleEvents(batchDF, batchId): 
  batchDF.createOrReplaceTempView("changes")
  
  (
     batchDF._jdf.sparkSession().sql("""
     
      MERGE INTO main.car_demo_data_lake.vehicle_data_silver tgt
          USING changes src ON tgt.carId = src.carId              

        -- When conditions match, update record
        WHEN MATCHED 
            THEN UPDATE SET tgt.carId = src.carId, tgt.brand = src.brand, tgt.model = src.model, 
                         tgt.year = src.year, tgt.color = src.color, tgt.mileage = src.mileage, tgt.price = src.price, tgt.quantity = src.quantity
                    

        -- When conditions do not match, insert record
        WHEN NOT MATCHED 
            THEN INSERT (carId, brand, model, year, color, mileage, price, quantity) 
                         
                 VALUES (carId, brand, model, year, color, mileage, price, quantity) 
                  """)
  ) 

# COMMAND ----------

# DBTITLE 1,For Each batch of data frame perform above merge operation then writing transformed silver data frame to the silver table
(
silverDF.writeStream \
.outputMode("append") \
.foreachBatch(writeUpdatedVehicleEvents) \
.option("checkpointLocation", "/dbfs/pubsub-vehicle-silver-18/") \
.trigger(processingTime = '5 seconds') \
.start()
)

# COMMAND ----------

# DBTITLE 1,Reading streaming data frame from silver table
goldDF = spark.readStream \
.table("main.car_demo_data_lake.vehicle_data_silver") 


# COMMAND ----------

# DBTITLE 1,aggregate cars quantity and price for each brands and years
group_cols = ["brand", "year"]
vechileGoldDF = goldDF.groupBy(group_cols) \
.agg(sum("quantity").alias("total_orderd_cars"), sum("price").alias("total_orderd_cars_price"))

# COMMAND ----------

# DBTITLE 1,Writing aggregated result to Gold table
(
    vechileGoldDF.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/dbfs/pubsub-vehicle-glod-18/") \
    .trigger(processingTime = '5 seconds') \
    .table("main.car_demo_all_brands.vehicle_data_gold")
)
