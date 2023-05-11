# Databricks notebook source
# MAGIC %run ./config/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC Handle Quarantined Records
# MAGIC \
# MAGIC Step 1: Load Quarantined Records from the Bronze Table

# COMMAND ----------

from pyspark.sql.functions import *
bronzeQuarantinedDF = (
    spark.readStream
    .table("movie_bronze")
    .filter(col("status") == "quarantined")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Transform the Quarantined Records

# COMMAND ----------

bronzeQuarTransDF = bronzeQuarantinedDF.select(
    col("value"),
    col("value.*")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Absolute the RunTime value

# COMMAND ----------

repairDF = bronzeQuarTransDF.distinct().withColumn("RunTime", abs(col("RunTime")))

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3: Using Merge API to insert cleaned record into Silver Table

# COMMAND ----------

from delta.tables import *

silverTable = DeltaTable.forPath(spark, silverPath + "Movie/")

insert_match = "silver.Id = df.Id"

def appendToSilver(microBatchOutputDF, batchId):
  (silverTable.alias("silver").merge(
      microBatchOutputDF.alias("df"),
      insert_match)
    .whenNotMatchedInsertAll()
    .execute()
  )


# COMMAND ----------

silverCleanedDF = repairDF.select(
    col("value"),
    col("Id").cast("INTEGER"),
    col("Budget"),
    col("Revenue"),
    col("RunTime").cast("INTEGER"),
    col("Price"),
    col("Title"),
    col("Overview"),
    col("Tagline"),
    col("ImdbUrl"),
    col("TmdbUrl"),
    col("PosterUrl"),
    col("BackdropUrl"),
    col("ReleaseDate").cast("DATE"),
    col("CreatedDate").cast("DATE").alias("p_CreatedDate"),
    col("UpdatedDate"),
    col("UpdatedBy"),
    col("CreatedBy"),
    col("Genres.id").alias("Genres_Id"),
    lit(1).alias("Language_Id")
)

silverCleanedDF_insert = silverCleanedDF.select(
        col("Id"),
        col("Budget"),
        col("Revenue"),
        col("RunTime"),
        col("Price"),
        col("Title"),
        col("Overview"),
        col("Tagline"),
        col("ImdbUrl"),
        col("TmdbUrl"),
        col("PosterUrl"),
        col("BackdropUrl"),
        col("ReleaseDate"),
        col("p_CreatedDate"),
        col("UpdatedDate"),
        col("UpdatedBy"),
        col("CreatedBy"),
        col("Genres_Id"),
        col("Language_Id"),
        current_timestamp().cast("date").alias("p_ingestdate")
    )
(    
    silverCleanedDF_insert
    .writeStream
    .format("delta")
    .option("cloudFiles.partitionColumns", "p_ingestdate")
    .outputMode("append")
    .queryName("SilverQuar_to_SilverClean_Movie")
    .trigger(once = True)
    .foreachBatch(appendToSilver)
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3-2: Update Bronze Table to reflect the Loads

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silverCleanedDF.withColumn("status", lit("loaded"))

update_match = "bronze.value = silver.value"
update = {"status": "silver.status"}


# Function to upsert microBatchOutputDF into Delta table using merge
def upsertToDelta(microBatchOutputDF, batchId):
  (deltaTable.alias("bronze").merge(
      microBatchOutputDF.alias("silver"),
      update_match)
    .whenMatchedUpdate(set = update)
    .execute()
  )


# COMMAND ----------

(
    silverAugmented.writeStream
  .format("delta")
  .trigger(once = True)
  .foreachBatch(upsertToDelta)
  .queryName("SilverClean_To_Bronze_Movie")
  .outputMode("append")
  .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM movie_silver
