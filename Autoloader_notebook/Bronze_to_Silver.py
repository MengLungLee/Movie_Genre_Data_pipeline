# Databricks notebook source
# MAGIC %md ### Setting up configuration file 
# MAGIC
# MAGIC Get data path from configuration.py, including Raw, Bronze, Silver data path.

# COMMAND ----------

# MAGIC %run ./config/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Silver Table: Enriched Recording Data
# MAGIC As a second hop in our silver level, we will do the follow enrichments and checks:
# MAGIC - Check and Cast all the data type where inferred from JSON format
# MAGIC - Our recordings data will be split to three different tables, Movie, Genre, Language
# MAGIC - Certain Movies have valid ID for the genre, but the name of the genre is missing
# MAGIC - Assume all the movies should have a minimum budget of 1 million, replacing it with 1 million
# MAGIC - We will exclude RunTime that are < 0 as the **Quarantined data**, as we know that these represent an error in transmission. The left data would be the clean data stored in **Silver_table_clean** and changed the status of bronze table as **Loaded**

# COMMAND ----------

from pyspark.sql.functions import col
bronze_df = (
    spark.readStream
    .table("movie_bronze")
    .filter(col("status") == "new")
)

# COMMAND ----------

# MAGIC %md ### Extract the "value" Column
# MAGIC
# MAGIC Extract the structtype from the value column and split three different tables, movie, genres, originalLanguage.
# MAGIC
# MAGIC Check each data type and cast its type reasonable.

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
bronzeAgumentedDF = bronze_df.select("value", "value.*")

silver_movie = bronzeAgumentedDF.select(
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
    lit(1).alias("Language_Id"),
    current_timestamp().cast("date").alias("p_ingestdate")
).withColumn("Budget", when((bronzeAgumentedDF.Budget < 100000), 100000).otherwise(bronzeAgumentedDF.Budget))

silver_genres = bronzeAgumentedDF.select(
    explode(col("genres"))
)

silver_language = bronzeAgumentedDF.select(
    col("OriginalLanguage")
)

# COMMAND ----------

# MAGIC %md ### Distinct Rows and split to data as cleaned and quarantined 
# MAGIC
# MAGIC **Runtime**, this column should greater than 0

# COMMAND ----------

silver_movie_clean = silver_movie.distinct().filter(col("RunTime") >= 0)
silver_movie_quarantined = silver_movie.distinct().filter(col("RunTime") < 0)

# COMMAND ----------

# MAGIC %md ### Fix the Genres table
# MAGIC fix the missing value of name for dropping the blank name.

# COMMAND ----------

silver_genres_flatten = silver_genres.select(col("col.*")).distinct()
silver_genres_clean = silver_genres_flatten.select(col("id").alias("Id"), col("name")).filter(col("name") != "")

# COMMAND ----------

# MAGIC %md ### Fix the language table

# COMMAND ----------

from pyspark.sql.functions import lit
silver_language_clean = silver_language.distinct().select(
    lit(1).alias("Id"),
    col("OriginalLanguage")
)

# COMMAND ----------

# MAGIC %md ### Write cleaned batch to a **Silver** Table (movie, genres, originalLanguage)

# COMMAND ----------

# MAGIC %md ##### Movie Table

# COMMAND ----------

(
    silver_movie_clean.select(
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
        col("p_ingestdate")
    )
    .writeStream
    .format("delta")
    .option("checkpointLocation", silverCheckpoint + "Movie/")
    .option("cloudFiles.partitionColumns", "p_ingestdate")
    .outputMode("append")
    .queryName("Bronze_to_Silver_Movie")
    .trigger(once = True)
    .start(silverPath + "Movie/")
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movie_silver
"""
)

spark.sql(
    f"""
CREATE TABLE movie_silver
USING DELTA
LOCATION "{silverPath+"Movie/"}"
"""
)

# COMMAND ----------

# MAGIC %md ##### Genres Table

# COMMAND ----------

(
    silver_genres_clean
    .writeStream
    .format("delta")
    .option("checkpointLocation", silverCheckpoint + "Genres/")
    .outputMode("append")
    .queryName("Bronze_to_Silver_Genres")
    .trigger(once = True)
    .start(silverPath + "Genres/")
    
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS genres_silver
"""
)

spark.sql(
    f"""
CREATE TABLE genres_silver
USING DELTA
LOCATION "{silverPath+"Genres/"}"
"""
)

# COMMAND ----------

# MAGIC %md ##### Language Table

# COMMAND ----------

(
    silver_language_clean
    .writeStream
    .format("delta")
    .option("checkpointLocation", silverCheckpoint + "Language/")
    .outputMode("append")
    .queryName("Bronze_to_Silver_Language")
    .trigger(once = True)
    .start(silverPath + "Language/")
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS language_silver
"""
)

spark.sql(
    f"""
CREATE TABLE language_silver
USING DELTA
LOCATION "{silverPath+"Language/"}"
"""
)

# COMMAND ----------

# MAGIC %md ### Update Bronze Table to reflect the Loads
# MAGIC \
# MAGIC Step 1: Update loaded records from silver to bronze

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silver_movie_clean.withColumn("status", lit("loaded"))

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
  .outputMode("update")
  .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check bronze table if the records have been updated in "status" from "new" to "loaded"
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Update Quarantined records

# COMMAND ----------


silverAugmented_qu = silver_movie_quarantined.withColumn(
    "status", lit("quarantined")
)

(
    silverAugmented_qu.writeStream
  .format("delta")
  .trigger(once = True)
  .foreachBatch(upsertToDelta)
  .outputMode("update")
  .start()
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_bronze WHERE status = 'quarantined'

# COMMAND ----------

quarantined_count = spark.sql("SELECT count(*) FROM movie_bronze WHERE status = 'quarantined'").collect()[0][0]
clean_count = spark.sql("SELECT count(*) FROM movie_bronze WHERE status = 'loaded'").collect()[0][0]

print("Quarantined Count: {}, Cleaed Count: {}".format(quarantined_count, clean_count))

# COMMAND ----------


