# Databricks notebook source
# MAGIC %run ./config/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze to Silver Step

# COMMAND ----------

#make notebook idempotent
dbutils.fs.rm(silverPath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Load New Record from the Bronze Records

# COMMAND ----------

from pyspark.sql.functions import col
bronzeDF = spark.read.table("movie_bronze").filter(col("status") == "new")
display(bronzeDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1: Extract the structtype from the value column and split three different tables, movie, genres, originalLanguage

# COMMAND ----------

# check the type of each column
bronzeDF.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode, flatten, lit, when

bronzeAgumentedDF = bronzeDF.select("value", "value.*")
# Fix all the minimum budget of 1 million
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
    lit(1).alias("Language_Id")
).withColumn("Budget", when((bronzeAgumentedDF.Budget < 100000), 100000).otherwise(bronzeAgumentedDF.Budget))

silver_genres = bronzeAgumentedDF.select(
    explode(col("genres"))
)

silver_language = bronzeAgumentedDF.select(
    col("OriginalLanguage")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Transform the data and clean

# COMMAND ----------

display(silver_movie)

# COMMAND ----------

# Distinct rows and filter runtime > 0
silver_movie_clean = silver_movie.distinct().filter(col("RunTime") >= 0)
silver_movie_quarantined = silver_movie.distinct().filter(col("RunTime") < 0)


# COMMAND ----------

# fix the missing value of name for dropping the blank name.
silver_genres_flatten = silver_genres.select(col("col.*")).distinct()
silver_genres_clean = silver_genres_flatten.select(col("id").alias("Id"), col("name")).filter(col("name") != "").orderBy("Id")
silver_genres_clean.printSchema()
display(silver_genres_clean)

# COMMAND ----------

from pyspark.sql.functions import lit
silver_language_clean = silver_language.distinct().select(
    lit(1).alias("Id"),
    col("OriginalLanguage")
)
display(silver_language_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Write Clean Batch to a Silver Table (movie, genres, originalLanguage)

# COMMAND ----------

# write clean of movie dataframe to delta format
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
        col("Language_Id")
    )
    .write.format("delta")
    .mode("overwrite")
    .partitionBy("p_CreatedDate")
    .save(silverPath+"movie/")
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
LOCATION "{silverPath+"movie/"}"
"""
)

# COMMAND ----------

# write genres dataframe to delta file

(
    silver_genres_clean
    .write.format("delta")
    .mode("overwrite")
    .partitionBy("Id")
    .save(silverPath+"genres/")
    
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
LOCATION "{silverPath+"genres/"}"
"""
)

# COMMAND ----------

# write originalLanguage dataframe to delta file

(
    silver_language_clean
    .write.format("delta")
    .mode("overwrite")
    .partitionBy("Id")
    .save(silverPath+"language/")
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
LOCATION "{silverPath+"language/"}"
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC Update Bronze table to Reflect the Loads
# MAGIC \
# MAGIC Step 1: Update Clean records

# COMMAND ----------

from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silver_movie_clean.withColumn("status", lit("loaded"))

update_match = "bronze.value = clean.value"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Update Quarantined records

# COMMAND ----------

display(silver_movie_quarantined)

# COMMAND ----------

silverAugmented = silver_movie_quarantined.withColumn(
    "status", lit("quarantined")
)

update_match = "bronze.value = quarantine.value"
update = {"status": "quarantine.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)


# COMMAND ----------


