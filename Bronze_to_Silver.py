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

# MAGIC %md ###Step 1: Extract the "value" Column
# MAGIC 
# MAGIC Extract the structtype from the value column and split three different tables, movie, genres, originalLanguage

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode, flatten, lit, when

bronzeAgumentedDF = bronze_df.select("value", "value.*")

# COMMAND ----------


