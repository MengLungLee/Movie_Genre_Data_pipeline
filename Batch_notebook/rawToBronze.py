# Databricks notebook source
# MAGIC %run ./config/configuration

# COMMAND ----------

display(dbutils.fs.ls(rawPath))
dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

rawMovieDf = (
            spark.read
            .format("json")
            .option("multiline", "true")
            .load(rawPath)
)
display(rawMovieDf)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Explode nested array into rows

# COMMAND ----------

from pyspark.sql.functions import explode

explodeDF = rawMovieDf.select(explode(rawMovieDf.movie))
display(explodeDF)

# COMMAND ----------

# MAGIC  %md
# MAGIC Ingestion Metadata

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col

raw_moive_data_df = (
    explodeDF.select(
            col("col").alias("value"),
            lit("movie.json").alias("datasource"),
            current_timestamp().alias("ingesttime"),
            lit("new").alias("status"),
            current_timestamp().cast("date").alias("ingestdate")
    )
)
display(raw_moive_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write batch to a bronze table

# COMMAND ----------

(
    raw_moive_data_df.select(
        col("datasource"),
        col("ingesttime"),
        col("value"),
        col("status"),
        col("ingestdate").alias("p_ingestdate"),
    )
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("p_ingestdate")
    .save(bronzePath)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS movie_bronze
""")

spark.sql(f"""
CREATE TABLE movie_bronze
USING DELTA
LOCATION "{bronzePath}"
""")

# COMMAND ----------


