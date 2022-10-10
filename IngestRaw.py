# Databricks notebook source
# MAGIC %md ### Setting up configuration file 
# MAGIC 
# MAGIC Get data path from configuration.py, including Raw, Bronze, Silver data path.

# COMMAND ----------

# MAGIC %run ./config/configuration

# COMMAND ----------

# MAGIC %md ### Ingest Raw Data and enrich it by adding Metadata
# MAGIC 
# MAGIC Adding Metadata, Datasource, Ingest_Time, Status, Ingest_timestamp.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

def autoload_Ingest_Raw(data_source, source_format):
    df = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .load(data_source)
         )
    
    return df

# COMMAND ----------

raw_df = autoload_Ingest_Raw(rawPath, "text")

# COMMAND ----------

raw_moive_data_df = (
    raw_df.select(
            col("value"),
            lit("movie.json").alias("datasource"),
            current_timestamp().alias("ingesttime"),
            lit("new").alias("status"),
            current_timestamp().cast("date").alias("p_ingestdate")
    )
)

# COMMAND ----------

streamQuery = (raw_moive_data_df.writeStream
      .format("delta")
      .option("checkpointLocation", bronzeCheckpoint)
      .option("cloudFiles.partitionColumns", "p_ingestdate")
      .outputMode("append")
      .queryName("Raw_To_Bronze")
      .trigger(once = True)
      .start(bronzePath)
)

# COMMAND ----------

# MAGIC %md ### Create Bronze tabe with Data Tab for visualizing the content

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

# MAGIC %sql
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

|
