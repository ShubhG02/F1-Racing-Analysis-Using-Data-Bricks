# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC step 1 : Read the json file using the spark dataframe reader 

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name String, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read\
.option("header",True)\
.schema(constructor_schema)\
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")


# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 : drop the unwanted columns 

# COMMAND ----------

from pyspark.sql.functions import col 

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 : Rename columns and add ingestion data 

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id")\
                                             .withColumnRenamed("constructorRef", "constructor_ref")\
                                             .withColumn("ingestion_date", current_timestamp())\
                                             .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;