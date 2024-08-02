# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuit.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - read the csv file using the spark datafream reader

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuit_schema = StructType(fields =[StructField("circuitid",IntegerType(),False),
                                     StructField("circuitRef",StringType(),True),
                                     StructField("name",StringType(),True),
                                     StructField("location",StringType(),True),
                                     StructField("country",StringType(),True),
                                     StructField("lat",DoubleType(),True),
                                     StructField("lng",DoubleType(),True),
                                     StructField("alt",IntegerType(),True),
                                     StructField("url",StringType(),True)
])

# COMMAND ----------

circuit_df = spark.read\
.option("header",True)\
.schema(circuit_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select only the required columns

# COMMAND ----------

circuit_selected_df = circuit_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

circuit_selected_df = circuit_df.select(circuit_df.circuitid,circuit_df.circuitRef,circuit_df.name,circuit_df.location,circuit_df.country,circuit_df.lat,circuit_df.lng,circuit_df.alt)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

##the diff between the 1st select and the other 2 is they let you apply any column based functions for ex if we want to change the name of column 
circuit_selected_df = circuit_df.select(col("circuitid"),col("circuitRef"),col("name"),col("location"),col("country").alias("race_country"),col("lat"),col("lng"),col("alt"))




# COMMAND ----------

# MAGIC %md
# MAGIC ##Rename the column

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuit_selected_df.withColumnRenamed("circuitid","circuit_id")\
.withColumnRenamed("circuitRef","circuit_ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")\
.withColumn("data_source", lit(v_data_source))
  

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Adding column 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , lit

# COMMAND ----------

circuit_final_df = add_ingestion_date(circuits_renamed_df)
                                                  

# COMMAND ----------

display(circuit_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #write data to datalake as parquet

# COMMAND ----------

circuit_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")


# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl210/processed/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;