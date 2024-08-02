# Databricks notebook source
# MAGIC %md
# MAGIC ##### Produce driver standings

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC Find race years for which the data is to be reprocessed 

# COMMAND ----------

race_results_list = spark.read.parquet(f"{presentation_folder_path}/race_results")\
.filter(f"file_date = '{v_file_date}'") \
.select("race_year")\
.distinct()\
.collect()

# COMMAND ----------

race_results_list

# COMMAND ----------

race_year_list = []
for race_year in race_results_list :
  race_year_list.append(race_year.race_year)


# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

final_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standings;

# COMMAND ----------

# MAGIC %sql 
# MAGIC --drop table f1_presentation.driver_standings;

# COMMAND ----------

