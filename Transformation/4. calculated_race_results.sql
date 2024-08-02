-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

REFRESH TABLE results;
REFRESH TABLE drivers;
REFRESH TABLE constructors;
REFRESH TABLE races;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_presentation.calculated_race_results;

create table f1_presentation.calculated_race_results
using parquet
as 
select races.race_year ,
       constructors.name as team_name, 
       drivers.name as driver_name , 
       results.position,  
       results.points,
       11 - results.position as calculated_points
  from results
  join f1_processed.drivers on (results.driver_id = drivers.driver_id)
  join f1_processed.constructors on (results.constructor_id = constructors.constructor_id)
  join f1_processed.races on (results.race_id = races.race_id)
  where results.position <= 10 






-- COMMAND ----------

select * from f1_presentation.calculated_race_results order by race_year asc , position asc;

-- COMMAND ----------

select races.race_year ,
       constructors.name as team_name, 
       drivers.name as driver_name , 
       results.position,  
       results.points,
       11 - results.position as calculated_points
  from results
  join f1_processed.drivers on (results.driver_id = drivers.driver_id)
  join f1_processed.constructors on (results.constructor_id = constructors.constructor_id)
  join f1_processed.races on (results.race_id = races.race_id)
  where results.position <= 10
  order by races.race_year , results.position asc;


-- COMMAND ----------

