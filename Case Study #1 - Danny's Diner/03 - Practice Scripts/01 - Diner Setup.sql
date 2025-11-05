-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Import Libraries

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC from pyspark.sql.window import *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reading Data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC members_path = "/Volumes/workspace/business_projects/01_diner/members.csv"
-- MAGIC menu_path = "/Volumes/workspace/business_projects/01_diner/menu.csv"
-- MAGIC sales_path = "/Volumes/workspace/business_projects/01_diner/sales.csv"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC members_df = spark.read.format('csv').option('header','true').option("inferSchema", 'true').load(members_path)
-- MAGIC menu_df = spark.read.format('csv').option('header','true').option("inferSchema", 'true').load(menu_path)
-- MAGIC sales_df= spark.read.format('csv').option('header','true').option("inferSchema", 'true').load(sales_path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC members_df = members_df.select('customer_id', to_date('join_date', 'dd-MM-yyyy').alias("join_date"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Catalog

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS sql_challenge;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS sql_challenge.diner;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC members_df.write.mode("overwrite").saveAsTable("sql_challenge.diner.members")
-- MAGIC menu_df.write.mode("overwrite").saveAsTable("sql_challenge.diner.menu")
-- MAGIC sales_df.write.mode("overwrite").saveAsTable("sql_challenge.diner.sales")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Read Tables

-- COMMAND ----------

select * from sql_challenge.diner.members limit 5;

-- COMMAND ----------

select * from sql_challenge.diner.menu limit 5;

-- COMMAND ----------

select * from sql_challenge.diner.sales