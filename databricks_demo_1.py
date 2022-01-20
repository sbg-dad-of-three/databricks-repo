# Databricks notebook source
# MAGIC %md This notebook is used for initial setup
# MAGIC 
# MAGIC  + Read raw source data and write to delta
# MAGIC  + Create the demo_bronze database to house SQL tables
# MAGIC  + Write the bronze layer delta data to the demo_bronze SQL tables

# COMMAND ----------

# MAGIC %md Read in demo data and write to bronze layer for delta lake

# COMMAND ----------

# read in data
customers = (
    spark.read.options(  header      = 'True'
                       , inferSchema = 'True') 
              .format('csv') 
              .load('dbfs:/FileStore/demo_data/jaffle_shop_customers.csv') 
)


# write to delta
(
  customers
  .write
  .format('delta')
  .mode('overwrite')
  .save('dbfs:/demo_data/bronze/customer_data')
)

# COMMAND ----------

# read in data
orders = (
    spark.read.options(  header      = 'True'
                       , inferSchema = 'True') 
              .format('csv') 
              .load('dbfs:/FileStore/demo_data/jaffle_shop_orders.csv') 
)


# write to delta
(
  orders
  .write
  .format('delta')
  .mode('overwrite')
  .save('dbfs:/demo_data/bronze/order_data')
)

# COMMAND ----------

# read in data
payments = (
    spark.read.options(  header      = 'True'
                       , inferSchema = 'True') 
              .format('csv') 
              .load('dbfs:/FileStore/demo_data/stripe_payments.csv') 
)


# write to delta
(
  payments
  .write
  .format('delta')
  .mode('overwrite')
  .save('dbfs:/demo_data/bronze/payment_data')
)

# COMMAND ----------

# MAGIC %md Create demo_bronze database to house bronze layer tables

# COMMAND ----------

import shutil
from pyspark.sql.types import *
# delete the old database and tables if needed
#_ = spark.sql('DROP DATABASE IF EXISTS demo_bronze CASCADE')

# create database to house SQL tables
_ = spark.sql('CREATE DATABASE demo_bronze')

# COMMAND ----------

# MAGIC %md Create queryable bronze layer delta tables 

# COMMAND ----------

spark.sql('''
          CREATE TABLE demo_bronze.customers 
          USING DELTA 
          LOCATION 'dbfs:/demo_data/bronze/customer_data'
          ''')

# COMMAND ----------

spark.sql('''
          CREATE TABLE demo_bronze.orders 
          USING DELTA 
          LOCATION 'dbfs:/demo_data/bronze/order_data'
          ''')

# COMMAND ----------

spark.sql('''
          CREATE TABLE demo_bronze.payments 
          USING DELTA 
          LOCATION 'dbfs:/demo_data/bronze/payment_data'
          ''')
