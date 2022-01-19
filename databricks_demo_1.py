# Databricks notebook source
# MAGIC %md Create bronze database

# COMMAND ----------

import shutil
from pyspark.sql.types import *
# delete the old database and tables if needed
_ = spark.sql('DROP DATABASE IF EXISTS bronze CASCADE')

# create database to house SQL tables
_ = spark.sql('CREATE DATABASE bronze')

# COMMAND ----------

# MAGIC %md Create silver database

# COMMAND ----------

import shutil
from pyspark.sql.types import *
# delete the old database and tables if needed
_ = spark.sql('DROP DATABASE IF EXISTS silver CASCADE')

# create database to house SQL tables
_ = spark.sql('CREATE DATABASE silver')

# COMMAND ----------

# MAGIC %md Create gold database

# COMMAND ----------

import shutil
from pyspark.sql.types import *
# delete the old database and tables if needed
_ = spark.sql('DROP DATABASE IF EXISTS gold CASCADE')

# create database to house SQL tables
_ = spark.sql('CREATE DATABASE gold')

# COMMAND ----------

# MAGIC %md Read in demo data

# COMMAND ----------

customers = (
    spark.read.options(  header      = 'True'
                       , inferSchema = 'True') 
              .format('csv') 
              .load('dbfs:/FileStore/demo_data/jaffle_shop_customers.csv') 
)

# COMMAND ----------

display(customers)

# COMMAND ----------

orders = (
    spark.read.options(  header      = 'True'
                       , inferSchema = 'True') 
              .format('csv') 
              .load('dbfs:/FileStore/demo_data/jaffle_shop_orders.csv') 
)

# COMMAND ----------

display(orders)

# COMMAND ----------

payments = (
    spark.read.options(  header      = 'True'
                       , inferSchema = 'True') 
              .format('csv') 
              .load('dbfs:/FileStore/demo_data/stripe_payments.csv') 
)

# COMMAND ----------

display(payments)

# COMMAND ----------

# MAGIC %md Write intial demo data to delta

# COMMAND ----------

(
  customers
  .write
  .format('delta')
  .mode('overwrite')
  .save('dbfs:/demo_data/bronze/customer_data')
)

# COMMAND ----------

spark.sql('''
          CREATE TABLE bronze.customers 
          USING DELTA 
          LOCATION 'dbfs:/demo_data/bronze/customer_data'
          ''')

# COMMAND ----------

(
  orders
  .write
  .format('delta')
  .mode('overwrite')
  .save('dbfs:/demo_data/bronze/order_data')
)

# COMMAND ----------

spark.sql('''
          CREATE TABLE bronze.orders 
          USING DELTA 
          LOCATION 'dbfs:/demo_data/bronze/order_data'
          ''')

# COMMAND ----------

(
  payments
  .write
  .format('delta')
  .mode('overwrite')
  .save('dbfs:/demo_data/bronze/payment_data')
)

# COMMAND ----------

spark.sql('''
          CREATE TABLE bronze.payments 
          USING DELTA 
          LOCATION 'dbfs:/demo_data/bronze/payment_data'
          ''')

# COMMAND ----------


