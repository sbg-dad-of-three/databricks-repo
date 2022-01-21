# Databricks notebook source
# MAGIC %md This notebook is used to analyze / visualize the gold layer data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM demo_gold.gold_customer_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   gold_customer_orders.customer_status,
# MAGIC   COUNT(DISTINCT gold_customer_orders.customer_id) AS customer_count,
# MAGIC   AVG(gold_customer_orders.customer_lifetime_value) AS average_customer_lifetime_value
# MAGIC FROM demo_gold.gold_customer_orders
# MAGIC GROUP BY gold_customer_orders.customer_status

# COMMAND ----------

customer_orders = (
    spark.read.format("delta").load('/user/hive/warehouse/demo_gold.db/gold_customer_orders/')
)


# COMMAND ----------

display(customer_orders)

# COMMAND ----------


