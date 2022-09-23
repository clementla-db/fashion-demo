# Databricks notebook source
# MAGIC %md 
# MAGIC # Setup notebook
# MAGIC This notebook will ensure the cluster has the proper settings, UC securable objects and install required lib.

# COMMAND ----------

checkpoint_path = "s3://databricks-dev-autoloader"
external_location = "s3://databricks-fashion-mnist-dataset"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Unity Catalog configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create External Location pointing to the Fashion MNIST Dataset
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_fashion_mnist_dataset
# MAGIC URL 's3://databricks-fashion-mnist-dataset'
# MAGIC WITH (STORAGE CREDENTIAL field_demos_credential)
# MAGIC COMMENT 'External Location for the Fashion MNIST dataset';
# MAGIC 
# MAGIC -- Create External Location for Auto Loader checkpointing
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS autoloader_checkpoint
# MAGIC URL 's3://databricks-dev-autoloader'
# MAGIC WITH (STORAGE CREDENTIAL field_demos_credential)
# MAGIC COMMENT 'Checkpoint and schema evolution for Autoloader';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant to all users necessary privileges for autoloader ingestion
# MAGIC GRANT WRITE FILES, READ FILES, CREATE TABLE ON EXTERNAL LOCATION autoloader_checkpoint TO `account users`;
# MAGIC -- Grant read-only access to the Fashion MNIST Dataset
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION databricks_fashion_mnist_dataset TO `account users`;

# COMMAND ----------

import pyspark.sql.functions as F
import re
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

catalog = "fashion_demo_"+current_user_no_at

catalog_exists = False
for r in spark.sql("SHOW CATALOGS").collect():
    if r['catalog'] == catalog:
        catalog_exists = True

#As non-admin users don't have permission by default, let's do that only if the catalog doesn't exist (an admin need to run it first)     
if not catalog_exists:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"GRANT CREATE, USAGE on CATALOG {catalog} TO `account users`")
spark.sql(f"USE CATALOG {catalog}")

database = 'fashion_demo_db'
db_not_exist = len([db for db in spark.catalog.listDatabases() if db.name == database]) == 0
if db_not_exist:
  print(f"creating {database} database")
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
  spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.{database} TO `account users`")
  spark.sql(f"ALTER SCHEMA {catalog}.{database} OWNER TO `account users`")
