# Databricks notebook source
# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST 's3://databricks-fashion-mnist-dataset/train/0/' LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON EXTERNAL LOCATION databricks_fashion_mnist_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON EXTERNAL LOCATION autoloader_checkpoint;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTERNAL LOCATION databricks_fashion_mnist_dataset;
# MAGIC --DESCRIBE EXTERNAL LOCATION autoloader_checkpoint;

# COMMAND ----------

checkpoint_path = "s3://databricks-dev-autoloader"
external_location = "s3://databricks-fashion-mnist-dataset"

# COMMAND ----------

checkpoint_path

# COMMAND ----------

bronze_fashion_images = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", "binaryfile")
                      .option("cloudFiles.schemaLocation", checkpoint_path+"/schema_bronze_images")
                      .option("cloudFiles.maxFilesPerTrigger", 10000)
                      .option("pathGlobFilter", "*.png")
                      .load(external_location+"/train"))

(bronze_fashion_images.writeStream
              .trigger(availableNow=True)
              .option("checkpointLocation", checkpoint_path+"/checkpoint_bronze_images")
              .table("fashion_demo_db.bronze_fashion_images").awaitTermination())

display(spark.table("fashion_demo_db.bronze_fashion_images"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fashion_demo_db.bronze_fashion_images LIMIT 100;

# COMMAND ----------

import io
import numpy as np
import pandas as pd
from pyspark.sql.functions import col, pandas_udf, substring_index
from PIL import Image

# COMMAND ----------

def extract_size(content):
  """Extract image size from its raw content."""
  image = Image.open(io.BytesIO(content))
  return image.size
 
@pandas_udf("width: int, height: int")
def extract_size_udf(content_series):
  sizes = content_series.apply(extract_size)
  return pd.DataFrame(list(sizes))

# COMMAND ----------

silver_fashion_images = (spark.readStream.table("fashion_demo_db.bronze_fashion_images")
                                .withColumn("image_id",substring_index(col('path'), '/', -1))
                                .withColumn("image_class", substring_index(substring_index(col('path'), '/', -2), '/', 1))
                                .withColumn("image_size", extract_size_udf(col("content")))
                                .filter("content is not null")
                                .select("image_id", "content", "image_size", "image_class"))

(silver_fashion_images.writeStream
                        .trigger(availableNow=True)
                        .option("checkpointLocation", checkpoint_path+"/checkpoint_silver_images")
                        .table("fashion_demo_db.silver_fashion_images").awaitTermination())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fashion_demo_db.silver_fashion_images;

# COMMAND ----------


