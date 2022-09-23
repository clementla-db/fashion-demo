# Databricks notebook source
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
