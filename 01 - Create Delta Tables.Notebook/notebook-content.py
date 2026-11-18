# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8cd8947b-cb7c-44d5-ad18-e38d48b9326b",
# META       "default_lakehouse_name": "wwilakehouse",
# META       "default_lakehouse_workspace_id": "e33cf0de-89a0-48de-82c1-8e91a8e7b58e",
# META       "known_lakehouses": [
# META         {
# META           "id": "8cd8947b-cb7c-44d5-ad18-e38d48b9326b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Spark session configuration
# This cell enables two Fabric features that optimize how data is written and read in subsequent cells: V-order and Optimize Write.
# Run this cell.

# CELL ********************

spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fact - Sale
# This cell reads raw parquet data from the `wwi-raw-data` folder, adds date part columns (`Year`, `Quarter`, and `Month`), and writes the data as a Delta table partitioned by `Year` and `Quarter`.
# Run this cell.

# CELL ********************

from pyspark.sql.functions import col, year, month, quarter, rand

table_name = 'fact_sale'
target_rows = 1_000_000

df = spark.read.format("parquet").load('Files/wwi-raw-data/parquet/full/fact_sale_1y_full')

# Parquet row counts come from file metadata, so this is cheap
total_rows = df.count()

# Oversample by 5%, then trim to exactly 5M
fraction = min(1.0, (target_rows / total_rows) * 1.05)
df = (df.sample(withReplacement=False, fraction=fraction, seed=42)
        .orderBy(rand(seed=42))
        .limit(target_rows))

df = df.withColumn('Year', year(col("InvoiceDateKey")))
df = df.withColumn('Quarter', quarter(col("InvoiceDateKey")))
df = df.withColumn('Month', month(col("InvoiceDateKey")))

df.write.mode("overwrite").format("delta").partitionBy("Year","Quarter").save("Tables/" + table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, year, month, quarter

table_name = 'fact_sale'

df = spark.read.format("parquet").load('Files/wwi-raw-data/parquet/full/fact_sale_1y_full')
df = df.withColumn('Year', year(col("InvoiceDateKey")))
df = df.withColumn('Quarter', quarter(col("InvoiceDateKey")))
df = df.withColumn('Month', month(col("InvoiceDateKey")))

df.write.mode("overwrite").format("delta").partitionBy("Year","Quarter").save("Tables/" + table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ### Dimensions
# This cell loads the dimension tables that provide descriptive context for the fact table. It defines a function that reads raw parquet data, drops the unused `Photo` column, and writes each table as a Delta table.
# It then loops through `dimension_city`, `dimension_customer`, `dimension_date`, `dimension_employee`, and `dimension_stock_item` to create Delta tables.
# Run this cell.

# CELL ********************

from pyspark.sql.types import *

def loadFullDataFromSource(table_name):
    df = spark.read.format("parquet").load('Files/wwi-raw-data/parquet/full/' + table_name)
    df = df.drop("Photo")
    df.write.mode("overwrite").format("delta").save("Tables/" + table_name)

full_tables = [
    'dimension_city',
    'dimension_customer',
    'dimension_date',
    'dimension_employee',
    'dimension_stock_item'
    ]

for table in full_tables:
    loadFullDataFromSource(table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
