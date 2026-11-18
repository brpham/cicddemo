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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.parquet("Files/wwi-raw-data/parquet/full/fact_sale/*.snappy.parquet")
# df now is a Spark DataFrame containing parquet data from "Files/wwi-raw-data/parquet/full/fact_sale/part-00000-ced648ca-e8c8-46e5-8526-5ca85d56e67e-c000.snappy.parquet".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfull = spark.read.parquet("Files/wwi-raw-data/parquet/full/fact_sale_1y_full/*.snappy.parquet")
# df now is a Spark DataFrame containing parquet data from "Files/wwi-raw-data/parquet/full/fact_sale/part-00000-ced648ca-e8c8-46e5-8526-5ca85d56e67e-c000.snappy.parquet".
display(dfull)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfull.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = spark.sql("SELECT * FROM wwilakehouse.fact_sale")
display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
