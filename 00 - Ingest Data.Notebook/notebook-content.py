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

# Ingest WWI sample data from public Blob storage into default lakehouse Files,
# mimicking the pipeline tutorial structure.

# Source details from tutorial
storage_account = "fabrictutorialdata"
container = "sampledata"
source_folder = "WideWorldImportersDW/parquet"  # recursive

# Target in the *default lakehouse* Files section
# This will create: Files/wwi-raw-data/WideWorldImportersDW/parquet/...
# (Fixed: removed unintended leading space before variable name that caused IndentationError)
target_folder = "Files/wwi-raw-data"

# 1) Configure Spark to access the public Azure Blob container anonymously via ABFS
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "Anonymous")
spark.conf.set(f"fs.azure.skipCreateContainerCheck.{storage_account}.dfs.core.windows.net", "true")

# 2) Build ABFS source path (equivalent to the pipeline settings)
abfs_source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{source_folder}"

print("Source (ABFS):", abfs_source)
print("Target (Lakehouse Files):", target_folder)

# 3) Copy recursively from the external container into the default lakehouse Files
#    This preserves the folder hierarchy under WideWorldImportersDW/parquet.

# IMPORTANT: notebookutils.fs.fastcp understands both ABFS (source) and
# lakehouse-relative (target) paths. "Files/..." points to the default lakehouse Files.

notebookutils.fs.fastcp(
    src=abfs_source,
    dest=target_folder,
    recurse=True  # copy all subfolders/files recursively
)

# Optional: list a few files to verify
for entry in notebookutils.fs.ls(target_folder)[:5]:
    print(entry.path, entry.size)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
