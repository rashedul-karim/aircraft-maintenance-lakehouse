# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC
# MAGIC Creates the Unity Catalog objects the pipeline uses. Safe to run more than once.
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |---|---|
# MAGIC | `<catalog>.pdm_bronze` | Raw data loaded as-is, plus lineage columns |
# MAGIC | `<catalog>.pdm_silver` | Cleaned and validated data (week 2) |
# MAGIC | `<catalog>.pdm_gold` | Features and fleet views (week 3) |
# MAGIC | `<catalog>.pdm_bronze.landing` | Volume where you upload the NASA files |
# MAGIC
# MAGIC In Databricks Free Edition the default catalog is usually `workspace`. If yours has a
# MAGIC different name, change the `catalog` widget at the top of this notebook.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

for schema in ("pdm_bronze", "pdm_silver", "pdm_gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog}.pdm_bronze.landing
    COMMENT 'Raw NASA C-MAPSS text files, uploaded by hand'
""")

display(spark.sql(f"SHOW SCHEMAS IN {catalog} LIKE 'pdm_*'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next step
# MAGIC
# MAGIC Upload the 12 data files from `CMAPSSData.zip` (`train_FD00*.txt`, `test_FD00*.txt`,
# MAGIC `RUL_FD00*.txt`) to the volume **pdm_bronze → landing** in Catalog Explorer, then run
# MAGIC `01_bronze_ingest`. Full steps are in `docs/setup.md`.

# COMMAND ----------

landing = f"/Volumes/{catalog}/pdm_bronze/landing"
files = sorted(f.name for f in dbutils.fs.ls(landing))
print(f"{len(files)} files in {landing}")
for name in files:
    print(" ", name)
