# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze ingestion
# MAGIC
# MAGIC Loads the raw NASA C-MAPSS files from the landing volume into Delta tables, with
# MAGIC as little change as possible:
# MAGIC
# MAGIC | Table | Contents |
# MAGIC |---|---|
# MAGIC | `pdm_bronze.engine_cycles` | One row per engine per operating cycle (train and test files) |
# MAGIC | `pdm_bronze.test_true_rul` | True remaining useful life for each test engine |
# MAGIC | `pdm_bronze.engine_cycles_rejects` | Lines that could not be parsed, with the reason |
# MAGIC
# MAGIC **Bronze rules used here**
# MAGIC - Keep every value as delivered; only split the line and set types.
# MAGIC - Add lineage: `dataset`, `split`, `source_file`, `ingested_at`.
# MAGIC - Never drop bad lines silently: they go to the rejects table.
# MAGIC - Full reload on each run (the source is a static benchmark), so re-running is safe.

# COMMAND ----------

import os
import sys

# Make src/ importable. Works when the repo is cloned as a Databricks Git folder.
src_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if src_dir not in sys.path:
    sys.path.append(src_dir)

from pyspark.sql import functions as F

import cmapss_parsing as cp

dbutils.widgets.text("catalog", "workspace", "Catalog")
catalog = dbutils.widgets.get("catalog")
landing = f"/Volumes/{catalog}/pdm_bronze/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Engine cycles (train and test files)

# COMMAND ----------

raw = (
    spark.read.text(f"{landing}/*_FD00*.txt")
    .select("value", F.col("_metadata.file_path").alias("source_file"))
    .filter(~F.col("source_file").contains("RUL_"))      # RUL files are handled below
    .filter(F.trim("value") != "")                        # ignore blank lines
)

cycles, rejects = cp.parse_cycles(raw)

(cycles.write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(cp.fq(catalog, "pdm_bronze", "engine_cycles")))
(rejects.write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(cp.fq(catalog, "pdm_bronze", "engine_cycles_rejects")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. True RUL for test engines
# MAGIC
# MAGIC Line N of `RUL_FD00x.txt` is the true RUL of test engine N. The file has no engine
# MAGIC number, so line order is the key, and it is read with plain Python to keep that order.

# COMMAND ----------

rul_paths = [os.path.join(landing, f.name) for f in dbutils.fs.ls(landing) if f.name.startswith("RUL_")]
rul = cp.rul_dataframe(spark, cp.rul_rows_from_files(rul_paths))

(rul.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(cp.fq(catalog, "pdm_bronze", "test_true_rul")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Checks
# MAGIC
# MAGIC The load **fails** (raises an error) if lines were rejected or if any test engine is
# MAGIC missing a true RUL value. A difference from the readme's engine counts is **reported**
# MAGIC but does not fail the load, because the files themselves are the source of truth.

# COMMAND ----------

cycles_t = spark.table(cp.fq(catalog, "pdm_bronze", "engine_cycles"))
rul_t = spark.table(cp.fq(catalog, "pdm_bronze", "test_true_rul"))
rejects_t = spark.table(cp.fq(catalog, "pdm_bronze", "engine_cycles_rejects"))

counts = cp.unit_counts(cycles_t)
display(counts)

n_rejects = rejects_t.count()
missing_rul = cp.rul_consistency(cycles_t, rul_t)
n_missing = missing_rul.count()
mismatches = counts.filter(~F.coalesce(F.col("matches_readme"), F.lit(False))).collect()

print(f"Rows loaded      : {cycles_t.count():,}")
print(f"Rejected lines   : {n_rejects}")
print(f"Test RUL problems: {n_missing}")
for r in mismatches:
    print(f"Readme mismatch  : {r['dataset']} {r['split']}: readme says {r['readme_units']}, "
          f"files contain {r['actual_units']}")

if n_rejects:
    display(rejects_t.limit(20))
    raise ValueError(f"{n_rejects} lines were rejected; see pdm_bronze.engine_cycles_rejects")
if n_missing:
    display(missing_rul)
    raise ValueError("Test engines and true RUL values do not line up")

print("Bronze load OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Table documentation
# MAGIC
# MAGIC Comments show up in Catalog Explorer, so anyone browsing the catalog sees what the
# MAGIC tables are without opening this notebook.

# COMMAND ----------

t = cp.fq(catalog, "pdm_bronze", "engine_cycles")
spark.sql(f"COMMENT ON TABLE {t} IS 'NASA C-MAPSS engine cycles as delivered, one row per engine per cycle. Source: NASA PCoE. Simulated data.'")
spark.sql(f"ALTER TABLE {t} ALTER COLUMN unit_id COMMENT 'Engine number, unique within dataset and split'")
spark.sql(f"ALTER TABLE {t} ALTER COLUMN cycle COMMENT 'Operating cycle number, starting at 1'")

t = cp.fq(catalog, "pdm_bronze", "test_true_rul")
spark.sql(f"COMMENT ON TABLE {t} IS 'True remaining useful life in cycles for each test engine, after its last recorded cycle'")
