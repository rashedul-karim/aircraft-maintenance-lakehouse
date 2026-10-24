"""Parsing and validation helpers for the NASA C-MAPSS turbofan dataset.

Kept in a plain Python module (not inside a notebook) so the same code runs in
Databricks and in local unit tests. Only DataFrame APIs are used, no RDDs, so it
works on Databricks serverless compute (Spark Connect).
"""

from __future__ import annotations

import re
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# 26 space-separated columns per row: unit, cycle, 3 operational settings, 21 sensors.
# (The dataset readme says "sensor measurement 26", but there are 21 sensors: 26 - 5.)
SETTING_COLS = [f"op_setting_{i}" for i in range(1, 4)]
SENSOR_COLS = [f"sensor_{i:02d}" for i in range(1, 22)]
CYCLE_COLS = ["unit_id", "cycle"] + SETTING_COLS + SENSOR_COLS
EXPECTED_FIELDS = len(CYCLE_COLS)  # 26

# Trajectory counts as stated in the dataset's readme.txt.
README_UNITS = {
    ("FD001", "train"): 100, ("FD001", "test"): 100,
    ("FD002", "train"): 260, ("FD002", "test"): 259,
    ("FD003", "train"): 100, ("FD003", "test"): 100,
    ("FD004", "train"): 248, ("FD004", "test"): 249,
}

CYCLE_FILE_RE = re.compile(r"(train|test)_(FD00[1-4])\.txt$")
RUL_FILE_RE = re.compile(r"RUL_(FD00[1-4])\.txt$")


def fq(catalog: str | None, schema: str, table: str) -> str:
    """Fully qualified table name; catalog is optional for local runs."""
    return f"{catalog}.{schema}.{table}" if catalog else f"{schema}.{table}"


def parse_cycles(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Parse raw text lines into typed engine-cycle rows.

    Expects columns `value` (the line) and `source_file` (full path).
    Returns (good_rows, rejected_rows). A row is rejected when it does not have
    exactly 26 numeric fields, or the file name does not match the naming pattern.
    """
    fields = F.split(F.trim(F.col("value")), r"\s+")
    file_name = F.element_at(F.split(F.col("source_file"), "/"), -1)

    staged = (
        raw.withColumn("_fields", fields)
        .withColumn("_n_fields", F.size("_fields"))
        .withColumn("_file_name", file_name)
        .withColumn("_split", F.regexp_extract("_file_name", CYCLE_FILE_RE.pattern, 1))
        .withColumn("_dataset", F.regexp_extract("_file_name", CYCLE_FILE_RE.pattern, 2))
    )

    # try_cast returns NULL instead of failing on bad values, so bad rows can be
    # routed to the rejects table rather than stopping the load.
    typed = staged
    for i, name in enumerate(CYCLE_COLS):
        target = "INT" if name in ("unit_id", "cycle") else "DOUBLE"
        typed = typed.withColumn(name, F.expr(f"try_cast(_fields[{i}] AS {target})"))

    all_numeric = F.lit(True)
    for name in CYCLE_COLS:
        all_numeric = all_numeric & F.col(name).isNotNull()

    is_good = (
        (F.col("_n_fields") == EXPECTED_FIELDS)
        & (F.col("_dataset") != "")
        & all_numeric
    )

    reason = (
        F.when(F.col("_dataset") == "", F.lit("unexpected file name"))
        .when(F.col("_n_fields") != EXPECTED_FIELDS,
              F.concat(F.lit("expected 26 fields, got "), F.col("_n_fields").cast("string")))
        .otherwise(F.lit("non-numeric value"))
    )

    meta = [
        F.col("_dataset").alias("dataset"),
        F.col("_split").alias("split"),
        F.col("_file_name").alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
    ]

    good = typed.filter(is_good).select(*meta, *CYCLE_COLS)
    rejected = typed.filter(~is_good).select(
        F.col("_file_name").alias("source_file"),
        F.col("value").alias("raw_line"),
        reason.alias("reject_reason"),
        F.current_timestamp().alias("ingested_at"),
    )
    return good, rejected


def rul_rows_from_files(paths: Iterable[str]) -> list[tuple[str, int, int, str]]:
    """Read RUL files with plain Python, keeping line order.

    In these files, line N holds the true RUL of test unit N, so the order of
    lines is the key. Plain file reads guarantee that order; Spark text reads do
    not promise it. Works on Databricks because /Volumes paths are POSIX files.
    """
    rows: list[tuple[str, int, int, str]] = []
    for path in sorted(paths):
        name = path.rsplit("/", 1)[-1]
        m = RUL_FILE_RE.search(name)
        if not m:
            continue
        with open(path, encoding="utf-8") as fh:
            values = [line.strip() for line in fh if line.strip()]
        for unit_id, value in enumerate(values, start=1):
            rows.append((m.group(1), unit_id, int(value), name))
    return rows


def rul_dataframe(spark: SparkSession, rows: list[tuple[str, int, int, str]]) -> DataFrame:
    schema = T.StructType([
        T.StructField("dataset", T.StringType(), False),
        T.StructField("unit_id", T.IntegerType(), False),
        T.StructField("true_rul", T.IntegerType(), False),
        T.StructField("source_file", T.StringType(), False),
    ])
    return spark.createDataFrame(rows, schema).withColumn("ingested_at", F.current_timestamp())


def unit_counts(cycles: DataFrame) -> DataFrame:
    """Engines (units) per dataset and split, compared with the readme."""
    spark = cycles.sparkSession
    readme = spark.createDataFrame(
        [(d, s, n) for (d, s), n in README_UNITS.items()],
        "dataset string, split string, readme_units int",
    )
    actual = cycles.groupBy("dataset", "split").agg(
        F.countDistinct("unit_id").alias("actual_units"),
        F.count(F.lit(1)).alias("rows"),
    )
    return (
        readme.join(actual, ["dataset", "split"], "full")
        .withColumn("matches_readme", F.col("readme_units") == F.col("actual_units"))
        .orderBy("dataset", "split")
    )


def rul_consistency(cycles: DataFrame, rul: DataFrame) -> DataFrame:
    """Every test engine needs exactly one true RUL value, and nothing extra."""
    test_units = cycles.filter(F.col("split") == "test").select("dataset", "unit_id").distinct()
    rul_units = rul.select("dataset", "unit_id")
    return (
        test_units.withColumn("in_test", F.lit(True))
        .join(rul_units.withColumn("in_rul", F.lit(True)), ["dataset", "unit_id"], "full")
        .filter(F.col("in_test").isNull() | F.col("in_rul").isNull())
    )
