"""Unit tests for src/cmapss_parsing.py, run locally with PySpark.

    pip install -r requirements-dev.txt
    pytest -q
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import cmapss_parsing as cp  # noqa: E402

GOOD_LINE = "1 1 -0.0007 -0.0004 100.0 " + " ".join(["518.67"] * 21) + "  "   # trailing spaces as in NASA files


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def raw_df(spark, rows):
    return spark.createDataFrame(rows, "value string, source_file string")


def test_good_line_is_parsed_with_lineage(spark):
    good, rejected = cp.parse_cycles(raw_df(spark, [(GOOD_LINE, "/Volumes/c/s/v/train_FD001.txt")]))
    row = good.collect()[0]
    assert rejected.count() == 0
    assert (row.dataset, row.split, row.source_file) == ("FD001", "train", "train_FD001.txt")
    assert row.unit_id == 1 and row.cycle == 1
    assert row.op_setting_3 == 100.0 and row.sensor_21 == 518.67
    assert len(good.columns) == 4 + 26


def test_wrong_field_count_is_rejected(spark):
    good, rejected = cp.parse_cycles(raw_df(spark, [("1 1 0.1 0.2", "/x/test_FD002.txt")]))
    assert good.count() == 0
    assert rejected.collect()[0].reject_reason == "expected 26 fields, got 4"


def test_non_numeric_value_is_rejected(spark):
    bad = GOOD_LINE.replace("518.67", "N/A", 1)
    good, rejected = cp.parse_cycles(raw_df(spark, [(bad, "/x/train_FD003.txt")]))
    assert good.count() == 0
    assert rejected.collect()[0].reject_reason == "non-numeric value"


def test_unknown_file_name_is_rejected(spark):
    good, rejected = cp.parse_cycles(raw_df(spark, [(GOOD_LINE, "/x/notes.txt")]))
    assert good.count() == 0
    assert rejected.collect()[0].reject_reason == "unexpected file name"


def test_rul_keeps_line_order_as_unit_id(tmp_path):
    f = tmp_path / "RUL_FD001.txt"
    f.write_text("112 \n98 \n\n69 \n")
    rows = cp.rul_rows_from_files([str(f), str(tmp_path / "readme.txt")])
    assert rows == [("FD001", 1, 112, "RUL_FD001.txt"),
                    ("FD001", 2, 98, "RUL_FD001.txt"),
                    ("FD001", 3, 69, "RUL_FD001.txt")]


def test_rul_consistency_finds_missing_values(spark, tmp_path):
    lines = [(GOOD_LINE.replace("1 1 ", f"{u} 1 ", 1), "/x/test_FD001.txt") for u in (1, 2, 3)]
    cycles, _ = cp.parse_cycles(raw_df(spark, lines))
    f = tmp_path / "RUL_FD001.txt"
    f.write_text("10\n20\n")                       # engine 3 has no RUL
    rul = cp.rul_dataframe(spark, cp.rul_rows_from_files([str(f)]))
    problems = cp.rul_consistency(cycles, rul).collect()
    assert [(p.dataset, p.unit_id) for p in problems] == [("FD001", 3)]


def test_unit_counts_flag_readme_mismatch(spark):
    lines = [(GOOD_LINE.replace("1 1 ", f"{u} 1 ", 1), "/x/train_FD001.txt") for u in range(1, 4)]
    cycles, _ = cp.parse_cycles(raw_df(spark, lines))
    row = [r for r in cp.unit_counts(cycles).collect() if (r.dataset, r.split) == ("FD001", "train")][0]
    assert row.actual_units == 3 and row.readme_units == 100 and row.matches_readme is False


def test_fq():
    assert cp.fq("workspace", "pdm_bronze", "t") == "workspace.pdm_bronze.t"
    assert cp.fq(None, "pdm_bronze", "t") == "pdm_bronze.t"
