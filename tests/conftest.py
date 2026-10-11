"""Pytest fixtures for RetailDataOps test suite."""

from __future__ import annotations

import os
import subprocess
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Set up dummy winutils.exe on Windows using csc.exe if missing
if os.name == "nt":
    hadoop_dir = Path(tempfile.gettempdir()) / "hadoop_home"
    bin_dir = hadoop_dir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    exe_file = bin_dir / "winutils.exe"

    if not exe_file.exists() or exe_file.stat().st_size < 100:
        cs_file = bin_dir / "winutils.cs"
        cs_file.write_text(
            "class Program {" " static int Main(string[] args) { return 0; } }"
        )
        csc = r"C:\Windows\Microsoft.NET" r"\Framework64\v4.0.30319\csc.exe"
        if Path(csc).exists():
            subprocess.run(
                [csc, f"/out:{exe_file}", str(cs_file)],
                capture_output=True,
                check=False,
            )

    os.environ["HADOOP_HOME"] = str(hadoop_dir)


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """Create a local SparkSession configured for Delta Lake for testing."""
    builder = (
        SparkSession.builder.appName("RetailDataOps-Test")
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.fs.nativeio.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()
