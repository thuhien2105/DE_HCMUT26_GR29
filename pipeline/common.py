"""Tiện ích dùng chung cho pipeline Spark: cấu hình, SparkSession, đường dẫn lake."""
from __future__ import annotations

import json
import os
import re
import socket
import time
from contextlib import contextmanager
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / "config"
DEFAULT_LAKE = PROJECT_ROOT / "data" / "lake"

# Số ngày từ mốc Excel 1899-12-30 tới Unix epoch 1970-01-01
EXCEL_TO_UNIX_DAYS = 25569


def load_yaml(name: str) -> dict:
    with open(CONFIG_DIR / name, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def project_config() -> dict:
    return load_yaml("project.yaml")


def rule_config() -> dict:
    return load_yaml("rules.yaml")


def normalize_column(name: str) -> str:
    """'%time' -> 'time', 'EC_drain_PC' -> 'ec_drain_pc', 'Truss development time' -> 'truss_development_time'."""
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    return name or "unnamed"


class Lake:
    """Bố cục data lake theo kiến trúc Medallion: bronze / silver / gold."""

    def __init__(self, root: str | os.PathLike | None = None):
        self.root = Path(root or os.environ.get("LAKE_DIR", DEFAULT_LAKE)).resolve()

    def path(self, layer: str, table: str) -> str:
        return str(self.root / layer / table)

    @property
    def metrics_dir(self) -> Path:
        p = self.root / "_metrics"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def relative_to_data(self, layer: str, table: str) -> str:
        """Đường dẫn ClickHouse nhìn thấy (user_files/data/...)."""
        data_root = PROJECT_ROOT / "data"
        rel = (self.root / layer / table).relative_to(data_root)
        return f"data/{rel.as_posix()}"


def get_spark(app_name: str, master: str | None = None):
    from pyspark.sql import SparkSession

    master = master or os.environ.get("SPARK_MASTER_URL", "local[*]")
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
    )
    if master.startswith("spark://"):
        # Executor ở container khác cần kết nối ngược về driver
        builder = (
            builder.config("spark.driver.host", socket.gethostbyname(socket.gethostname()))
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "1g"))
            .config("spark.executor.cores", os.environ.get("SPARK_EXECUTOR_CORES", "2"))
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


class StageTimer:
    """Đo thời gian + số dòng từng bước, ghi ra JSON để đánh giá hiệu năng."""

    def __init__(self, run_name: str):
        self.run_name = run_name
        self.started = time.time()
        self.stages: list[dict] = []

    @contextmanager
    def stage(self, name: str, **extra):
        t0 = time.time()
        rec = {"stage": name, **extra}
        try:
            yield rec
        finally:
            rec["seconds"] = round(time.time() - t0, 3)
            self.stages.append(rec)
            rows = f" rows={rec['rows']}" if "rows" in rec else ""
            print(f"[{self.run_name}] {name}: {rec['seconds']}s{rows}", flush=True)

    def dump(self, lake: Lake, **meta) -> Path:
        out = {
            "run_name": self.run_name,
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.started)),
            "total_seconds": round(time.time() - self.started, 3),
            "stages": self.stages,
            **meta,
        }
        path = lake.metrics_dir / f"{self.run_name}_{time.strftime('%Y%m%d_%H%M%S')}.json"
        path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
        latest = lake.metrics_dir / f"{self.run_name}_latest.json"
        latest.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
        return path
