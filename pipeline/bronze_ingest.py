"""BRONZE: nạp nguyên trạng 43 CSV vào lake (Parquet), mọi cột giữ kiểu chuỗi.

* Không sửa giá trị — chỉ chuẩn hoá tên cột và gắn metadata nguồn (_team, _source_file,
  _ingest_ts, _row_id) để truy vết (lineage) từ Gold ngược về file gốc.
* scale_factor > 1: nhân bản dữ liệu mỗi team thành các "khoang ảo" (AICU_r2, ...) để
  benchmark khả năng mở rộng trong bối cảnh Big Data.
"""
from __future__ import annotations

import re
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.common import Lake, normalize_column, project_config


def _team_lookup(cfg: dict) -> dict[str, str]:
    return {re.sub(r"[^a-z0-9]", "", t["team_code"].lower()): t["team_code"] for t in cfg["teams"]}


def discover_files(raw_dir: Path, cfg: dict) -> list[dict]:
    teams = _team_lookup(cfg)
    files = []
    for csv in sorted(raw_dir.rglob("*.csv")):
        stem = re.sub(r"[^a-z0-9]", "", csv.stem.lower())
        table = cfg["source_files"].get(stem)
        if table is None:
            print(f"  skip (không thuộc schema nguồn): {csv}")
            continue
        team = None
        for parent in csv.relative_to(raw_dir).parents:
            key = re.sub(r"[^a-z0-9]", "", parent.name.lower())
            if key in teams:
                team = teams[key]
                break
        if table != "weather" and team is None:
            print(f"  skip (không xác định được team): {csv}")
            continue
        files.append({"path": csv, "table": table, "team": team,
                      "rel": csv.relative_to(raw_dir).as_posix()})
    return files


def read_one(spark: SparkSession, f: dict) -> DataFrame:
    df = (spark.read.option("header", True).option("inferSchema", False)
          .option("encoding", "UTF-8").csv(str(f["path"])))
    seen: dict[str, int] = {}
    cols = []
    for c in df.columns:
        n = normalize_column(c)
        if n in seen:  # tránh trùng tên sau khi chuẩn hoá
            seen[n] += 1
            n = f"{n}_{seen[n]}"
        else:
            seen[n] = 0
        cols.append(F.col(f"`{c}`").alias(n))
    return (df.select(*cols)
            .withColumn("_team", F.lit(f["team"]).cast("string"))
            .withColumn("_source_file", F.lit(f["rel"]))
            .withColumn("_row_id", F.monotonically_increasing_id())
            .withColumn("_ingest_ts", F.current_timestamp()))


def replicate(df: DataFrame, scale_factor: int) -> DataFrame:
    if scale_factor <= 1:
        return df
    copies = [df] + [
        df.withColumn("_team", F.concat(F.col("_team"), F.lit(f"_r{i}")))
        for i in range(2, scale_factor + 1)
    ]
    return reduce(lambda a, b: a.unionByName(b), copies)


def run(spark: SparkSession, raw_dir: str, lake: Lake, timer, scale_factor: int = 1) -> dict:
    cfg = project_config()
    raw = Path(raw_dir).resolve()
    files = discover_files(raw, cfg)
    if not files:
        raise SystemExit(f"Không tìm thấy CSV nguồn trong {raw}. Hãy giải nén dữ liệu Kaggle vào đây "
                         f"hoặc chạy: python -m tools.make_sample_data --out data/sample_raw")
    print(f"Bronze: tìm thấy {len(files)} file CSV trong {raw}")
    stats = {}
    by_table: dict[str, list[dict]] = {}
    for f in files:
        by_table.setdefault(f["table"], []).append(f)
    for table, group in sorted(by_table.items()):
        with timer.stage(f"bronze.{table}", files=len(group)) as rec:
            dfs = [read_one(spark, f) for f in group]
            df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
            if table != "weather":
                df = replicate(df, scale_factor)
            df.coalesce(4).write.mode("overwrite").parquet(lake.path("bronze", table))
            rec["rows"] = spark.read.parquet(lake.path("bronze", table)).count()
            stats[table] = {"files": len(group), "rows": rec["rows"]}
    return stats
