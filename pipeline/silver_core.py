"""SILVER: lõi tích hợp chuẩn hoá 3NF (tư duy Inmon).

Thực thể: SITE, TEAM, CROP, COMPARTMENT, GROWING_CYCLE (1 cycle = 1 vụ của 1 khoang) và các
bảng quan sát/sự kiện gắn với cycle: CLIMATE_OBSERVATION, ROOTZONE_OBSERVATION, RESOURCE_DAY,
HARVEST, CROP_MEASUREMENT, FRUIT_QUALITY, LAB_ANALYSIS (dạng dài), WEATHER_OBSERVATION (gắn site).

Quy tắc làm sạch (mọi bước đều được đếm để đối soát):
  1. Parse thời gian: Excel serial (ngày kể từ 1899-12-30) hoặc chuỗi ngày giờ; làm tròn 5 phút.
  2. Không parse được thời gian / nằm ngoài cửa sổ thí nghiệm  -> QUARANTINE (giữ nguyên bản ghi gốc).
  3. Trùng khoá (cycle, ts) hoặc (cycle, date) -> giữ bản ghi đầu tiên theo _row_id.
  4. Giá trị ngoài ngưỡng vật lý -> NULL, ghi nhận vào DQ_ISSUE.
  5. Kiểu dữ liệu: số -> DOUBLE; đơn vị giữ như nguồn ở core (MJ/m², kWh/m², L/m², kg/m²).
"""
from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.common import EXCEL_TO_UNIX_DAYS, Lake, project_config

STRING_TIME_FORMATS = [
    "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd",
    "dd-MM-yyyy HH:mm:ss", "dd-MM-yyyy HH:mm", "dd-MM-yyyy", "dd/MM/yyyy HH:mm", "dd/MM/yyyy",
    "M/d/yyyy H:mm", "M/d/yyyy",
]

# silver_column -> danh sách tên cột bronze ứng viên (sau normalize_column)
CLIMATE_COLS = {
    "tair": ["tair"], "rh": ["rhair"], "co2": ["co2air"], "hum_def": ["humdef"],
    "pipe_low": ["pipelow"], "pipe_grow": ["pipegrow"], "par_total": ["tot_par"],
    "par_lamps": ["tot_par_lamps"], "assim_light": ["assimlight"], "vent_lee": ["ventlee"],
    "vent_wind": ["ventwind"], "energy_screen": ["enscr"], "blackout_screen": ["blackscr"],
    "ec_drain": ["ec_drain_pc"], "ph_drain": ["ph_drain_pc"], "water_sup": ["water_sup"],
    "cum_irr": ["cum_irr"], "t_heat_sp": ["t_heat_sp"], "t_heat_vip": ["t_heat_vip"],
    "t_vent_sp": ["t_vent_sp", "t_ventlee_sp"], "t_vent_vip": ["t_ventlee_vip", "t_ventwind_vip"],
    "co2_sp": ["co2_sp"], "co2_vip": ["co2_vip"], "co2_dosing": ["co2_dos"],
}
WEATHER_COLS = {
    "tout": ["tout"], "rhout": ["rhout"], "iglob": ["iglob"], "par_out": ["parout"],
    "rain": ["rain"], "wind_speed": ["windsp"], "wind_dir": ["winddir"],
    "abs_hum_out": ["abshumout"], "rad_sum": ["radsum"], "pyrgeo": ["pyrgeo"],
}
ROOTZONE_COLS = {
    "ec_slab1": ["ec_slab1"], "ec_slab2": ["ec_slab2"], "wc_slab1": ["wc_slab1"],
    "wc_slab2": ["wc_slab2"], "t_slab1": ["t_slab1"], "t_slab2": ["t_slab2"],
}
RESOURCE_COLS = {
    "heat_mj_m2": ["heat_cons"], "elec_high_kwh_m2": ["elechigh"], "elec_low_kwh_m2": ["eleclow"],
    "co2_kg_m2": ["co2_cons"], "irr_l_m2": ["irr"], "drain_l_m2": ["drain"],
}
HARVEST_COLS = {
    "prod_a_kg_m2": ["proda"], "prod_b_kg_m2": ["prodb"],
    "avg_nr_harvested_trays": ["avg_nr_harvested_trays"],
    "truss_dev_time_days": ["truss_development_time"],
    "nr_fruits_class_a": ["nr_fruits_classa"], "weight_fruits_class_a_g": ["weight_fruits_classa"],
    "nr_fruits_class_b": ["nr_fruits_classb"], "weight_fruits_class_b_g": ["weight_fruits_classb"],
}
CROP_COLS = {
    "stem_elong_cm": ["stem_elong"], "stem_thick_mm": ["stem_thick"], "cum_trusses": ["cum_trusses"],
    "stem_density": ["stem_dens"], "plant_density": ["plant_dens"],
}
QUALITY_COLS = {
    "flavour": ["flavour"], "tss_brix": ["tss"], "acid": ["acid"], "juice_pct": ["juice"],
    "bite": ["bite"], "fruit_weight_g": ["weight"], "dmc_pct": ["dmc_fruit"],
}


def parse_time(raw: F.Column, round_minutes: int = 5) -> F.Column:
    step = round_minutes * 60
    num = raw.cast("double")
    serial = F.when(
        num.between(20000, 80000),
        F.timestamp_seconds((F.round((num - EXCEL_TO_UNIX_DAYS) * 86400 / step) * step).cast("long")),
    )
    text = F.coalesce(*[F.to_timestamp(F.trim(raw), fmt) for fmt in STRING_TIME_FORMATS])
    return F.coalesce(serial, text)


def pick(df: DataFrame, candidates: list[str]) -> F.Column:
    for c in candidates:
        if c in df.columns:
            v = F.trim(F.col(c))
            return F.when(F.lower(v).isin("nan", "", "null", "na", "-"), None).otherwise(v).cast("double")
    return F.lit(None).cast("double")


def time_column(df: DataFrame) -> str:
    for c in ("time", "datetime", "date", "timestamp"):
        if c in df.columns:
            return c
    return df.columns[0]


class SilverBuilder:
    def __init__(self, spark: SparkSession, lake: Lake, timer):
        self.spark, self.lake, self.timer = spark, lake, timer
        self.cfg = project_config()
        self.start = self.cfg["experiment"]["start_date"]
        self.end = self.cfg["experiment"]["end_date"]
        self.ranges = self.cfg["plausible_ranges"]
        self.quarantine: list[DataFrame] = []
        self.dq: list[DataFrame] = []
        self.stats: dict[str, dict] = {}
        self.cycles: DataFrame | None = None

    # ---------------------------------------------------------------- helpers
    def bronze(self, table: str) -> DataFrame | None:
        try:
            return self.spark.read.parquet(self.lake.path("bronze", table))
        except Exception:  # bảng không có trong nguồn
            print(f"  (không có bronze.{table})")
            return None

    def write(self, df: DataFrame, table: str, parts: int = 2) -> int:
        df.coalesce(parts).write.mode("overwrite").parquet(self.lake.path("silver", table))
        return self.spark.read.parquet(self.lake.path("silver", table)).count()

    def _clean(self, table: str, src: str, df: DataFrame, cols: dict, keys: list[str],
               daily: bool) -> DataFrame:
        """Parse thời gian, quarantine, khử trùng, kiểm tra ngưỡng. Trả về DataFrame sạch."""
        tcol = time_column(df)
        df = df.withColumn("_ts", parse_time(F.col(tcol)))
        if daily:
            df = df.withColumn("_date", F.to_date("_ts"))
        bronze_rows = df.count()

        when_bad = (
            F.when(F.col("_ts").isNull(), F.lit("unparseable_time"))
            .when((F.to_date("_ts") < F.lit(self.start)) | (F.to_date("_ts") > F.lit(self.end)),
                  F.lit("outside_experiment_window"))
        )
        df = df.withColumn("_reason", when_bad)
        meta_cols = [c for c in df.columns if not c.startswith("_")]
        q = df.where(F.col("_reason").isNotNull()).select(
            F.lit(table).alias("target_table"), F.lit(src).alias("source_table"),
            F.col("_team").alias("team_code"), F.col("_source_file").alias("source_file"),
            F.col(tcol).cast("string").alias("raw_time"), F.col("_reason").alias("reason"),
            F.to_json(F.struct(*meta_cols)).alias("raw_record"),
        )
        self.quarantine.append(q)
        quarantined = q.count()
        df = df.where(F.col("_reason").isNull())

        sel = [F.col(k) for k in ("_team", "_ts", "_date", "_row_id", "_source_file") if k in df.columns]
        sel += [pick(df, cand).alias(name) for name, cand in cols.items()]
        df = df.select(*sel)

        w = Window.partitionBy(*keys).orderBy("_row_id")
        before_dedup = df.count()
        df = df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
        after_dedup = df.count()

        # Kiểm tra ngưỡng vật lý: đếm trên dữ liệu TRƯỚC khi NULL hoá, rồi mới NULL hoá
        bad_cond = {n: F.col(n).isNotNull() & ~F.col(n).between(*self.ranges[n])
                    for n in cols if n in self.ranges}
        agg_exprs = [F.sum(F.when(c, 1).otherwise(0)).alias(f"oor__{n}") for n, c in bad_cond.items()]
        # null_value = rỗng ở nguồn (chưa tính các giá trị bị NULL hoá do ngoài ngưỡng)
        agg_exprs += [F.sum(F.when(F.col(n).isNull(), 1).otherwise(0)).alias(f"null__{n}") for n in cols]
        agg = df.groupBy("_team").agg(*agg_exprs, F.count("*").alias("_rows"))
        stack = [(n, "out_of_range", f"oor__{n}") for n in bad_cond] + \
                [(n, "null_value", f"null__{n}") for n in cols]
        exprs = ", ".join(f"'{n}', '{kind}', CAST(`{c}` AS BIGINT)" for n, kind, c in stack)
        dq = agg.selectExpr("_team", "_rows", f"stack({len(stack)}, {exprs}) as (column_name, issue, n_rows)")
        self.dq.append(dq.select(F.lit(table).alias("table_name"), F.col("_team").alias("team_code"),
                                 "column_name", "issue", "n_rows", F.col("_rows").alias("table_rows")))
        for n, c in bad_cond.items():
            df = df.withColumn(n, F.when(c, None).otherwise(F.col(n)))

        self.stats[table] = {
            "bronze_rows": bronze_rows, "quarantined": quarantined,
            "duplicates_removed": before_dedup - after_dedup, "silver_rows": after_dedup,
        }
        return df

    # ---------------------------------------------------------------- reference entities
    def build_reference(self) -> None:
        cfg = self.cfg
        teams = {}
        for table in ("greenhouse_climate", "resources", "production"):
            b = self.bronze(table)
            if b is not None:
                for r in b.select("_team").distinct().collect():
                    teams[r["_team"]] = True
        team_cfg = {t["team_code"]: t for t in cfg["teams"]}
        rows_team, rows_comp, rows_cycle = [], [], []
        for i, code in enumerate(sorted(teams), start=1):
            base, _, rep = code.partition("_r")
            t = team_cfg.get(base, {"team_name": base, "control_type": "Unknown",
                                    "compartment_code": f"X{i:02d}", "floor_area_m2": 96})
            comp_code = t["compartment_code"] + (f"_r{rep}" if rep else "")
            rows_team.append((i, code, t["team_name"] + (f" (replica {rep})" if rep else ""), t["control_type"]))
            rows_comp.append((i, cfg["site"]["site_id"], comp_code, float(t["floor_area_m2"])))
            rows_cycle.append((i, i, i, cfg["crop"]["crop_id"], f"AGC2019-{code}",
                               cfg["experiment"]["start_date"], cfg["experiment"]["end_date"], bool(rep)))
        sp = self.spark
        site = cfg["site"]
        frames = {
            "site": sp.createDataFrame([(site["site_id"], site["site_code"], site["site_name"], site["country"],
                                         float(site["latitude"]), float(site["longitude"]))],
                                       "site_id int, site_code string, site_name string, country string, latitude double, longitude double"),
            "crop": sp.createDataFrame([(cfg["crop"]["crop_id"], cfg["crop"]["species"], cfg["crop"]["cultivar"])],
                                       "crop_id int, species string, cultivar string"),
            "team": sp.createDataFrame(rows_team, "team_id int, team_code string, team_name string, control_type string"),
            "compartment": sp.createDataFrame(rows_comp, "compartment_id int, site_id int, compartment_code string, floor_area_m2 double"),
            "growing_cycle": sp.createDataFrame(rows_cycle, "cycle_id int, compartment_id int, team_id int, crop_id int, "
                                                            "cycle_code string, start_date string, end_date string, is_replica boolean")
            .withColumn("start_date", F.to_date("start_date")).withColumn("end_date", F.to_date("end_date")),
        }
        for name, df in frames.items():
            with self.timer.stage(f"silver.{name}") as rec:
                rec["rows"] = self.write(df, name, 1)
        self.cycles = frames["growing_cycle"].join(frames["team"], "team_id").select("cycle_id", "team_code")

    def _with_cycle(self, df: DataFrame) -> DataFrame:
        return df.join(F.broadcast(self.cycles), df["_team"] == self.cycles["team_code"]).drop("team_code")

    # ---------------------------------------------------------------- observation tables
    def build_timeseries(self, table: str, src: str, cols: dict) -> None:
        b = self.bronze(src)
        if b is None:
            return
        with self.timer.stage(f"silver.{table}") as rec:
            df = self._clean(table, src, b, cols, ["_team", "_ts"], daily=False)
            df = self._with_cycle(df).select("cycle_id", F.col("_ts").alias("ts"), *cols.keys(),
                                             F.col("_source_file").alias("source_file"))
            rec["rows"] = self.write(df, table, 4)

    def build_weather(self) -> None:
        b = self.bronze("weather")
        if b is None:
            return
        b = b.withColumn("_team", F.lit("SITE"))
        with self.timer.stage("silver.weather_observation") as rec:
            df = self._clean("weather_observation", "weather", b, WEATHER_COLS, ["_ts"], daily=False)
            df = df.select(F.lit(self.cfg["site"]["site_id"]).alias("site_id"), F.col("_ts").alias("ts"),
                           *WEATHER_COLS.keys(), F.col("_source_file").alias("source_file"))
            rec["rows"] = self.write(df, "weather_observation", 2)

    def build_daily(self, table: str, src: str, cols: dict, date_name: str) -> None:
        b = self.bronze(src)
        if b is None:
            return
        with self.timer.stage(f"silver.{table}") as rec:
            df = self._clean(table, src, b, cols, ["_team", "_date"], daily=True)
            df = self._with_cycle(df).select("cycle_id", F.col("_date").alias(date_name), *cols.keys(),
                                             F.col("_source_file").alias("source_file"))
            rec["rows"] = self.write(df, table, 1)

    def build_lab(self) -> None:
        b = self.bronze("lab_analysis")
        if b is None:
            return
        with self.timer.stage("silver.lab_analysis") as rec:
            analytes = [c for c in b.columns if not c.startswith("_") and c != time_column(b)]
            cols = {c: [c] for c in analytes}
            df = self._clean("lab_analysis", "lab_analysis", b, cols, ["_team", "_date"], daily=True)
            df = self._with_cycle(df)
            exprs = ", ".join(f"'{c}', `{c}`" for c in analytes)
            long = df.selectExpr("cycle_id", "_date as sample_date", "_source_file as source_file",
                                 f"stack({len(analytes)}, {exprs}) as (measure, value)")
            long = (long.where(F.col("value").isNotNull())
                    .withColumn("sample_type", F.when(F.col("measure").startswith("irr"), "irrigation")
                                .when(F.col("measure").startswith("drain"), "drain").otherwise("other"))
                    .withColumn("analyte", F.regexp_replace("measure", r"^(irr|drain)_", ""))
                    .select("cycle_id", "sample_date", "sample_type", "analyte", "value", "source_file"))
            rec["rows"] = self.write(long, "lab_analysis", 1)

    def finish(self) -> None:
        with self.timer.stage("silver.quarantine") as rec:
            q = reduce(lambda a, b: a.unionByName(b), self.quarantine)
            rec["rows"] = self.write(q, "quarantine", 1)
        with self.timer.stage("silver.dq_issue") as rec:
            dq = reduce(lambda a, b: a.unionByName(b), self.dq).where("n_rows > 0")
            rec["rows"] = self.write(dq, "dq_issue", 1)
        rows = [(k, v["bronze_rows"], v["quarantined"], v["duplicates_removed"], v["silver_rows"])
                for k, v in self.stats.items()]
        st = self.spark.createDataFrame(rows, T.StructType([
            T.StructField("table_name", T.StringType()), T.StructField("bronze_rows", T.LongType()),
            T.StructField("quarantined", T.LongType()), T.StructField("duplicates_removed", T.LongType()),
            T.StructField("silver_rows", T.LongType())]))
        self.write(st, "load_stats", 1)


def run(spark: SparkSession, lake: Lake, timer) -> dict:
    sb = SilverBuilder(spark, lake, timer)
    sb.build_reference()
    sb.build_timeseries("climate_observation", "greenhouse_climate", CLIMATE_COLS)
    sb.build_timeseries("rootzone_observation", "grodan_sens", ROOTZONE_COLS)
    sb.build_weather()
    sb.build_daily("resource_day", "resources", RESOURCE_COLS, "date")
    sb.build_daily("harvest", "production", HARVEST_COLS, "harvest_date")
    sb.build_daily("crop_measurement", "crop_parameters", CROP_COLS, "date")
    sb.build_daily("fruit_quality", "tom_quality", QUALITY_COLS, "date")
    sb.build_lab()
    sb.finish()
    return sb.stats
