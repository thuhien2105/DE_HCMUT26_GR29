"""GOLD: dimensional marts (Star schema - tư duy Kimball) sinh từ core Silver.

Conformed dimensions dùng chung cho mọi fact: dim_date, dim_time, dim_cycle, dim_compartment,
dim_rule_version. Facts phục vụ 6 business requirement:

  fact_climate        grain: cycle x 5 phút   -> BR-01, BR-05, BR-06
  fact_rootzone       grain: cycle x 5 phút   -> BR-03
  fact_weather        grain: site  x 5 phút   -> BR-02
  fact_irrigation_day grain: cycle x ngày     -> BR-02
  fact_energy_day     grain: cycle x ngày     -> BR-04 (MJ/m² / 3.6 -> kWh/m²)
  fact_harvest        grain: cycle x ngày thu hoạch -> BR-04, BR-05
  agg_* : bảng tổng hợp sẵn (aggregate) cho dashboard độ trễ thấp.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.common import Lake, project_config, rule_config

MJ_PER_KWH = 3.6


def date_key(col: F.Column) -> F.Column:
    return F.date_format(col, "yyyyMMdd").cast("int")


def time_key(ts: F.Column) -> F.Column:
    return (F.hour(ts) * 60 + F.minute(ts)).cast("int")


class GoldBuilder:
    def __init__(self, spark: SparkSession, lake: Lake, timer):
        self.spark, self.lake, self.timer = spark, lake, timer
        self.cfg = project_config()

    def silver(self, table: str) -> DataFrame | None:
        try:
            return self.spark.read.parquet(self.lake.path("silver", table))
        except Exception:
            return None

    def write(self, name: str, df: DataFrame, parts: int = 2) -> None:
        with self.timer.stage(f"gold.{name}") as rec:
            df.coalesce(parts).write.mode("overwrite").parquet(self.lake.path("gold", name))
            rec["rows"] = self.spark.read.parquet(self.lake.path("gold", name)).count()

    # ------------------------------------------------------------------ dimensions
    def dimensions(self) -> None:
        exp = self.cfg["experiment"]
        start = exp["start_date"]
        bounds = []
        for t, c in (("climate_observation", "ts"), ("resource_day", "date"), ("harvest", "harvest_date"),
                     ("weather_observation", "ts")):
            df = self.silver(t)
            if df is not None:
                bounds.append(df.select(F.min(F.to_date(c)).alias("lo"), F.max(F.to_date(c)).alias("hi")))
        b = bounds[0]
        for x in bounds[1:]:
            b = b.unionByName(x)
        lo, hi = b.agg(F.min("lo"), F.max("hi")).first()
        dim_date = (
            self.spark.sql(f"SELECT explode(sequence(DATE'{lo}', DATE'{hi}', INTERVAL 1 DAY)) AS date")
            .select(
                date_key(F.col("date")).alias("date_key"), "date",
                F.year("date").alias("year"), F.month("date").alias("month"),
                F.date_format("date", "MMM").alias("month_name"), F.dayofmonth("date").alias("day"),
                F.weekofyear("date").alias("iso_week"),
                F.date_trunc("week", "date").cast("date").alias("week_start"),
                F.dayofweek("date").alias("day_of_week"),
                (F.floor(F.datediff("date", F.lit(start)) / 7) + 1).cast("int").alias("experiment_week"),
                F.datediff("date", F.lit(start)).alias("day_of_experiment"),
            )
        )
        self.write("dim_date", dim_date, 1)

        dim_time = (
            self.spark.range(0, 24 * 60, 5).select(F.col("id").cast("int").alias("time_key"))
            .withColumn("hour", (F.col("time_key") / 60).cast("int"))
            .withColumn("minute", (F.col("time_key") % 60).cast("int"))
            .withColumn("hhmm", F.format_string("%02d:%02d", "hour", "minute"))
            .withColumn("day_part", F.when(F.col("hour").between(6, 19), "day").otherwise("night"))
        )
        self.write("dim_time", dim_time, 1)

        comp = self.silver("compartment").join(self.silver("site"), "site_id")
        self.write("dim_compartment", comp.select(
            F.col("compartment_id").alias("compartment_key"), "compartment_code", "floor_area_m2",
            "site_code", "site_name", "country"), 1)

        cyc = (self.silver("growing_cycle").join(self.silver("team"), "team_id")
               .join(self.silver("crop"), "crop_id"))
        self.write("dim_cycle", cyc.select(
            F.col("cycle_id").alias("cycle_key"), F.col("compartment_id").alias("compartment_key"),
            "cycle_code", "team_code", "team_name", "control_type", "cultivar", "start_date", "end_date",
            "is_replica"), 1)

        rules = rule_config()["rule_versions"]
        cols = list(rules[0].keys())
        rows = [tuple(r[c] for c in cols) for r in rules]
        rdf = self.spark.createDataFrame(rows, cols)
        for c in cols:
            if c not in ("rule_version", "is_active", "description"):
                rdf = rdf.withColumn(c, F.col(c).cast("double"))
        self.write("dim_rule_version", rdf, 1)

    # ------------------------------------------------------------------ facts
    def facts(self) -> None:
        clim = self.silver("climate_observation")
        heat_src = (F.when(F.col("t_heat_sp").isNotNull(), "requested")
                    .when(F.col("t_heat_vip").isNotNull(), "realized").otherwise("missing"))
        co2_src = (F.when(F.col("co2_sp").isNotNull(), "requested")
                   .when(F.col("co2_vip").isNotNull(), "realized").otherwise("missing"))
        fact_climate = clim.select(
            F.col("cycle_id").alias("cycle_key"), date_key(F.col("ts")).alias("date_key"),
            time_key(F.col("ts")).alias("time_key"), "ts",
            "tair", "rh", "co2", "hum_def", "par_total", "pipe_low", "pipe_grow", "vent_lee", "vent_wind",
            F.coalesce("t_heat_sp", "t_heat_vip").alias("heat_sp"), heat_src.alias("heat_sp_source"),
            F.coalesce("t_vent_sp", "t_vent_vip").alias("vent_sp"),
            F.coalesce("co2_sp", "co2_vip").alias("co2_sp"), co2_src.alias("co2_sp_source"),
        )
        self.write("fact_climate", fact_climate, 4)

        root = self.silver("rootzone_observation")
        rz = clim.select("cycle_id", "ts", "ec_drain", "ph_drain")
        if root is not None:
            rz = rz.join(root.select(
                "cycle_id", "ts",
                ((F.col("ec_slab1") + F.col("ec_slab2")) / 2).alias("ec_slab"),
                ((F.col("wc_slab1") + F.col("wc_slab2")) / 2).alias("wc_slab"),
                ((F.col("t_slab1") + F.col("t_slab2")) / 2).alias("t_slab")), ["cycle_id", "ts"], "full")
        else:
            rz = rz.select("*", *[F.lit(None).cast("double").alias(c) for c in ("ec_slab", "wc_slab", "t_slab")])
        self.write("fact_rootzone", rz.select(
            F.col("cycle_id").alias("cycle_key"), date_key(F.col("ts")).alias("date_key"),
            time_key(F.col("ts")).alias("time_key"), "ts", "ec_drain", "ph_drain", "ec_slab", "wc_slab", "t_slab"), 4)

        w = self.silver("weather_observation")
        self.write("fact_weather", w.select(
            date_key(F.col("ts")).alias("date_key"), time_key(F.col("ts")).alias("time_key"), "ts",
            "tout", "rhout", "iglob", "par_out", "rain", "wind_speed"), 2)

        res = self.silver("resource_day")
        self.write("fact_irrigation_day", res.select(
            F.col("cycle_id").alias("cycle_key"), date_key(F.col("date")).alias("date_key"),
            "irr_l_m2", "drain_l_m2",
            F.when(F.col("irr_l_m2") > 0, F.col("drain_l_m2") / F.col("irr_l_m2") * 100).alias("drain_pct"),
            (F.col("irr_l_m2") - F.col("drain_l_m2")).alias("uptake_l_m2")), 1)

        elec = F.when(F.col("elec_high_kwh_m2").isNull() & F.col("elec_low_kwh_m2").isNull(), None) \
            .otherwise(F.coalesce("elec_high_kwh_m2", F.lit(0.0)) + F.coalesce("elec_low_kwh_m2", F.lit(0.0)))
        energy = res.select(
            F.col("cycle_id").alias("cycle_key"), date_key(F.col("date")).alias("date_key"),
            "heat_mj_m2", (F.col("heat_mj_m2") / MJ_PER_KWH).alias("heat_kwh_m2"),
            "elec_high_kwh_m2", "elec_low_kwh_m2", elec.alias("elec_kwh_m2"), "co2_kg_m2",
        ).withColumn(
            "total_kwh_m2",
            F.when(F.col("heat_kwh_m2").isNull() & F.col("elec_kwh_m2").isNull(), None)
            .otherwise(F.coalesce("heat_kwh_m2", F.lit(0.0)) + F.coalesce("elec_kwh_m2", F.lit(0.0))),
        ).withColumn(
            "is_complete",
            F.col("heat_mj_m2").isNotNull() & F.col("elec_high_kwh_m2").isNotNull() & F.col("elec_low_kwh_m2").isNotNull(),
        )
        self.write("fact_energy_day", energy, 1)

        hv = self.silver("harvest")
        self.write("fact_harvest", hv.select(
            F.col("cycle_id").alias("cycle_key"), date_key(F.col("harvest_date")).alias("date_key"),
            "prod_a_kg_m2", "prod_b_kg_m2",
            (F.coalesce("prod_a_kg_m2", F.lit(0.0)) + F.coalesce("prod_b_kg_m2", F.lit(0.0))).alias("prod_total_kg_m2"),
            "nr_fruits_class_a", "weight_fruits_class_a_g"), 1)

        # ---------------- aggregate tables (materialized cho dashboard)
        self.write("agg_climate_hourly", fact_climate.groupBy(
            "cycle_key", "date_key", F.hour("ts").alias("hour")).agg(
            F.avg("tair").alias("tair_avg"), F.min("tair").alias("tair_min"), F.max("tair").alias("tair_max"),
            F.avg("rh").alias("rh_avg"), F.avg("co2").alias("co2_avg"), F.avg("heat_sp").alias("heat_sp_avg"),
            F.avg("co2_sp").alias("co2_sp_avg"), F.count("*").alias("n_obs")), 2)

        self.write("agg_weather_day", w.groupBy(date_key(F.col("ts")).alias("date_key")).agg(
            (F.sum(F.when(F.col("rain") > 0, 1).otherwise(0)) * 5 / 60).alias("rain_hours"),
            (F.sum("iglob") * 300 / 1e4).alias("radiation_j_cm2"),
            F.avg("tout").alias("tout_avg"), F.avg("rhout").alias("rhout_avg"),
            F.count("*").alias("n_obs")), 1)


def run(spark: SparkSession, lake: Lake, timer) -> None:
    gb = GoldBuilder(spark, lake, timer)
    gb.dimensions()
    gb.facts()
