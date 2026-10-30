"""Sinh bộ dữ liệu MẪU có cùng cấu trúc với archive Kaggle/4TU
"Autonomous Greenhouse Challenge 2nd edition (2019)".

Chỉ dùng để kiểm thử pipeline khi chưa có dữ liệu thật: số liệu là giả lập
(có chu kỳ ngày/đêm, khác biệt giữa các team) và cố ý cài các lỗi dữ liệu
mà slide nhóm đã nêu để kiểm tra các bước làm sạch:
  * thời gian dạng Excel serial (cột %time / %Time), giá trị "NaN"
  * AICU thiếu requested setpoint: ~70% t_heat_sp, ~86% co2_sp
  * Reference có 1 dòng thu hoạch ngày 14/02/2019 (sai năm)
  * vài dòng trùng lặp, vài giá trị ngoài ngưỡng vật lý

Cách chạy:  python -m tools.make_sample_data --out data/sample_raw
"""
from __future__ import annotations

import argparse
import os

import numpy as np
import pandas as pd

TEAMS = {
    # team: (heat_sp_offset, co2_sp, irr_factor, yield_factor, rh_bias)
    "AICU": (0.4, 800, 1.05, 1.10, -2.0),
    "Automatoes": (0.0, 900, 0.95, 1.20, -4.0),
    "Digilog": (0.8, 700, 1.15, 0.85, 3.0),
    "IUACAAS": (0.2, 750, 1.00, 0.95, 1.0),
    "Reference": (1.0, 800, 1.10, 1.00, 2.0),
    "TheAutomators": (-0.3, 850, 0.90, 1.05, -1.0),
}
START = pd.Timestamp("2019-12-16")
N_5MIN = 47809  # 166 ngày * 288 + 1, giống bộ dữ liệu thật
EXCEL_EPOCH = pd.Timestamp("1899-12-30")


def excel_serial(ts: pd.Series | pd.DatetimeIndex) -> np.ndarray:
    return ((pd.DatetimeIndex(ts) - EXCEL_EPOCH) / pd.Timedelta(days=1)).to_numpy()


def fmt(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, na_rep="NaN", float_format="%.6g")


def make_weather(idx: pd.DatetimeIndex, rng: np.random.Generator) -> pd.DataFrame:
    n = len(idx)
    doy = idx.dayofyear.to_numpy()
    hour = idx.hour.to_numpy() + idx.minute.to_numpy() / 60
    season = np.where(doy > 300, doy - 365, doy)  # -15 .. 150
    sun = np.clip(np.sin((hour - 6) / 12 * np.pi), 0, None)
    day_len_factor = 0.4 + season / 150 * 0.8
    cloud = np.repeat(rng.uniform(0.2, 1.0, n // 288 + 1), 288)[:n]
    iglob = sun * 600 * day_len_factor * cloud + rng.normal(0, 5, n).clip(0)
    tout = 4 + season / 150 * 10 + 4 * np.sin((hour - 9) / 24 * 2 * np.pi) + rng.normal(0, 1.2, n)
    rain_day = np.repeat(rng.random(n // 288 + 1) < 0.35, 288)[:n]
    rain = (rain_day & (rng.random(n) < 0.3)).astype(int)
    rhout = np.clip(85 - 15 * sun + 10 * rain + rng.normal(0, 4, n), 30, 100)
    return pd.DataFrame({
        "%time": excel_serial(idx),
        "AbsHumOut": np.round(6 + rng.normal(0, 0.8, n), 3),
        "Iglob": np.round(iglob, 2),
        "PARout": np.round(iglob * 2.1, 2),
        "Pyrgeo": np.round(-60 + rng.normal(0, 15, n), 2),
        "RadSum": np.round(np.cumsum(iglob * 300 / 1e4) % 2500, 1),
        "Rain": rain,
        "Rhout": np.round(rhout, 2),
        "Tout": np.round(tout, 2),
        "Winddir": np.round(rng.uniform(0, 360, n), 1),
        "Windsp": np.round(np.abs(rng.normal(4, 2, n)), 2),
    })


def make_team(team: str, idx: pd.DatetimeIndex, weather: pd.DataFrame, rng: np.random.Generator, out: str) -> None:
    heat_off, co2_base, irr_f, yield_f, rh_bias = TEAMS[team]
    n = len(idx)
    hour = idx.hour.to_numpy() + idx.minute.to_numpy() / 60
    is_day = (hour >= 6) & (hour < 20)
    iglob = weather["Iglob"].to_numpy()

    t_heat_sp = np.where(is_day, 19.5, 17.0) + heat_off
    t_vent_sp = t_heat_sp + 2.0
    co2_sp = np.where(is_day, co2_base, 450.0)
    solar_gain = iglob / 600 * 6
    tair = t_heat_sp + 0.6 + solar_gain + rng.normal(0, 0.7, n)
    tair = np.minimum(tair, t_vent_sp + 1.5 + rng.normal(0, 0.8, n))
    rh = np.clip(80 + rh_bias - solar_gain * 2 + rng.normal(0, 5, n), 40, 100)
    co2 = co2_sp + rng.normal(0, 70, n) - np.where(is_day, iglob / 600 * 120, 0)
    vent = np.clip((tair - t_vent_sp) * 30, 0, 100)
    ec_drain = np.clip(3.6 + 0.6 * np.sin(np.arange(n) / 2000) + rng.normal(0, 0.45, n), 0.5, 9)
    ph_drain = np.clip(6.0 + 0.25 * np.sin(np.arange(n) / 3100) + rng.normal(0, 0.18, n), 4, 8)

    clim = pd.DataFrame({
        "%time": excel_serial(idx),
        "AssimLight": np.where(is_day & (iglob < 150), 100, 0),
        "BlackScr": np.where(is_day, 0, 95),
        "CO2air": np.round(co2, 1),
        "Cum_irr": np.round((np.arange(n) % 288) / 288 * 3 * irr_f, 3),
        "EC_drain_PC": np.round(ec_drain, 2),
        "EnScr": np.where(is_day, 0, 100),
        "HumDef": np.round(np.clip(8 - rh / 15, 0.5, 9), 2),
        "PipeGrow": np.round(np.where(is_day, 35, 40) + rng.normal(0, 2, n), 1),
        "PipeLow": np.round(np.where(is_day, 45, 55) + rng.normal(0, 3, n), 1),
        "Rhair": np.round(rh, 1),
        "Tair": np.round(tair, 2),
        "Tot_PAR": np.round(iglob * 1.6 + np.where(is_day & (iglob < 150), 180, 0), 1),
        "Tot_PAR_Lamps": np.where(is_day & (iglob < 150), 180, 0),
        "VentLee": np.round(vent, 1),
        "Ventwind": np.round(vent * 0.6, 1),
        "assim_sp": 100.0,
        "assim_vip": np.where(is_day & (iglob < 150), 100, 0),
        "co2_dos": np.round(np.where(is_day, 0.002, 0), 4),
        "co2_sp": co2_sp,
        "co2_vip": co2_sp,
        "pH_drain_PC": np.round(ph_drain, 2),
        "t_heat_sp": t_heat_sp,
        "t_heat_vip": t_heat_sp,
        "t_vent_sp": t_vent_sp,
        "t_ventlee_vip": t_vent_sp,
        "t_ventwind_vip": t_vent_sp + 0.5,
        "water_sup": np.round((np.arange(n) % 288) * irr_f * 10, 0),
    })
    if team == "AICU":  # thiếu requested setpoint giống dữ liệu thật
        clim.loc[rng.random(n) < 0.70, "t_heat_sp"] = np.nan
        clim.loc[rng.random(n) < 0.86, "co2_sp"] = np.nan
    gaps = rng.random(n) < 0.004
    clim.loc[gaps, ["Tair", "Rhair", "CO2air"]] = np.nan
    bad = rng.choice(n, 3, replace=False)
    clim.loc[bad, "Tair"] = 999.0                     # giá trị ngoài ngưỡng
    clim = pd.concat([clim, clim.iloc[rng.choice(n, 5, replace=False)]])  # bản ghi trùng
    fmt(clim, f"{out}/{team}/GreenhouseClimate.csv")

    # Root zone (Grodan slab sensors), 5 phút
    fmt(pd.DataFrame({
        "%time": excel_serial(idx),
        "EC_slab1": np.round(ec_drain - 0.3 + rng.normal(0, 0.2, n), 2),
        "EC_slab2": np.round(ec_drain - 0.2 + rng.normal(0, 0.2, n), 2),
        "WC_slab1": np.round(65 + rng.normal(0, 4, n), 1),
        "WC_slab2": np.round(63 + rng.normal(0, 4, n), 1),
        "t_slab1": np.round(tair - 1 + rng.normal(0, 0.5, n), 2),
        "t_slab2": np.round(tair - 1.2 + rng.normal(0, 0.5, n), 2),
    }), f"{out}/{team}/GrodanSens.csv")

    days = pd.date_range(START, periods=166, freq="D")
    dn = len(days)
    daily_rad = weather.groupby(idx.normalize())["Iglob"].sum().reindex(days).to_numpy() * 300 / 1e4
    season = np.arange(dn) / dn
    irr = np.clip((0.4 + daily_rad / 900 * 3.5) * irr_f + rng.normal(0, 0.2, dn), 0.1, None)
    res = pd.DataFrame({
        "%Time": excel_serial(days),
        "Heat_cons": np.round(np.clip(3.2 - 2.6 * season + rng.normal(0, 0.25, dn), 0.1, None) * (1 + heat_off / 10), 3),
        "ElecHigh": np.round(np.clip(0.55 - 0.45 * season + rng.normal(0, 0.05, dn), 0, None), 3),
        "ElecLow": np.round(np.clip(0.30 - 0.25 * season + rng.normal(0, 0.04, dn), 0, None), 3),
        "CO2_cons": np.round(0.008 + rng.normal(0, 0.001, dn), 4),
        "Irr": np.round(irr, 3),
        "Drain": np.round(irr * np.clip(rng.normal(0.30, 0.06, dn), 0.05, 0.6), 3),
    })
    res.loc[rng.choice(dn, 3, replace=False), "ElecHigh"] = np.nan
    fmt(res, f"{out}/{team}/Resources.csv")

    # Thu hoạch: từ đầu tháng 2, 2 lần/tuần
    hdays = pd.date_range("2020-02-03", "2020-05-29", freq="3D")
    hn = len(hdays)
    ramp = np.linspace(0.08, 0.40, hn)
    prod_a = np.round(ramp * yield_f + rng.normal(0, 0.02, hn), 3).clip(0)
    prod = pd.DataFrame({
        "%time": excel_serial(hdays),
        "ProdA": prod_a,
        "ProdB": np.round(prod_a * rng.uniform(0.03, 0.08, hn), 3),
        "avg_nr_harvested_trays": np.round(prod_a * 40, 1),
        "Truss development time": np.round(rng.normal(55, 3, hn), 1),
        "Nr_fruits_ClassA": np.round(prod_a * 80, 1),
        "Weight_fruits_ClassA": np.round(rng.normal(12.5, 0.8, hn), 2),
        "Nr_fruits_ClassB": np.round(prod_a * 4, 1),
        "Weight_fruits_ClassB": np.round(rng.normal(10, 1, hn), 2),
    })
    if team == "Reference":  # ngày thu hoạch "năm ngoái" 14/02/2019 -> phải bị cách ly
        wrong = prod.iloc[[2]].copy()
        wrong["%time"] = excel_serial(pd.DatetimeIndex([pd.Timestamp("2019-02-14")]))
        prod = pd.concat([prod, wrong])
    fmt(prod, f"{out}/{team}/Production.csv")

    weeks = pd.date_range(START, periods=24, freq="7D")
    wn = len(weeks)
    fmt(pd.DataFrame({
        "%Time": excel_serial(weeks),
        "Stem_elong": np.round(rng.normal(18, 2, wn), 1),
        "Stem_thick": np.round(rng.normal(10, 0.8, wn), 1),
        "Cum_trusses": np.round(np.linspace(2, 30, wn) + rng.normal(0, 0.5, wn), 1),
        "stem_dens": np.where(np.arange(wn) < 6, 3.2, 3.9),
        "plant_dens": 3.2,
    }), f"{out}/{team}/CropParameters.csv")

    bi = pd.date_range(START, periods=12, freq="14D")
    bn = len(bi)
    lab = {"%Time": excel_serial(bi)}
    for ion, base in [("NH4", 0.6), ("K", 8.0), ("Ca", 5.0), ("Mg", 2.5), ("NO3", 20.0), ("SO4", 3.0), ("PO4", 1.3)]:
        lab[f"irr_{ion}"] = np.round(base + rng.normal(0, base * 0.08, bn), 2)
        lab[f"drain_{ion}"] = np.round(base * 1.2 + rng.normal(0, base * 0.1, bn), 2)
    lab["irr_EC"] = np.round(rng.normal(2.8, 0.15, bn), 2)
    lab["drain_EC"] = np.round(rng.normal(3.7, 0.3, bn), 2)
    lab["irr_pH"] = np.round(rng.normal(5.6, 0.1, bn), 2)
    lab["drain_pH"] = np.round(rng.normal(6.0, 0.2, bn), 2)
    fmt(pd.DataFrame(lab), f"{out}/{team}/LabAnalysis.csv")

    qd = pd.date_range("2020-02-10", periods=8, freq="14D")
    qn = len(qd)
    fmt(pd.DataFrame({
        "%Time": excel_serial(qd),
        "Flavour": np.round(rng.normal(75, 4, qn), 1),
        "TSS": np.round(rng.normal(8.5, 0.5, qn), 2),
        "Acid": np.round(rng.normal(0.75, 0.05, qn), 3),
        "%Juice": np.round(rng.normal(70, 3, qn), 1),
        "Bite": np.round(rng.normal(210, 15, qn), 1),
        "Weight": np.round(rng.normal(12, 1, qn), 2),
        "DMC_fruit": np.round(rng.normal(9, 0.5, qn), 2),
    }), f"{out}/{team}/TomQuality.csv")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/sample_raw")
    ap.add_argument("--seed", type=int, default=29)
    args = ap.parse_args()
    rng = np.random.default_rng(args.seed)
    idx = pd.date_range(START, periods=N_5MIN, freq="5min")
    weather = make_weather(idx, rng)
    fmt(weather, f"{args.out}/Weather/Weather.csv")
    for team in TEAMS:
        make_team(team, idx, weather, rng, args.out)
    print(f"Sample dataset written to {args.out} (43 CSV)")


if __name__ == "__main__":
    main()
