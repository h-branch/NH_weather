import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime, timedelta
import re


RAW_DIR = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\온열질환\동네예보_raw")
OUT_DIR = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\온열질환\processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# 격자-시군구 매칭표
# 컬럼 예시: grid_id, 시도, 시군구
GRID_SGG_PATH = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\온열질환\grid_to_sgg.csv")

# 동네예보 격자 크기
NX = 149
NY = 253
N_GRID = NX * NY

# bin 자료 dtype
# 기존 기상청 bin 자료가 float32면 그대로 사용
BIN_DTYPE = np.float32

# 분석 기간
START_DATE = "20210630"
END_DATE   = "20230930"

TARGET_TMFC_HOUR = 8

VARS = ["TMP", "REH", "WSD", "PCP"]


grid_to_sgg = pd.read_csv(GRID_SGG_PATH, encoding="utf-8-sig")

print(grid_to_sgg.head())
print(grid_to_sgg.columns)
print(grid_to_sgg.shape)


def date_range(start_yyyymmdd, end_yyyymmdd):
    start = datetime.strptime(start_yyyymmdd, "%Y%m%d")
    end = datetime.strptime(end_yyyymmdd, "%Y%m%d")

    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def make_tmef_list_d0(tmfc_dt):
    """
    08시 발표 당일 예측용.
    08시 발표 이후 09~23시 예보만 사용.
    """
    tmef_list = []

    for h in range(tmfc_dt.hour + 1, 24):
        tmef_dt = datetime(
            tmfc_dt.year,
            tmfc_dt.month,
            tmfc_dt.day,
            h
        )
        tmef_list.append(tmef_dt)

    return tmef_list


def read_bin_to_df(var, tmfc, tmef):
    """
    쉼표 구분 텍스트형 격자파일 읽기
    예: -99.00, -99.00, ...
    """

    file_path = RAW_DIR / var / f"{var}_tmfc{tmfc}_tmef{tmef}.bin"

    if not file_path.exists():
        print(f"[파일 없음] {file_path}")
        return None

    try:
        text = file_path.read_text(encoding="utf-8", errors="ignore")
        arr = np.fromstring(text.replace("\n", ""), sep=",")
    except Exception as e:
        print(f"[읽기 오류] {file_path.name}: {e}")
        return None

    # 파일 끝 쉼표 등으로 1개가 더 읽힌 경우 보정
    if arr.size == N_GRID + 1:
        arr = arr[:N_GRID]

    if arr.size != N_GRID:
        print(f"[크기 이상] {file_path.name} size={arr.size}, expected={N_GRID}")
        return None

    arr = arr.astype(float)
    arr[arr <= -90] = np.nan

    df = pd.DataFrame({
        "grid_id": np.arange(N_GRID, dtype=int),
        var: arr
    })

    return df


def calc_heat_index_c(tmp, rh):
    """
    기온 TMP, 상대습도 RH 기반 체감온도 계산.
    고온다습 조건이 아니면 기온값을 그대로 사용.
    """

    tmp = np.asarray(tmp, dtype=float)
    rh = np.asarray(rh, dtype=float)

    tmp_f = tmp * 9 / 5 + 32

    hi_f = (
        -42.379
        + 2.04901523 * tmp_f
        + 10.14333127 * rh
        - 0.22475541 * tmp_f * rh
        - 0.00683783 * tmp_f ** 2
        - 0.05481717 * rh ** 2
        + 0.00122874 * tmp_f ** 2 * rh
        + 0.00085282 * tmp_f * rh ** 2
        - 0.00000199 * tmp_f ** 2 * rh ** 2
    )

    hi_c = (hi_f - 32) * 5 / 9

    # 낮은 온도에서는 과도한 체감온도 계산 방지
    heat = np.where((tmp >= 27) & (rh >= 40), hi_c, tmp)

    return heat


def process_one_day(cur_date):
    tmfc_dt = datetime(
        cur_date.year,
        cur_date.month,
        cur_date.day,
        TARGET_TMFC_HOUR
    )

    tmfc = tmfc_dt.strftime("%Y%m%d%H")
    tmef_list = make_tmef_list_d0(tmfc_dt)

    hourly_sgg_list = []

    for tmef_dt in tmef_list:
        tmef = tmef_dt.strftime("%Y%m%d%H")

        var_dfs = []

        for var in VARS:
            temp = read_bin_to_df(var, tmfc, tmef)

            if temp is None:
                break

            var_dfs.append(temp)

        # 네 변수 중 하나라도 없으면 해당 시각 건너뜀
        if len(var_dfs) != len(VARS):
            print(f"[시각 건너뜀] tmfc={tmfc}, tmef={tmef}")
            continue

        # grid_id 기준 병합
        df = var_dfs[0]

        for temp in var_dfs[1:]:
            df = df.merge(temp, on="grid_id", how="inner")

        # 격자-시군구 매칭
        df = df.merge(grid_to_sgg, on="grid_id", how="left")

        # 시군구 매칭 안 된 격자 제거
        df = df[df["시도"].notna() & df["시군구"].notna()].copy()

        # 체감온도 계산
        df["HEAT"] = calc_heat_index_c(df["TMP"], df["REH"])

        df["tmfc"] = pd.to_datetime(tmfc, format="%Y%m%d%H")
        df["datetime"] = pd.to_datetime(tmef, format="%Y%m%d%H")
        df["date"] = df["datetime"].dt.floor("D")

        # 시군구-시간 단위 집계
        # 격자가 여러 개면 평균값 사용
        sgg_hourly = (
            df
            .groupby(["tmfc", "datetime", "date", "시도", "시군구"], as_index=False)
            .agg(
                TMP=("TMP", "mean"),
                REH=("REH", "mean"),
                WSD=("WSD", "mean"),
                PCP=("PCP", "mean"),
                HEAT=("HEAT", "mean")
            )
        )

        hourly_sgg_list.append(sgg_hourly)

    if len(hourly_sgg_list) == 0:
        return None

    sgg_hourly_all = pd.concat(hourly_sgg_list, ignore_index=True)

    # 시군구-일 단위 집계
    daily_sgg = (
        sgg_hourly_all
        .groupby(["tmfc", "date", "시도", "시군구"], as_index=False)
        .agg(
            tmp_max=("TMP", "max"),
            tmp_min=("TMP", "min"),
            tmp_mean=("TMP", "mean"),

            reh_max=("REH", "max"),
            reh_mean=("REH", "mean"),

            wsd_max=("WSD", "max"),
            wsd_mean=("WSD", "mean"),

            rain_sum=("PCP", "sum"),

            heat_max=("HEAT", "max"),
            heat_mean=("HEAT", "mean")
        )
    )

    daily_sgg["rain_yn"] = (daily_sgg["rain_sum"] > 0).astype(int)

    return daily_sgg


all_daily_list = []

for cur_date in date_range(START_DATE, END_DATE):
    print("처리 중:", cur_date.strftime("%Y-%m-%d"))

    daily = process_one_day(cur_date)

    if daily is None:
        print("[자료 없음]", cur_date.strftime("%Y-%m-%d"))
        continue

    all_daily_list.append(daily)

if len(all_daily_list) == 0:
    raise ValueError("처리된 일자료가 없습니다.")

weather_daily_sgg = pd.concat(all_daily_list, ignore_index=True)

print(weather_daily_sgg.shape)
print(weather_daily_sgg.head())


out_path_csv = OUT_DIR / "weather_daily_sgg_2021_2023.csv"

weather_daily_sgg.to_csv(
    out_path_csv,
    index=False,
    encoding="utf-8-sig"
)

print("저장 완료:", out_path_csv)
