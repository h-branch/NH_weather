import requests
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from io import StringIO
from pathlib import Path
from datetime import datetime
import time
plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False


# =====================================================
# 1. 기본 설정
# =====================================================
AUTH_KEY = "R3WTydcASuG1k8nXABrhgA"   # 여기에 새 인증키 입력 권장
BASE_URL = "https://apihub.kma.go.kr/api/typ01/url/wrn_met_data.php"
# 저장 경로
BASE_DIR = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\이상기후대응팀\#기상특보일수")
RAW_DIR = BASE_DIR / "raw_yearly"
RESULT_DIR = BASE_DIR / "result"
FIG_DIR = BASE_DIR / "figure"
RAW_DIR.mkdir(parents=True, exist_ok=True)
RESULT_DIR.mkdir(parents=True, exist_ok=True)
FIG_DIR.mkdir(parents=True, exist_ok=True)
# 분석 기간
START_DT = datetime(2005, 7, 1, 0, 0)
END_DT = datetime.now()
print("분석 시작:", START_DT)
print("분석 종료:", END_DT)


def download_wrn_met_data(tmfc1, tmfc2, wrn="A", reg="0", disp="0", help_="1", max_retry=5):
    """
    기상청 APIHub 기상특보자료 다운로드 함수
    Parameters
    ----------
    tmfc1 : str
        시작 발표시각, 예: '200507010000'
    tmfc2 : str
        종료 발표시각, 예: '200512312359'
    wrn : str
        특보종류. A 또는 공백이면 전체로 사용 가능.
        R: 호우, T: 태풍, H: 폭염 등
    reg : str
        특보구역. 0은 전체 또는 전국 기준으로 사용
    disp : str
        표출단계. 0: 기본
    help_ : str
        1이면 도움말 포함
    """
    params = {
        "reg": reg,
        "wrn": wrn,
        "tmfc1": tmfc1,
        "tmfc2": tmfc2,
        "disp": disp,
        "help": help_,
        "authKey": AUTH_KEY
    }
    for attempt in range(1, max_retry + 1):
        try:
            response = requests.get(
                BASE_URL,
                params=params,
                timeout=120
            )
            response.encoding = "utf-8"
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"[재시도 {attempt}/{max_retry}] {tmfc1} ~ {tmfc2}")
            print(e)
            time.sleep(3 * attempt)
    print(f"[실패] {tmfc1} ~ {tmfc2}")
    return None


def parse_wrn_met_text(text):
    """
    wrn_met_data.php 응답 텍스트를 DataFrame으로 변환
    """
    if text is None:
        return pd.DataFrame()
    lines = text.splitlines()
    data_lines = []
    for line in lines:
        line = line.strip()
        if line == "":
            continue
        # 도움말, 주석 제거
        if line.startswith("#"):
            continue
        # 제어문 제거
        if line.upper().startswith("START"):
            continue
        if line.upper().startswith("END"):
            continue
        # 에러 메시지성 행 방지
        if "ERROR" in line.upper():
            print("API 오류 메시지:", line)
            continue
        data_lines.append(line)
    if len(data_lines) == 0:
        return pd.DataFrame()
    raw_text = "\n".join(data_lines)
    df = pd.read_csv(
        StringIO(raw_text),
        sep=r"\s+",
        header=None,
        engine="python"
    )
    return df


def add_wrn_columns(df):
    """
    기상특보자료 컬럼명 부여
    """
    cols = [
        "REG_ID",     # 특보구역코드
        "TM_ST",      # 시작시각
        "TM_ED",      # 종료시각
        "REG_SP",     # 특성
        "REG_UP",     # 상위 특보구역코드
        "REG_KO",     # 특보구역명 약어
        "REG_NAME",   # 특보구역명
        "TM_FC",      # 발표시각
        "TM_EF",      # 발효시각
        "TM_IN",      # 입력시각
        "STN",        # 발표관서
        "WRN",        # 특보종류
        "LVL",        # 특보수준
        "CMD",        # 특보명령
        "GRD",        # 태풍경보 등급
        "CNT",        # 작업상태
        "RPT",        # 통보문 발송구분
        "STN_ID",     # 발표관서 ID
        "TM_SEQ",     # 발표번호
        "MAN_FC",     # 예보관명
        "MAN_IN"      # 입력자명
    ]
    df = df.copy()
    if df.empty:
        return df
    n_cols = df.shape[1]
    if n_cols <= len(cols):
        df.columns = cols[:n_cols]
    else:
        extra_cols = [f"EXTRA_{i}" for i in range(n_cols - len(cols))]
        df.columns = cols + extra_cols
    return df


def preprocess_wrn_df(df):
    """
    기상특보자료 날짜, 특보명 전처리
    """
    df = df.copy()
    if df.empty:
        return df
    # 시간 컬럼 변환
    time_cols = ["TM_ST", "TM_ED", "TM_FC", "TM_EF", "TM_IN"]
    for col in time_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)
            df[col] = pd.to_datetime(df[col], format="%Y%m%d%H%M", errors="coerce")
    # 발표일 기준
    if "TM_FC" in df.columns:
        df["fc_date"] = df["TM_FC"].dt.date
        df["fc_year"] = df["TM_FC"].dt.year
        df["fc_month"] = df["TM_FC"].dt.month
    # 발효일 기준
    if "TM_EF" in df.columns:
        df["ef_date"] = df["TM_EF"].dt.date
        df["ef_year"] = df["TM_EF"].dt.year
        df["ef_month"] = df["TM_EF"].dt.month
    # 특보종류 코드 매핑
    wrn_map = {
        "W": "강풍",
        "R": "호우",
        "C": "한파",
        "D": "건조",
        "O": "폭풍해일",
        "N": "지진해일",
        "V": "풍랑",
        "T": "태풍",
        "S": "대설",
        "Y": "황사",
        "H": "폭염",
        "F": "안개",
        "A": "전체"
    }
    if "WRN" in df.columns:
        df["WRN"] = df["WRN"].astype(str)
        df["wrn_name"] = df["WRN"].map(wrn_map).fillna(df["WRN"])
    # 특보수준 코드 매핑
    lvl_map = {
        "1": "예비",
        "2": "주의보",
        "3": "경보"
    }
    if "LVL" in df.columns:
        df["LVL"] = df["LVL"].astype(str)
        df["lvl_name"] = df["LVL"].map(lvl_map).fillna(df["LVL"])
    return df


# =====================================================
# 5. 연도별 기상특보자료 다운로드 및 저장
# =====================================================
start_year = START_DT.year
end_year = END_DT.year
for year in range(start_year, end_year + 1):
    # 첫 해는 7월 1일부터
    if year == START_DT.year:
        sdt = START_DT
    else:
        sdt = datetime(year, 1, 1, 0, 0)
    # 마지막 해는 현재까지
    if year == END_DT.year:
        edt = END_DT
    else:
        edt = datetime(year, 12, 31, 23, 59)
    tmfc1 = sdt.strftime("%Y%m%d%H%M")
    tmfc2 = edt.strftime("%Y%m%d%H%M")
    out_csv = RAW_DIR / f"wrn_met_all_{year}.csv"
    if out_csv.exists():
        print(f"[건너뜀] 이미 저장됨: {out_csv.name}")
        continue
    print("=" * 60)
    print(f"[다운로드] {year}: {tmfc1} ~ {tmfc2}")
    text = download_wrn_met_data(
        tmfc1=tmfc1,
        tmfc2=tmfc2,
        wrn="",     # 전체 특보 저장
        reg="0",
        disp="0",
        help_="0"
    )
    df_raw = parse_wrn_met_text(text)
    if df_raw.empty:
        print(f"[자료 없음] {year}")
        continue
    df_year = add_wrn_columns(df_raw)
    df_year = preprocess_wrn_df(df_year)
    df_year.to_csv(
        out_csv,
        index=False,
        encoding="utf-8-sig"
    )
    print(f"[저장 완료] {out_csv.name}, 행 수: {len(df_year):,}")
    time.sleep(1)


# =====================================================
# 6. 연도별 저장 파일 병합
# =====================================================
csv_files = sorted(RAW_DIR.glob("wrn_met_all_*.csv"))
print("병합 대상 파일 수:", len(csv_files))
df_list = []
for file in csv_files:
    print("읽는 중:", file.name)
    df_temp = pd.read_csv(file, encoding="utf-8-sig")
    # 날짜형 재변환
    for col in ["TM_ST", "TM_ED", "TM_FC", "TM_EF", "TM_IN"]:
        if col in df_temp.columns:
            df_temp[col] = pd.to_datetime(df_temp[col], errors="coerce")
    if "fc_date" in df_temp.columns:
        df_temp["fc_date"] = pd.to_datetime(df_temp["fc_date"], errors="coerce")
    if "ef_date" in df_temp.columns:
        df_temp["ef_date"] = pd.to_datetime(df_temp["ef_date"], errors="coerce")
    df_list.append(df_temp)
df_all = pd.concat(df_list, ignore_index=True)
#print("전체 행 수:", len(df_all):,)
#print(df_all.head())
#print(df_all.columns)
df_all


# =====================================================
# 기존 잘못 붙은 df_all 임시 복구
# =====================================================
df_fix = df_all.copy()
df_fix["TM_FC_fix"] = (
    df_fix["REG_ID"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .str.strip()
)
df_fix["REG_ID_fix"] = (
    df_fix["REG_UP"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .str.strip()
)
df_fix["WRN_fix"] = (
    df_fix["REG_KO"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .str.strip()
)
df_fix["LVL_fix"] = (
    df_fix["REG_NAME"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .str.strip()
)
df_fix["CMD_fix"] = (
    df_fix["WRN"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .str.strip()
)
df_fix["TM_FC_fix"] = pd.to_datetime(
    df_fix["TM_FC_fix"],
    format="%Y%m%d%H%M",
    errors="coerce"
)
df_fix["date"] = df_fix["TM_FC_fix"].dt.floor("D")
df_fix["year"] = df_fix["date"].dt.year
target_wrn = {
    "R": "호우",
    "T": "태풍",
    "H": "폭염"
}
df_fix["wrn_name"] = df_fix["WRN_fix"].map(target_wrn)
df_target = df_fix[df_fix["WRN_fix"].isin(target_wrn.keys())].copy()
df_target = df_target.dropna(subset=["date", "year", "wrn_name"]).copy()
df_target = df_target[df_target["date"] >= pd.to_datetime("2005-07-01")].copy()
df_national_days = (
    df_target
    .drop_duplicates(subset=["year", "date", "WRN_fix"])
    .groupby(["year", "wrn_name"], as_index=False)
    .agg(
        warning_days=("date", "count")
    )
)
print(df_national_days.head(30))


df_pivot = (
    df_national_days
    .pivot(index="year", columns="wrn_name", values="warning_days")
    .fillna(0)
    .reset_index()
)
df_pivot["year"] = df_pivot["year"].astype(int)
for col in ["호우", "태풍", "폭염"]:
    if col not in df_pivot.columns:
        df_pivot[col] = 0
df_pivot = df_pivot[["year", "호우", "태풍", "폭염"]]
print(df_pivot)


df_pivot.to_csv(
    r"C:\Users\lhj15\OneDrive\문서\[NH]\이상기후대응팀\#기상특보일수\result\연도별_전국_호우_태풍_폭염_특보일수.csv",
    index=False,
    encoding="utf-8-sig"
)


# =====================================================
# 10. 전국 특보일수 변화 그래프
# =====================================================
fig, ax = plt.subplots(figsize=(15, 7))
ax.plot(
    df_pivot["year"],
    df_pivot["호우"],
    marker="o",
    linewidth=2,
    label="호우특보"
)
ax.plot(
    df_pivot["year"],
    df_pivot["태풍"],
    marker="o",
    linewidth=2,
    label="태풍특보"
)
ax.plot(
    df_pivot["year"],
    df_pivot["폭염"],
    marker="o",
    linewidth=2,
    label="폭염특보"
)
ax.set_title(
    "연도별 전국 호우·태풍·폭염 특보일수 변화",
    fontsize=17,
    fontweight="bold"
)
ax.set_xlabel("연도", fontsize=12)
ax.set_ylabel("전국 특보일수", fontsize=12)
ax.set_xticks(df_pivot["year"])
ax.set_xticklabels(df_pivot["year"], rotation=45)
ax.grid(axis="y", alpha=0.3)
ax.legend(fontsize=11)
plt.tight_layout()
fig_path = FIG_DIR / "연도별_전국_호우_태풍_폭염_특보일수_변화.png"
plt.savefig(fig_path, dpi=300, bbox_inches="tight")
plt.show()
print("그림 저장:", fig_path)


# =====================================================
# 11. 특보별 개별 그래프 저장
# =====================================================
for col in ["호우", "태풍", "폭염"]:
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar(
        df_pivot["year"],
        df_pivot[col],
        label=f"{col}특보일수"
    )
    mean_10yr = df_pivot.tail(10)[col].mean()
    ax.axhline(
        mean_10yr,
        linestyle="--",a
        linewidth=2,
        label=f"최근 10년 평균: {mean_10yr:.1f}일"
    )
    ax.set_title(
        f"연도별 전국 {col}특보일수 변화",
        fontsize=16,
        fontweight="bold"
    )
    ax.set_xlabel("연도")
    ax.set_ylabel("전국 특보일수")
    ax.set_xticks(df_pivot["year"])
    ax.set_xticklabels(df_pivot["year"], rotation=45)
    ax.grid(axis="y", alpha=0.3)
    ax.legend()
    plt.tight_layout()
    fig_path = FIG_DIR / f"연도별_전국_{col}특보일수_변화.png"
    plt.savefig(fig_path, dpi=300, bbox_inches="tight")
    plt.show()
    print("저장:", fig_path)


# =====================================================
# 1. 2026년 제외
# =====================================================
df_plot = df_pivot.copy()
df_plot["year"] = df_plot["year"].astype(int)
df_plot = df_plot[df_plot["year"] != 2026].copy()
df_plot = df_plot[df_plot["year"] != 2005].copy()
print(df_plot.tail())
print(df_plot.head())


asos_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\이상기후대응팀\#기상특보일수\asos_daily")
csv_files = sorted(asos_dir.glob("*.csv"))
print("ASOS CSV 파일 수:", len(csv_files))
if len(csv_files) == 0:
    raise FileNotFoundError(f"CSV 파일이 없습니다: {asos_dir}")
df_list = []
for file in csv_files:
    print("읽는 중:", file.name)
    # 기상청 CSV는 보통 utf-8-sig 또는 cp949일 수 있음
    try:
        df_temp = pd.read_csv(file, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df_temp = pd.read_csv(file, encoding="cp949")
    df_temp["source_file"] = file.name
    df_list.append(df_temp)
df_asos = pd.concat(df_list, ignore_index=True)
print("전체 행 수:", len(df_asos))
print(df_asos.head())
print(df_asos.columns)


# =====================================================
# 2. 컬럼명 정리
# =====================================================
df_asos.columns = (
    df_asos.columns
    .astype(str)
    .str.strip()
    .str.replace("\ufeff", "", regex=False)
)
print("정리 후 컬럼명:")
print(df_asos.columns.tolist())


# =====================================================
# 3. 필요한 컬럼 자동 탐색
# =====================================================
def find_col(df, keywords, required=True):
    """
    keywords에 포함된 단어들이 모두 들어간 컬럼을 찾는 함수
    """
    for col in df.columns:
        col_str = str(col)
        if all(k in col_str for k in keywords):
            return col
    if required:
        raise KeyError(f"해당 키워드를 포함하는 컬럼을 찾지 못했습니다: {keywords}")
    return None
date_col = find_col(df_asos, ["일시"])
stn_col = find_col(df_asos, ["지점"], required=False)
stn_name_col = find_col(df_asos, ["지점명"], required=False)
max_temp_col = find_col(df_asos, ["최고기온"])
rain_col = find_col(df_asos, ["강수량"])
max_wind_col = find_col(df_asos, ["최대", "풍속"])
print("일시 컬럼:", date_col)
print("지점 컬럼:", stn_col)
print("지점명 컬럼:", stn_name_col)
print("최고기온 컬럼:", max_temp_col)
print("강수량 컬럼:", rain_col)
print("최대풍속 컬럼:", max_wind_col)


# =====================================================
# 4. 날짜 및 숫자형 변환
# =====================================================
df_asos_use = df_asos.copy()
df_asos_use["date"] = pd.to_datetime(df_asos_use[date_col], errors="coerce")
df_asos_use["year"] = df_asos_use["date"].dt.year
df_asos_use["month"] = df_asos_use["date"].dt.month
df_asos_use["day"] = df_asos_use["date"].dt.day
# 숫자형 변환
for col in [max_temp_col, rain_col, max_wind_col]:
    df_asos_use[col] = pd.to_numeric(df_asos_use[col], errors="coerce")
# 분석기간: 2006~2025년
df_asos_use = df_asos_use[
    (df_asos_use["year"] >= 2006) &
    (df_asos_use["year"] <= 2025)
].copy()
print("분석 대상 행 수:", len(df_asos_use))
print(df_asos_use[["date", "year", max_temp_col, rain_col, max_wind_col]].head())


# =====================================================
# 5. 일별 전국 평균값 산정
# =====================================================
df_daily_national = (
    df_asos_use
    .groupby(["year", "date"], as_index=False)
    .agg(
        daily_max_temp_mean=(max_temp_col, "mean"),
        daily_rain_mean=(rain_col, "mean"),
        daily_max_wind_mean=(max_wind_col, "mean"),
        station_count=(date_col, "count")
    )
)
print(df_daily_national.head())
print(df_daily_national.tail())


# =====================================================
# 6. 연도별 기상요소 산정
# =====================================================
df_weather_year = (
    df_daily_national
    .groupby("year", as_index=False)
    .agg(
        max_temp_mean=("daily_max_temp_mean", "mean"),      # 연평균 일최고기온
        rain_mean=("daily_rain_mean", "mean"),              # 연평균 일강수량
        rain_total=("daily_rain_mean", "sum"),              # 전국 평균 기준 연누적강수량
        max_wind_mean=("daily_max_wind_mean", "mean"),      # 연평균 일최대풍속
        max_wind_max=("daily_max_wind_mean", "max"),        # 연최대 일최대풍속
        valid_days=("date", "count")
    )
)
# 2026년 제외는 이미 2025년까지만 했지만, 안전하게 한 번 더 제외
df_weather_year = df_weather_year[df_weather_year["year"] != 2026].copy()
print(df_weather_year)


# =====================================================
# 7. ASOS 연도별 기상요소 결과 저장
# =====================================================
result_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\이상기후대응팀\#기상특보일수\result")
result_dir.mkdir(parents=True, exist_ok=True)
out_csv = result_dir / "연도별_ASOS_전국_기상요소_2006_2025.csv"
df_weather_year.to_csv(
    out_csv,
    index=False,
    encoding="utf-8-sig"
)
print("저장 완료:", out_csv)


# =====================================================
# 8. 특보일수 df_pivot과 ASOS 기상요소 병합
# =====================================================
df_plot = df_pivot.copy()
df_plot["year"] = df_plot["year"].astype(int)
# 2026년 제외
df_plot = df_plot[df_plot["year"] != 2026].copy()
# 2006~2025년만 사용
df_plot = df_plot[
    (df_plot["year"] >= 2006) &
    (df_plot["year"] <= 2025)
].copy()
df_plot = df_plot.merge(
    df_weather_year,
    on="year",
    how="left"
)
print(df_plot.head())
print(df_plot.tail())
print(df_plot.columns)


# =====================================================
# 9. 병합 결과 저장
# =====================================================
out_csv = result_dir / "연도별_특보일수_ASOS기상요소_병합_2006_2025.csv"
df_plot.to_csv(
    out_csv,
    index=False,
    encoding="utf-8-sig"
)
print("저장 완료:", out_csv)


# =====================================================
# 10. 막대 + 꺾은선 그래프
# =====================================================
fig_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\이상기후대응팀\#기상특보일수\figure")
fig_dir.mkdir(parents=True, exist_ok=True)
plot_info = {
    "폭염": {
        "bar_col": "폭염",
        "line_col": "max_temp_mean",
        "bar_label": "폭염특보일수",
        "line_label": "연평균 일최고기온",
        "line_ylabel": "연평균 일최고기온(℃)",
        "title": "연도별 전국 폭염특보일수와 연평균 일최고기온 변화",
        "filename": "연도별_폭염특보일수_ASOS_연평균일최고기온.png"
    },
    "호우": {
        "bar_col": "호우",
        "line_col": "rain_mean",
        "bar_label": "호우특보일수",
        "line_label": "연평균강수량",
        "line_ylabel": "연평균 일강수량(mm)",
        "title": "연도별 전국 호우특보일수와 연평균 일강수량 변화",
        "filename": "연도별_호우특보일수_ASOS_연평균강수량.png"
    },
    "태풍": {
        "bar_col": "태풍",
        "line_col": "max_wind_mean",
        "bar_label": "태풍특보일수",
        "line_label": "연평균 일최대풍속",
        "line_ylabel": "연평균 일최대풍속(m/s)",
        "title": "연도별 전국 태풍특보일수와 연평균 일최대풍속 변화",
        "filename": "연도별_태풍특보일수_ASOS_연평균일최대풍속.png"
    }
}
for key, info in plot_info.items():
    fig, ax1 = plt.subplots(figsize=(14, 6))
    # 막대: 특보일수
    ax1.bar(
        df_plot["year"],
        df_plot[info["bar_col"]],
        label=info["bar_label"],
        alpha=0.7
    )
    ax1.set_xlabel("연도")
    ax1.set_ylabel(info["bar_label"])
    ax1.set_xticks(df_plot["year"])
    ax1.set_xticklabels(df_plot["year"], rotation=45)
    ax1.grid(axis="y", alpha=0.3)
    # 꺾은선: 기상요소
    ax2 = ax1.twinx()
    ax2.plot(
        df_plot["year"],
        df_plot[info["line_col"]],
        marker="o",
        linewidth=2.5,
        label=info["line_label"]
    )
    ax2.set_ylabel(info["line_ylabel"])
    # 범례 합치기
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(
        lines1 + lines2,
        labels1 + labels2,
        loc="upper left"
    )
    ax1.set_title(
        info["title"],
        fontsize=16,
        fontweight="bold"
    )
    plt.tight_layout()
    fig_path = fig_dir / info["filename"]
    plt.savefig(
        fig_path,
        dpi=300,
        bbox_inches="tight"
    )
    plt.show()
    print("그림 저장 완료:", fig_path)


# =====================================================
# 10. 막대 + 꺾은선 그래프
#     - 막대 수치: 바닥에 통일
#     - 꺾은선 수치: 점 위에 표시
#     - 특보별 색상 지정
# =====================================================
# -----------------------------------------------------
# 1. 저장 폴더 설정
# -----------------------------------------------------
fig_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\이상기후대응팀\#기상특보일수\figure")
fig_dir.mkdir(parents=True, exist_ok=True)
# -----------------------------------------------------
# 2. 한글 폰트 설정
# -----------------------------------------------------
plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False
# -----------------------------------------------------
# 3. 그래프 설정
# -----------------------------------------------------
plot_info = {
    "폭염": {
        "bar_col": "폭염",
        "line_col": "max_temp_mean",
        "bar_label": "폭염특보일수",
        "line_label": "연평균 일최고기온",
        "line_ylabel": "연평균 일최고기온(℃)",
        "title": "연도별 전국 폭염특보일수와 연평균 일최고기온 변화",
        "filename": "연도별_폭염특보일수_ASOS_연평균일최고기온.png",
        # 색상
        "bar_color": "#FFD8A8",     # 연한 주황
        "line_color": "#D62728",    # 빨강
        # 수치 표시 형식
        "bar_fmt": "{:.0f}",
        "line_fmt": "{:.1f}"
    },
    "호우": {
        "bar_col": "호우",
        "line_col": "rain_mean",
        "bar_label": "호우특보일수",
        "line_label": "연평균강수량",
        "line_ylabel": "연평균 일강수량(mm)",
        "title": "연도별 전국 호우특보일수와 연평균 일강수량 변화",
        "filename": "연도별_호우특보일수_ASOS_연평균강수량.png",
        # 색상
        "bar_color": "#BFE3FF",     # 연한 파랑
        "line_color": "#1F77B4",    # 파랑
        # 수치 표시 형식
        "bar_fmt": "{:.0f}",
        "line_fmt": "{:.1f}"
    },
    "태풍": {
        "bar_col": "태풍",
        "line_col": "max_wind_mean",
        "bar_label": "태풍특보일수",
        "line_label": "연평균 일최대풍속",
        "line_ylabel": "연평균 일최대풍속(m/s)",
        "title": "연도별 전국 태풍특보일수와 연평균 일최대풍속 변화",
        "filename": "연도별_태풍특보일수_ASOS_연평균일최대풍속.png",
        # 색상
        "bar_color": "#C8E6C9",     # 연한 초록
        "line_color": "#2E7D32",    # 초록
        # 수치 표시 형식
        "bar_fmt": "{:.0f}",
        "line_fmt": "{:.1f}"
    }
}
# -----------------------------------------------------
# 4. 그래프 반복 생성
# -----------------------------------------------------
for key, info in plot_info.items():
    # -------------------------------------------------
    # 4-1. 그래프용 데이터 정리
    # -------------------------------------------------
    df_g = df_plot.copy()
    # 연도 정수형 변환
    df_g["year"] = df_g["year"].astype(int)
    # 2026년 제외
    df_g = df_g[df_g["year"] != 2026].copy()
    # 필요한 컬럼 숫자형 변환
    df_g[info["bar_col"]] = pd.to_numeric(
        df_g[info["bar_col"]],
        errors="coerce"
    )
    df_g[info["line_col"]] = pd.to_numeric(
        df_g[info["line_col"]],
        errors="coerce"
    )
    # 결측 제거
    df_g = df_g.dropna(
        subset=[info["bar_col"], info["line_col"]]
    ).copy()
    # 혹시 연도순 정렬
    df_g = df_g.sort_values("year").copy()
    # 데이터가 없으면 건너뜀
    if len(df_g) == 0:
        print(f"[건너뜀] {key}: 그래프를 그릴 데이터가 없습니다.")
        continue
    # -------------------------------------------------
    # 4-2. 그래프 생성
    # -------------------------------------------------
    fig, ax1 = plt.subplots(figsize=(15, 7))
    # -------------------------------------------------
    # 막대그래프: 특보일수
    # -------------------------------------------------
    bars = ax1.bar(
        df_g["year"],
        df_g[info["bar_col"]],
        label=info["bar_label"],
        color=info["bar_color"],
        edgecolor="gray",
        linewidth=0.6,
        alpha=0.95
    )
    ax1.set_xlabel("연도")
    ax1.set_ylabel(info["bar_label"])
    ax1.set_xticks(df_g["year"])
    ax1.set_xticklabels(df_g["year"], rotation=45)
    ax1.grid(
        axis="y",
        alpha=0.3
    )
    # 막대 y축 범위 설정
    bar_max = df_g[info["bar_col"]].max()
    if pd.isna(bar_max) or bar_max <= 0:
        bar_max = 1
    ax1.set_ylim(
        0,
        bar_max * 1.25
    )
    # -------------------------------------------------
    # 막대 수치 표시: 바닥에 통일
    # -------------------------------------------------
    # y축 아래쪽 3% 지점에 고정
    bar_y_pos = bar_max * 0.035
    for x, v in zip(df_g["year"], df_g[info["bar_col"]]):
        if pd.isna(v):
            continue
        ax1.text(
            x,
            bar_y_pos,
            info["bar_fmt"].format(v),
            ha="center",
            va="bottom",
            fontsize=9,
            color="black",
            fontweight="bold",
            bbox=dict(
                facecolor="white",
                edgecolor="none",
                alpha=0.75,
                boxstyle="round,pad=0.15"
            )
        )
    # -------------------------------------------------
    # 꺾은선그래프: 기상요소
    # -------------------------------------------------
    ax2 = ax1.twinx()
    ax2.plot(
        df_g["year"],
        df_g[info["line_col"]],
        marker="o",
        markersize=6,
        linewidth=2.5,
        color=info["line_color"],
        label=info["line_label"]
    )
    ax2.set_ylabel(info["line_ylabel"])
    # 꺾은선 y축 범위 설정
    line_values = df_g[info["line_col"]].values
    line_min = np.nanmin(line_values)
    line_max = np.nanmax(line_values)
    line_range = line_max - line_min
    if line_range == 0:
        line_range = abs(line_max) * 0.1 if line_max != 0 else 1
    ax2.set_ylim(
        line_min - line_range * 0.15,
        line_max + line_range * 0.25
    )
    # -------------------------------------------------
    # 꺾은선 수치 표시
    # -------------------------------------------------
    for x, y in zip(df_g["year"], df_g[info["line_col"]]):
        if pd.isna(y):
            continue
        ax2.text(
            x,
            y + line_range * 0.04,
            info["line_fmt"].format(y),
            ha="center",
            va="bottom",
            fontsize=9,
            color=info["line_color"],
            fontweight="bold"
        )
    # -------------------------------------------------
    # 범례 합치기
    # -------------------------------------------------
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(
        lines1 + lines2,
        labels1 + labels2,
        loc="upper left",
        frameon=True
    )
    # -------------------------------------------------
    # 제목
    # -------------------------------------------------
    ax1.set_title(
        info["title"],
        fontsize=16,
        fontweight="bold"
    )
    plt.tight_layout()
    # -------------------------------------------------
    # 저장
    # -------------------------------------------------
    fig_path = fig_dir / info["filename"]
    plt.savefig(
        fig_path,
        dpi=300,
        bbox_inches="tight"
    )
    plt.show()
    print("그림 저장 완료:", fig_path)
