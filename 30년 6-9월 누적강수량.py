import os
import time
import requests
import pandas as pd
from io import StringIO
from requests.exceptions import ChunkedEncodingError, ReadTimeout, ConnectionError
from pathlib import Path
import matplotlib.pyplot as plt

plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False


path='https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd3.php?'
key='R3WTydcASuG1k8nXABrhgA'
out_dir = r"C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/260512 6-9 강수/asos_daily_clean"
os.makedirs(out_dir, exist_ok=True)
asos_cols = [
    "tm",              # 0  날짜, YYYYMMDD
    "stn",             # 1  지점번호
    "ws_avg",          # 2  평균풍속
    "wr_day",          # 3  일풍정
    "wd_max",          # 4  최대풍향
    "ws_max",          # 5  최대풍속
    "ws_max_tm",       # 6  최대풍속시각
    "wd_ins",          # 7  최대순간풍향
    "ws_ins",          # 8  최대순간풍속
    "ws_ins_tm",       # 9  최대순간풍속시각
    "ta_avg",          # 10 평균기온
    "ta_max",          # 11 최고기온
    "ta_max_tm",       # 12 최고기온시각
    "ta_min",          # 13 최저기온
    "ta_min_tm",       # 14 최저기온시각
    "td_avg",          # 15 평균이슬점온도
    "ts_avg",          # 16 평균지면온도
    "tg_min",          # 17 최저초상온도
    "hm_avg",          # 18 평균상대습도
    "hm_min",          # 19 최저상대습도
    "hm_min_tm",       # 20 최저상대습도시각
    "pv_avg",          # 21 평균증기압
    "ev_s",            # 22 소형증발량
    "ev_l",            # 23 대형증발량
    "fg_dur",          # 24 안개계속시간
    "pa_avg",          # 25 평균현지기압
    "ps_avg",          # 26 평균해면기압
    "ps_max",          # 27 최고해면기압
    "ps_max_tm",       # 28 최고해면기압시각
    "ps_min",          # 29 최저해면기압
    "ps_min_tm",       # 30 최저해면기압시각
    "ca_tot",          # 31 평균전운량
    "ss_day",          # 32 일조시간
    "ss_dur",          # 33 가조시간
    "ss_cmb",          # 34 합계일조시간 또는 관련값
    "si_day",          # 35 일사량
    "si_60m_max",      # 36 1시간 최다일사량
    "si_60m_max_tm",   # 37 1시간 최다일사량시각
    "rn_day",          # 38 일강수량
    "rn_d99",          # 39 9-9 강수량
    "rn_dur",          # 40 강수계속시간
    "rn_60m_max",      # 41 1시간 최다강수량
    "rn_60m_max_tm",   # 42 1시간 최다강수량시각
    "rn_10m_max",      # 43 10분 최다강수량
    "rn_10m_max_tm",   # 44 10분 최다강수량시각
    "rn_pow",          # 45 강수강도
    "rn_pow_tm",       # 46 강수강도시각
    "sd_new",          # 47 신적설
    "sd_new_tm",       # 48 신적설시각
    "sd_max",          # 49 최심적설
    "sd_max_tm",       # 50 최심적설시각
    "te_5",            # 51 5cm 지중온도
    "te_10",           # 52 10cm 지중온도
    "te_15",           # 53 15cm 지중온도
    "te_30",           # 54 30cm 지중온도
    "te_50"            # 55 50cm 지중온도
]


def parse_kma_asos_daily_text(text):
    """
    기상청 API Hub ASOS 일자료 텍스트 응답을 DataFrame으로 변환합니다.
    처리 내용
    1. #으로 시작하는 설명 줄 제거
    2. 빈 줄 제거
    3. 공백 구분 자료를 DataFrame으로 변환
    4. 컬럼명 부여
    5. 날짜, 연도, 월, 강수량 컬럼 정리
    """
    lines = text.splitlines()
    data_lines = []
    for line in lines:
        line_strip = line.strip()
        # 빈 줄 제거
        if line_strip == "":
            continue
        # 설명 줄 제거
        if line_strip.startswith("#"):
            continue
        data_lines.append(line_strip)
    if len(data_lines) == 0:
        return pd.DataFrame()
    clean_text = "\n".join(data_lines)
    df = pd.read_csv(
        StringIO(clean_text),
        sep=r"\s+",
        header=None
    )
    # 컬럼 개수 확인
    if df.shape[1] != len(asos_cols):
        print("컬럼 개수 불일치")
        print("읽은 컬럼 수:", df.shape[1])
        print("기대 컬럼 수:", len(asos_cols))
        print(df.head())
        raise ValueError("API 응답 컬럼 구조가 예상과 다릅니다.")
    df.columns = asos_cols
    # 날짜 처리
    df["tm"] = df["tm"].astype(str)
    df["date"] = pd.to_datetime(df["tm"], format="%Y%m%d", errors="coerce")
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    # 지점번호 정리
    df["stn"] = pd.to_numeric(df["stn"], errors="coerce").astype("Int64")
    # 강수량 숫자 변환
    df["rn_day"] = pd.to_numeric(df["rn_day"], errors="coerce")
    # 중요:
    # 기상청 자료에서 강수 관련 -9, -99 값이 섞여 있음
    # 6~9월 누적강수량 산정 목적에서는 음수값을 0으로 처리
    # 단, 엄밀한 결측 검토가 필요하면 별도 QC 필요
    df["rn_day_raw"] = df["rn_day"]
    df["rn_day"] = df["rn_day"].where(df["rn_day"] >= 0, 0)
    return df


def request_asos_daily_period(tm1, tm2, stn="0", max_retry=5):
    """
    기상청 API Hub ASOS 일자료 기간 요청 함수
    tm1: 시작일, 예: '19960601'
    tm2: 종료일, 예: '19960930'
    stn: 지점번호, 전체지점은 보통 '0'
    """
    option = {
        "tm1": tm1,
        "tm2": tm2,
        "stn": stn,
        "help": "0",
        "authKey": key
    }
    for attempt in range(1, max_retry + 1):
        try:
            response = requests.get(
                path,
                params=option,
                timeout=(10, 120)
            )
            response.raise_for_status()
            return response.text
        except (ChunkedEncodingError, ReadTimeout, ConnectionError) as e:
            print(f"[재시도 {attempt}/{max_retry}] {tm1}~{tm2} 연결 오류")
            print(e)
            time.sleep(3 * attempt)
        except Exception as e:
            print(f"[중단] {tm1}~{tm2} 요청 오류")
            print(e)
            return None
    print(f"[실패] {tm1}~{tm2} 최대 재시도 초과")
    return None


for year in range(1996, 2026):
    tm1 = f"{year}0601"
    tm2 = f"{year}0930"
    out_csv = os.path.join(out_dir, f"{year}_data_clean.csv")
    # 이미 저장된 파일은 건너뜀
    if os.path.exists(out_csv):
        print(f"이미 있음, 건너뜀: {year}")
        continue
    print(f"수집 중: {tm1} ~ {tm2}")
    text = request_asos_daily_period(
        tm1=tm1,
        tm2=tm2,
        stn="0",
        max_retry=5
    )
    if text is None:
        print(f"수집 실패: {year}")
        continue
    try:
        df_year = parse_kma_asos_daily_text(text)
        if df_year.empty:
            print(f"자료 없음: {year}")
            continue
        # 혹시 6~9월 이외 자료가 섞이면 제거
        df_year = df_year[df_year["month"].isin([6, 7, 8, 9])].copy()
        # 전처리된 연도별 CSV 저장
        df_year.to_csv(
            out_csv,
            index=False,
            encoding="utf-8-sig"
        )
        print(f"저장 완료: {out_csv}, 행 수: {len(df_year)}")
    except Exception as e:
        print(f"전처리 실패: {year}")
        print(e)
    # 서버 부담 줄이기
    time.sleep(1)


out_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\이상기후대응팀\260512 6-9 강수\asos_daily_clean")
csv_files = sorted(out_dir.glob("*_data_clean.csv"))
print("병합 대상 파일 수:", len(csv_files))
for f in csv_files[:10]:
    print(f.name)
df_list = []
for file in csv_files:
    print("읽는 중:", file.name)
    df_temp = pd.read_csv(
        file,
        encoding="utf-8-sig"
    )
    df_list.append(df_temp)
df_all = pd.concat(df_list, ignore_index=True)
print("전체 행 수:", len(df_all))
print(df_all.head())
print(df_all.columns)


df_rain = df_all[["date", "year", "month", "stn", "rn_day"]].copy()
df_station_year = (
    df_rain
    .groupby(["year", "stn"], as_index=False)
    .agg(
        rain_jun_sep=("rn_day", "sum"),
        obs_days=("rn_day", "count")
    )
)
print(df_station_year.head())


df_korea = (
    df_station_year
    .groupby("year", as_index=False)
    .agg(
        total_rain_all_stations=("rain_jun_sep", "sum"),
        station_count=("stn", "nunique")
    )
)
df_korea["rain_jun_sep_mean"] = (
    df_korea["total_rain_all_stations"] / df_korea["station_count"]
)
print(df_korea)


# =====================================================
# 평균값 계산
# =====================================================
mean_30yr = df_korea["rain_jun_sep_mean"].mean()
mean_10yr = df_korea[
    df_korea["year"] >= df_korea["year"].max() - 9
]["rain_jun_sep_mean"].mean()
mean_5yr = df_korea[
    df_korea["year"] >= df_korea["year"].max() - 4
]["rain_jun_sep_mean"].mean()
# =====================================================
# 그래프 작성
# =====================================================
fig, ax = plt.subplots(figsize=(14, 6))
# 1. 연도별 강수량 막대그래프
ax.bar(
    df_korea["year"],
    df_korea["rain_jun_sep_mean"],
    label="전국 평균 6~9월 누적강수량",
    color='skyblue'
)
# 2. 30년 평균선
#ax.axhline(
#    mean_30yr,
#    color='green',
#    linestyle="-",
#    linewidth=2,
#    label=f"1996~2025 평균: {mean_30yr:.1f} mm"
#)
# 3. 최근 10년 평균선
ax.axhline(
    mean_10yr,
    color='red',
    linestyle="--",
    linewidth=2,
    label=f"최근 10년 평균: {mean_10yr:.1f} mm"
)
# 4. 최근 5년 평균선
#ax.axhline(
#    mean_5yr,
#    color='red',
#    linestyle=":",
#    linewidth=2.5,
#    label=f"최근 5년 평균: {mean_5yr:.1f} mm"
#)
# =====================================================
# 축, 제목, 범례
# =====================================================
ax.set_title("1996~2025년 전국 6~9월 누적강수량", fontsize=16)
ax.set_xlabel("연도")
ax.set_ylabel("6~9월 누적강수량 평균(mm)")
ax.set_xticks(df_korea["year"])
ax.set_xticklabels(df_korea["year"], rotation=45)
ax.grid(axis="y", alpha=0.3)
ax.legend()
plt.tight_layout()
plt.show()
