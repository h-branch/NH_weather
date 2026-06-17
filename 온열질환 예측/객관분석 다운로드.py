from pathlib import Path
from datetime import datetime, timedelta
import requests
import time
import os

# =====================================================
# 1. 기본 설정
# =====================================================
AUTH_KEY = "R3WTydcASuG1k8nXABrhgA"

if AUTH_KEY is None:
    raise ValueError("환경변수 KMA_AUTH_KEY에 기상청 API 키를 설정하세요.")

# 저장 폴더
RAW_DIR = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\온열질환\동네예보_raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# 수집 기간
START_DATE = "20230520"
END_DATE   = "20230930"

# 발표시간: 08시 기준
TARGET_TMFC_HOUR = "08"

# 필요한 변수
VARS = ["TMP", "REH", "WSD", "PCP"]

# 재시도 설정
MAX_RETRY = 3
SLEEP_SEC = 0.3


def date_range(start_yyyymmdd, end_yyyymmdd):
    start = datetime.strptime(start_yyyymmdd, "%Y%m%d")
    end = datetime.strptime(end_yyyymmdd, "%Y%m%d")

    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def make_tmef_list(tmfc_dt, lead_days=(0, 1, 2)):
    """
    tmfc_dt: 발표시각 datetime
    lead_days:
      0 = 당일
      1 = 내일
      2 = 모레

    반환:
      예보 대상시각 리스트
    """

    tmef_list = []

    for lead in lead_days:
        target_date = tmfc_dt.date() + timedelta(days=lead)

        # 당일 08시 발표라면 당일은 09~23시만 사용
        if lead == 0:
            start_hour = tmfc_dt.hour + 1
        else:
            start_hour = 0

        for h in range(start_hour, 24):
            tmef_dt = datetime(
                target_date.year,
                target_date.month,
                target_date.day,
                h
            )
            tmef_list.append(tmef_dt)

    return tmef_list


def download_one_file(var, tmfc, tmef, out_path):
    """
    var  : TMP, REH, WSD, PCP 등
    tmfc : 발표시각 문자열, 예: 2024071008
    tmef : 예보시각 문자열, 예: 2024071015
    """

    # =====================================================
    # 여기를 실제 기상청 APIHub 동네예보 URL로 수정
    # =====================================================
    BASE_URL = "https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-dfs_shrt_grd?"

    params = {
        "authKey": AUTH_KEY,
        "tmfc": tmfc,
        "tmef": tmef,
        "vars": var,
    }

    for attempt in range(1, MAX_RETRY + 1):
        try:
            response = requests.get(
                BASE_URL,
                params=params,
                timeout=30
            )

            if response.status_code == 200 and len(response.content) > 0:
                out_path.parent.mkdir(parents=True, exist_ok=True)

                with open(out_path, "wb") as f:
                    f.write(response.content)

                return True

            else:
                print(
                    f"[실패] {var} tmfc={tmfc} tmef={tmef} "
                    f"status={response.status_code} size={len(response.content)}"
                )

        except Exception as e:
            print(
                f"[오류] {var} tmfc={tmfc} tmef={tmef} "
                f"attempt={attempt} error={e}"
            )

        time.sleep(1)

    return False


def run_download():
    total_count = 0
    skip_count = 0
    success_count = 0
    fail_count = 0

    for cur_date in date_range(START_DATE, END_DATE):

        tmfc_dt = datetime(
            cur_date.year,
            cur_date.month,
            cur_date.day,
            int(TARGET_TMFC_HOUR)
        )

        tmfc = tmfc_dt.strftime("%Y%m%d%H")
        #tmef_list = make_tmef_list(tmfc_dt, lead_days=(0, 1, 2))
        tmef_list = make_tmef_list(tmfc_dt, lead_days=(0,))

        print("=" * 60)
        print("발표시각:", tmfc, "예보시각 수:", len(tmef_list))

        for tmef_dt in tmef_list:
            tmef = tmef_dt.strftime("%Y%m%d%H")

            for var in VARS:
                total_count += 1

                out_path = (
                    RAW_DIR
                    / var
                    / f"{var}_tmfc{tmfc}_tmef{tmef}.bin"
                )

                # 이미 있으면 건너뛰기
                if out_path.exists() and out_path.stat().st_size > 0:
                    skip_count += 1
                    continue

                ok = download_one_file(
                    var=var,
                    tmfc=tmfc,
                    tmef=tmef,
                    out_path=out_path
                )

                if ok:
                    success_count += 1
                else:
                    fail_count += 1

                time.sleep(SLEEP_SEC)

        print(
            f"누적 total={total_count}, "
            f"skip={skip_count}, "
            f"success={success_count}, "
            f"fail={fail_count}"
        )

    print("\n다운로드 완료")
    print("전체:", total_count)
    print("건너뜀:", skip_count)
    print("성공:", success_count)
    print("실패:", fail_count)


run_download()
