import re
import numpy as np
import pandas as pd
from pathlib import Path


# =====================================================
# 1. 경로 설정
# =====================================================
# AWS 객관분석 시강우 파일 폴더
aws_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\강수위험지도\AWS객관분석")
# 동네예보 시강우 파일 폴더
dfs_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\강수위험지도\동네예보\PCP_1hr")
# 결과 저장 폴더
save_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\벼침수위험_작업\강우자료_3시간")
save_dir.mkdir(parents=True, exist_ok=True)


# =====================================================
# 2. 기준시각 설정
# =====================================================
# 자동 설정: 현재 시각 기준
#issue_time = pd.Timestamp.now(tz="Asia/Seoul").floor("h").tz_localize(None)
# 테스트용으로 직접 지정하고 싶으면 아래 사용
issue_time = pd.Timestamp("2025-07-20 09:00")
print("기준시각:", issue_time)


# =====================================================
# 3. 동네예보 발표시각 선택
# =====================================================
def get_latest_tmfc(now, delay_minutes=40):
    """
    현재 시각 기준으로 사용 가능한 최신 동네예보 발표시각을 선택합니다.
    발표시각: 02, 05, 08, 11, 14, 17, 20, 23시
    delay_minutes: 자료 수신 지연 고려
    """
    base_hours = [2, 5, 8, 11, 14, 17, 20, 23]
    now_adj = now - pd.Timedelta(minutes=delay_minutes)
    day = now_adj.normalize()
    candidates = []
    for h in base_hours:
        candidates.append(day + pd.Timedelta(hours=h))
    # 새벽에는 전날 23시도 후보
    candidates.append(day - pd.Timedelta(days=1) + pd.Timedelta(hours=23))
    candidates = [t for t in candidates if t <= now_adj]
    return max(candidates)
tmfc = get_latest_tmfc(issue_time)
print("동네예보 사용 발표시각 tmfc:", tmfc)


# =====================================================
# 4. 분석 기간 설정
# =====================================================
aws_start = issue_time - pd.Timedelta(days=4)
aws_end = issue_time
dfs_start = tmfc
dfs_end = tmfc + pd.Timedelta(days=3)
print("AWS 기간:", aws_start, "~", aws_end)
print("DFS 기간:", dfs_start, "~", dfs_end)


# =====================================================
# 5. API 저장 경로 및 인증키 설정
# =====================================================
import requests
from pathlib import Path
from time import sleep
# API 인증키
AUTH_KEY = "R3WTydcASuG1k8nXABrhgA"
# 저장 기본 폴더
download_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\벼침수위험_작업\api_download")
# AWS 객관분석 저장 폴더
aws_save_dir = download_dir / "AWS_OBJECTIVE_1H"
# 동네예보 저장 폴더
dfs_save_dir = download_dir / "DFS_PCP_1H"
aws_save_dir.mkdir(parents=True, exist_ok=True)
dfs_save_dir.mkdir(parents=True, exist_ok=True)
print("AWS 저장 폴더:", aws_save_dir)
print("DFS 저장 폴더:", dfs_save_dir)


# =====================================================
# 6. 요청 시간 목록 생성
# =====================================================
# AWS 객관분석: 과거 4일 ~ 현재
aws_times = pd.date_range(
    start=aws_start,
    end=aws_end,
    freq="1h"
)
# 동네예보: 오늘 예보시간 기준 ~ 미래 3일
dfs_tmefs = pd.date_range(
    start=dfs_start,
    end=dfs_end,
    freq="1h"
)
print("AWS 요청 시간 수:", len(aws_times))
print("DFS 요청 시간 수:", len(dfs_tmefs))
print("\nAWS 앞 5개")
print(aws_times[:5])
print("\nAWS 뒤 5개")
print(aws_times[-5:])
print("\nDFS 앞 5개")
print(dfs_tmefs[:5])
print("\nDFS 뒤 5개")
print(dfs_tmefs[-5:])


# =====================================================
# 7. 공통 다운로드 함수
# =====================================================
def download_api_text(
    url,
    params,
    out_file,
    max_retry=3,
    sleep_sec=1.0,
    overwrite=False
):
    """
    API 응답을 텍스트 파일로 저장하는 함수입니다.
    Parameters
    ----------
    url : str
        API 주소
    params : dict
        API 요청 파라미터
    out_file : Path or str
        저장 파일 경로
    max_retry : int
        재시도 횟수
    sleep_sec : float
        요청 간 대기 시간
    overwrite : bool
        기존 파일 덮어쓰기 여부
    """
    out_file = Path(out_file)
    if out_file.exists() and not overwrite:
        print("이미 있음, 건너뜀:", out_file.name)
        return True
    for attempt in range(1, max_retry + 1):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=60
            )
            if response.status_code != 200:
                print(
                    f"HTTP 오류 {response.status_code} "
                    f"({attempt}/{max_retry}) : {out_file.name}"
                )
                sleep(sleep_sec)
                continue
            text = response.text
            if text is None or len(text.strip()) == 0:
                print(
                    f"빈 응답 ({attempt}/{max_retry}) : {out_file.name}"
                )
                sleep(sleep_sec)
                continue
            # 기상청 API가 오류 메시지를 텍스트로 반환하는 경우 방지
            check_text = text[:500].lower()
            if "error" in check_text or "exception" in check_text:
                print(
                    f"API 오류문 가능성 ({attempt}/{max_retry}) : {out_file.name}"
                )
                print(text[:300])
                sleep(sleep_sec)
                continue
            out_file.write_text(text, encoding="utf-8")
            print("저장 완료:", out_file.name)
            return True
        except Exception as e:
            print(
                f"다운로드 오류 ({attempt}/{max_retry}) : "
                f"{out_file.name} / {e}"
            )
            sleep(sleep_sec)
    print("최종 실패:", out_file.name)
    return False


# =====================================================
# 8. AWS 객관분석 시강우 API 저장
# =====================================================
# TODO: 실제 AWS 객관분석 격자 API 주소로 수정 필요
AWS_API_URL = "https://apihub.kma.go.kr/api/typ01/cgi-bin/aws/nph-aws_min_obj?"
aws_success = []
aws_fail = []
for t in aws_times:
    tm = t.strftime("%Y%m%d%H%M")
    out_file = aws_save_dir / f"AWS_OBJECTIVE_1H_{tm}.txt"
    # TODO: 실제 API 파라미터명에 맞게 수정 필요
    # 예시입니다.
    params = {
        "tm": tm,
        "obs": "rn_60m",
        "authKey": AUTH_KEY,
        "obj": "mq",
        "map": "D3",
        "grid": "1",
        "stn": "0"
    }
    ok = download_api_text(
        url=AWS_API_URL,
        params=params,
        out_file=out_file,
        max_retry=3,
        sleep_sec=1.0,
        overwrite=False
    )
    if ok:
        aws_success.append(tm)
    else:
        aws_fail.append(tm)
print("====================================")
print("AWS 다운로드 완료")
print("성공:", len(aws_success))
print("실패:", len(aws_fail))
print("실패 시간:", aws_fail[:20])
print("====================================")


# =====================================================
# 9. 동네예보 시강우 API 저장
# =====================================================
DFS_API_URL = "https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-dfs_shrt_grd"
tmfc_str = tmfc.strftime("%Y%m%d%H%M")
dfs_success = []
dfs_fail = []
for tmef in dfs_tmefs:
    tmef_str = tmef.strftime("%Y%m%d%H%M")
    out_file = dfs_save_dir / f"PCP_tmfc{tmfc_str}_tmef{tmef_str}.bin"
    params = {
        "tmfc": tmfc_str,
        "tmef": tmef_str,
        "vars": "PCP",
        "authKey": AUTH_KEY
    }
    ok = download_api_text(
        url=DFS_API_URL,
        params=params,
        out_file=out_file,
        max_retry=3,
        sleep_sec=1.0,
        overwrite=False
    )
    if ok:
        dfs_success.append(tmef_str)
    else:
        dfs_fail.append(tmef_str)
print("====================================")
print("DFS 다운로드 완료")
print("성공:", len(dfs_success))
print("실패:", len(dfs_fail))
print("실패 시간:", dfs_fail[:20])
print("====================================")


# =====================================================
# 10. 다운로드 로그 저장
# =====================================================
download_log_rows = []
for tm in aws_success:
    download_log_rows.append({
        "data_type": "AWS",
        "status": "success",
        "time": tm
    })
for tm in aws_fail:
    download_log_rows.append({
        "data_type": "AWS",
        "status": "fail",
        "time": tm
    })
for tmef in dfs_success:
    download_log_rows.append({
        "data_type": "DFS",
        "status": "success",
        "time": tmef,
        "tmfc": tmfc_str
    })
for tmef in dfs_fail:
    download_log_rows.append({
        "data_type": "DFS",
        "status": "fail",
        "time": tmef,
        "tmfc": tmfc_str
    })
download_log = pd.DataFrame(download_log_rows)
log_file = download_dir / "download_log.csv"
download_log.to_csv(
    log_file,
    index=False,
    encoding="utf-8-sig"
)
print("다운로드 로그 저장:", log_file)


# =====================================================
# 11. 저장 파일 확인
# =====================================================
aws_saved = sorted(aws_save_dir.glob("*.txt"))
dfs_saved = sorted(dfs_save_dir.glob("*.bin"))
print("AWS 저장 파일 수:", len(aws_saved))
print("DFS 저장 파일 수:", len(dfs_saved))
print("\nAWS 앞 5개")
for f in aws_saved[:5]:
    print(f.name)
print("\nAWS 뒤 5개")
for f in aws_saved[-5:]:
    print(f.name)
print("\nDFS 앞 5개")
for f in dfs_saved[:5]:
    print(f.name)
print("\nDFS 뒤 5개")
for f in dfs_saved[-5:]:
    print(f.name)
