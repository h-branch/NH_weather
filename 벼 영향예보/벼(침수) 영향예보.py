import numpy as np
import pandas as pd
import geopandas as gpd
import requests
from datetime import datetime, timedelta
import pyproj
from pyproj import CRS, Transformer
import os
import re


Key='R3WTydcASuG1k8nXABrhgA'
short_url='https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-dfs_shrt_grd?'
out_dir='C:/Users/lhj15/OneDrive/문서/[NH]/강수위험지도/260428 읍면동 AMC2/event2'
os.makedirs(out_dir, exist_ok=True)
# 발표시간: 0000년 00월 00일 08시 고정
tmfc_dt=datetime(2026,4,30,8)
# 글피 자정까지
end_dt=datetime(tmfc_dt.year, tmfc_dt.month, tmfc_dt.day) + timedelta(days=3)
# 발효시간 1시간 간격
tmef_list = pd.date_range(start=tmfc_dt, end=end_dt, freq="1h")
# =========================
# API 요청 + 파일 저장
# =========================
log_list=[]
for tmef_dt in tmef_list:
    tmfc=tmfc_dt.strftime("%Y%m%d%H")
    tmef=tmef_dt.strftime("%Y%m%d%H")
    option={"tmfc": tmfc, "tmef": tmef, "vars": "PCP", "authKey": Key}
    response=requests.get(short_url, params=option)
    # 저장 파일명
    out_file=os.path.join(out_dir, f"PCP_tmfc{tmfc}_tmef{tmef}.bin")
    # 응답 저장
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(response.text)
    log_list.append({"tmfc": tmfc, "tmef": tmef, "status_code": response.status_code, "file": out_file})
# 저장 로그 확인
df_log=pd.DataFrame(log_list)
df_log.to_csv(os.path.join(out_dir, "download_log.csv"), index=False, encoding="utf-8-sig")


# =========================
# 설정
# =========================
in_dir='C:/Users/lhj15/OneDrive/문서/[NH]/강수위험지도/260428 읍면동 AMC2/event2'
out_dir='C:/Users/lhj15/OneDrive/문서/[NH]/강수위험지도/260428 읍면동 AMC2/3hr2'
os.makedirs(out_dir, exist_ok=True)
ny, nx=253, 149
window=3
# =========================
# 함수
# =========================
def get_tmef_from_filename(path):
    fname=os.path.basename(path)
    times=re.findall(r"\d{10}", fname)
    if len(times) == 0:
        raise ValueError(f"파일명에서 날짜시간 10자리를 찾지 못했습니다: {fname}")
    return times[-1]
  
def read_pcp_bin(path, ny=253, nx=149):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()
    values=text.replace("\n", ",").replace("\r", ",").split(",")
    values=[v.strip() for v in values if v.strip() != ""]
    clean_values=[]
    for v in values:
        try:
            clean_values.append(float(v))
        except ValueError:
            raise ValueError(
                f"\n숫자로 변환할 수 없는 값이 있습니다."
                f"\n파일명: {os.path.basename(path)}"
                f"\n문제 값: {repr(v)}"
                f"\n파일 앞부분:\n{text[:500]}"
            )
    arr=np.array(clean_values, dtype=float)
    # 결측값 처리
    arr=np.where(arr <= -90, np.nan, arr)
    if arr.size != ny * nx:
        raise ValueError(
            f"\n격자 크기 불일치"
            f"\n파일명: {os.path.basename(path)}"
            f"\n값 개수: {arr.size}"
            f"\n기대값: {ny * nx}"
            f"\n파일 앞부분:\n{text[:500]}"
        )
    return arr.reshape(ny, nx)
# =========================
# 파일 목록 만들기
# =========================
files=[]
for fname in os.listdir(in_dir):
    fpath=os.path.join(in_dir, fname)
    if not os.path.isfile(fpath):
        continue
    if fname.lower().endswith(".csv"):
        continue
    if re.search(r"\d{10}", fname):
        files.append(fpath)
files=sorted(files)
print("입력 파일 개수:", len(files))
for f in files[:10]:
    print(os.path.basename(f))
if len(files) == 0:
    raise FileNotFoundError("입력 파일을 찾지 못했습니다. 파일명에 10자리 날짜시간이 있는지 확인하세요.")
# =========================
# tmef 기준 정렬
# =========================
file_df=pd.DataFrame({
    "file": files,
    "tmef": [get_tmef_from_filename(f) for f in files]
})
file_df["tmef_dt"]=pd.to_datetime(file_df["tmef"], format="%Y%m%d%H")
file_df=file_df.sort_values("tmef_dt").reset_index(drop=True)
print(file_df.head())
print(file_df.tail())
# =========================
# 3시간 이동누적 계산
# =========================
log_list=[]
error_list=[]
for i in range(window - 1, len(file_df)):
    selected=file_df.iloc[i - window + 1 : i + 1]
    arr_list=[]
    ok=True
    for f in selected["file"]:
        try:
            arr=read_pcp_bin(f, ny=ny, nx=nx)
            arr_list.append(arr)
        except Exception as e:
            ok=False
            error_list.append({
                "target_tmef": file_df.loc[i, "tmef"],
                "problem_file": os.path.basename(f),
                "error": str(e)
            })
            print("오류 발생:", os.path.basename(f))
            print(e)
            break
    # 3개 파일 중 하나라도 문제 있으면 해당 3시간 누적은 건너뜀
    if not ok:
        continue
    arr_3hr=np.nansum(arr_list, axis=0)
    tmef=file_df.loc[i, "tmef"]
    out_file=os.path.join(
        out_dir,
        f"PCP_3hr_sum_tmef{tmef}.bin"
    )
    np.savetxt(
        out_file,
        arr_3hr,
        fmt="%.2f",
        delimiter=","
    )
    log_list.append({
        "tmef": tmef,
        "start_tmef": selected["tmef"].iloc[0],
        "end_tmef": selected["tmef"].iloc[-1],
        "file": out_file,
        "min": np.nanmin(arr_3hr),
        "max": np.nanmax(arr_3hr)
    })
df_3hr_log=pd.DataFrame(log_list)
df_error_log=pd.DataFrame(error_list)
df_3hr_log.to_csv(
    os.path.join(out_dir, "PCP_3hr_sum_log.csv"),
    index=False,
    encoding="utf-8-sig"
)
df_error_log.to_csv(
    os.path.join(out_dir, "PCP_3hr_error_log.csv"),
    index=False,
    encoding="utf-8-sig"
)
print("3시간 누적 파일 개수:", len(df_3hr_log))
print("오류 파일 개수:", len(df_error_log))


