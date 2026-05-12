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


# =========================
# 설정
# =========================
in_dir='C:/Users/lhj15/OneDrive/문서/[NH]/강수위험지도/260428 읍면동 AMC2/3hr2'
nx, ny=149, 253
n_expected=nx * ny
# =========================
# 3hr 폴더 안 파일 찾기
# =========================
bin_files=[]
for fname in os.listdir(in_dir):
    fpath=os.path.join(in_dir, fname)
    if not os.path.isfile(fpath):
        continue
    if fname.lower().endswith(".csv"):
        continue
    if re.search(r"\d{10}", fname):
        bin_files.append(fpath)
bin_files=sorted(bin_files)
print("전처리 대상 파일 개수:", len(bin_files))
for f in bin_files[:10]:
    print(os.path.basename(f))
# =========================
# 전체 파일 전처리
# =========================
arr_list=[]
name_list=[]
for f_path in bin_files:
    with open(f_path, "r", encoding="utf-8") as f:
        text = f.read()
    # 핵심 수정:
    # 줄바꿈을 쉼표로 바꾼 뒤 쉼표 기준 분리
    parts=text.replace("\n", ",").replace("\r", ",").split(",")
    # 공백 제거 + 빈 문자열 제거
    parts=[x.strip() for x in parts]
    parts=[x for x in parts if x != ""]
    print("=" * 60)
    print("파일명:", os.path.basename(f_path))
    print("읽은 값 개수:", len(parts))
    print("기대 값 개수:", n_expected)
    print("앞 10개:", parts[:10])
    if len(parts) != n_expected:
        raise ValueError(
            f"격자 개수 불일치: {os.path.basename(f_path)} / "
            f"읽은 값 개수={len(parts)}, 기대 값 개수={n_expected}"
        )
    values=np.array([float(x) for x in parts], dtype=float)
    print("float 변환 후 개수:", values.size)
    arr=values.reshape(ny, nx)
    print("shape:", arr.shape)
    # -99 결측 처리
    arr[arr == -99.0]=np.nan
    arr[arr <= -90.0]=np.nan
    print("최소:", np.nanmin(arr))
    print("최대:", np.nanmax(arr))
    arr_list.append(arr)
    name_list.append(os.path.basename(f_path))
print("전체 전처리 완료")
print("전처리 배열 개수:", len(arr_list))


cell_size=5000
center_lat=38
center_lon=126
center_grid=(136, 43)
projection=pyproj.Proj(proj='lcc', lat_1=30, lat_2=60, lat_0=center_lat, lon_0=center_lon, datum='WGS84')
j_indices, i_indices=np.meshgrid(np.arange(nx), np.arange(ny))
x=(j_indices-center_grid[1])*cell_size
y=(i_indices-center_grid[0])*cell_size
lon, lat=projection(x, y, inverse=True)
print(lon.shape)
print(lat.shape)


# 동네예보 CRS
kma_lcc=CRS.from_proj4(
    "+proj=lcc +lat_1=30 +lat_2=60 "
    "+lat_0=38 +lon_0=126 "
    "+x_0=215000 +y_0=680000 "
    "+a=6371008.77 +b=6371008.77 "
    "+units=m +no_defs"
)
# 위경도(WGS84)
wgs84=CRS.from_epsg(4326)
# 역변환기: 투영좌표 -> 위경도
transformer=Transformer.from_crs(kma_lcc, wgs84, always_xy=True)
# 격자 번호 (1-based)
xg=np.arange(1, nx + 1)
yg=np.arange(1, ny + 1)
# 격자 중심 투영좌표
x_center=xg * cell_size
y_center=yg * cell_size
# 2차원 mesh
Xc, Yc=np.meshgrid(x_center, y_center)
NX, NY=np.meshgrid(xg, yg)
# 위경도 변환
lon, lat=transformer.transform(Xc, Yc)
# 표 형태로 정리
df_grid = pd.DataFrame({
    "nx": NX.ravel(),
    "ny": NY.ravel(),
    "x_m": Xc.ravel(),
    "y_m": Yc.ravel(),
    "lon": lon.ravel(),
    "lat": lat.ravel()
})
print(df_grid.head())
print(df_grid.shape)


sido='C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/#map_basic/ramp_gis/sido_ramp.shp'
emd='C:/Users/lhj15/OneDrive/문서/[NH]/강수위험지도/영향한계강우량/gpd_emd_weighted.shp'
gpd_sido=gpd.read_file(sido)
gpd_emd=gpd.read_file(emd)
if gpd_sido.crs != gpd_emd.crs:
    gpd_emd = gpd_emd.to_crs(gpd_sido.crs)


sido_map={
    '11': '서울특별시',
    '26': '부산광역시',
    '27': '대구광역시',
    '28': '인천광역시',
    '29': '광주광역시',
    '30': '대전광역시',
    '31': '울산광역시',
    '36': '세종특별자치시',
    '41': '경기도',
    '42': '강원도',
    '43': '충청북도',
    '44': '충청남도',
    '45': '전라북도',
    '46': '전라남도',
    '47': '경상북도',
    '48': '경상남도',
    '50': '제주도'
}
gpd_emd['sd_cd']=gpd_emd['EMD_CD'].astype(str).str[:2]
gpd_emd['sido']=gpd_emd['sd_cd'].map(sido_map)
gpd_emd


gpd_grid=gpd.GeoDataFrame(
    df_grid.copy(),
    geometry=gpd.points_from_xy(df_grid["lon"], df_grid["lat"]),
    crs="EPSG:4326"
)
# 읍면동 좌표계와 통일
if gpd_emd.crs is None:
    gpd_emd=gpd_emd.set_crs("EPSG:4326")
gpd_grid=gpd_grid.to_crs(gpd_emd.crs)
print(gpd_grid.head())
print(gpd_grid.shape)
print(gpd_grid.crs)
print(gpd_emd.crs)


bin_dir='C:/Users/lhj15/OneDrive/문서/[NH]/강수위험지도/260428 읍면동 AMC2/3hr2'
nx, ny=149, 253
n_expected=nx * ny
bin_files=[]
for fname in os.listdir(bin_dir):
    fpath=os.path.join(bin_dir, fname)
    if not os.path.isfile(fpath):
        continue
    if fname.lower().endswith(".csv"):
        continue
    if re.search(r"\d{10}", fname):
        bin_files.append(fpath)
bin_files=sorted(bin_files)
print("3시간 누적강수 파일 개수:", len(bin_files))
for f in bin_files[:5]:
    print(os.path.basename(f))


# =====================================================
# 전체 bin 파일을 배열로 전처리
# =====================================================
arr_list=[]
name_list=[]
for f_path in bin_files:
    with open(f_path, "r", encoding="utf-8") as f:
        text = f.read()
    # 줄바꿈까지 쉼표로 처리해야 값 개수가 맞음
    parts=text.replace("\n", ",").replace("\r", ",").split(",")
    parts=[x.strip() for x in parts]
    parts=[x for x in parts if x != ""]
    if len(parts) != n_expected:
        raise ValueError(
            f"격자 개수 불일치: {os.path.basename(f_path)} / "
            f"읽은 값={len(parts)}, 기대값={n_expected}"
        )
    values=np.array([float(x) for x in parts], dtype=float)
    arr=values.reshape(ny, nx)
    # 결측 처리
    arr[arr == -99.0]=np.nan
    arr[arr <= -90.0]=np.nan
    arr_list.append(arr)
    name_list.append(os.path.basename(f_path))
print("전처리 완료 배열 개수:", len(arr_list))
print("첫 배열 shape:", arr_list[0].shape)


# 3. 격자점과 읍면동 공간매칭
# =====================================================
# gpd_emd_sub가 따로 없다면 전체 읍면동 자료를 사용
gpd_emd_sub = gpd_emd.copy()
# 좌표계 일치 확인
if gpd_grid.crs != gpd_emd_sub.crs:
    gpd_emd_sub=gpd_emd_sub.to_crs(gpd_grid.crs)
grid_emd_join=gpd.sjoin(
    gpd_grid,
    gpd_emd_sub,
    how="left",
    predicate="within"
)
missing_count=grid_emd_join["EMD_CD"].isna().sum()
print("전체 격자 수:", len(grid_emd_join))
print("읍면동 매칭 안 된 격자 수:", missing_count)
print("읍면동 매칭 격자 수:", len(grid_emd_join) - missing_count)


# 4. 3시간 누적강수 bin 파일 목록
# =====================================================
bin_files = []
for fname in os.listdir(bin_dir):
    fpath=os.path.join(bin_dir, fname)
    if not os.path.isfile(fpath):
        continue
    # 로그 csv 제외
    if fname.lower().endswith(".csv"):
        continue
    # 파일명에 2025071705 같은 10자리 시간이 들어간 파일만 사용
    if re.search(r"\d{10}", fname):
        bin_files.append(fpath)
bin_files=sorted(bin_files)
print("3시간 누적강수 bin 파일 개수:", len(bin_files))
for f in bin_files[:5]:
    print(os.path.basename(f))
if len(bin_files) == 0:
    raise FileNotFoundError(f"3hr 폴더에서 파일을 찾지 못했습니다: {bin_dir}")


# 5. bin 파일 읽기 함수
# =====================================================
def read_3hr_bin(f_path, nx=149, ny=253):
    n_expected=nx * ny
    with open(f_path, "r", encoding="utf-8") as f:
        text=f.read()
    # 줄바꿈까지 쉼표로 바꾼 뒤 분리
    parts=text.replace("\n", ",").replace("\r", ",").split(",")
    # 공백 제거 + 빈 문자열 제거
    parts=[x.strip() for x in parts]
    parts=[x for x in parts if x != ""]
    if len(parts) != n_expected:
        raise ValueError(
            f"격자 개수 불일치: {os.path.basename(f_path)} / "
            f"읽은 값 개수={len(parts)}, 기대 값 개수={n_expected}"
        )
    values=np.array([float(x) for x in parts], dtype=float)
    arr=values.reshape(ny, nx)
    # 결측 처리
    arr[arr == -99.0]=np.nan
    arr[arr <= -90.0]=np.nan
    return arr


# 6. 위험단계 분류 함수
# =====================================================
def classify_stage(row):
    rain=row["rain3h"]
    if pd.isna(rain):
        return np.nan
    elif rain < row["cn2a"]:
        return "관심"
    elif rain < row["cn2b"]:
        return "주의"
    elif rain < row["cn2c"]:
        return "경고"
    else:
        return "위험"


# =====================================================
# 6-1. grid_id가 있는 격자-읍면동 매칭표 재생성
# =====================================================
# 1) gpd_grid에 grid_id 먼저 부여
gpd_grid=gpd_grid.reset_index(drop=True)
gpd_grid["grid_id"]=gpd_grid.index
# 2) 읍면동 기준 컬럼 정리
need_cols=[
    "EMD_CD", "EMD_NM", "SGG", "sido",
    "cn2a", "cn2b", "cn2c", "geometry"
]
gpd_emd_sub=gpd_emd[need_cols].copy()
# 3) 격자점과 읍면동 공간매칭 다시 수행
grid_emd_join=gpd.sjoin(
    gpd_grid,
    gpd_emd_sub,
    how="left",
    predicate="within"
)
# 4) 중복 매칭 제거
grid_emd_join=(
    grid_emd_join
    .sort_values(["grid_id", "EMD_CD"])
    .drop_duplicates(subset="grid_id", keep="first")
    .reset_index(drop=True)
)
# 5) 확인
print("gpd_grid 격자 수:", len(gpd_grid))
print("grid_emd_join 수:", len(grid_emd_join))
print("grid_id 존재 여부:", "grid_id" in grid_emd_join.columns)
missing_count=grid_emd_join["EMD_CD"].isna().sum()
print("읍면동 매칭 안 된 격자 수:", missing_count)
print("읍면동 매칭 격자 수:", len(grid_emd_join) - missing_count)


# 7. 시간별 읍면동 평균강수량 및 위험단계 산정
#    - shp 저장하지 않고 CSV용 테이블만 생성
# =====================================================
result_table_list = []
log_list = []
for f_path in bin_files:
    fname=os.path.basename(f_path)
    # 파일명에서 tmef 추출
    times=re.findall(r"\d{10}", fname)
    if len(times) == 0:
        raise ValueError(f"파일명에서 tmef를 찾을 수 없습니다: {fname}")
    tmef=times[-1]
    print("처리 중:", tmef, fname)
    # 3시간 누적강수 배열 읽기
    arr=read_3hr_bin(f_path, nx=nx, ny=ny)
    # 1차원으로 변환
    rain_flat=arr.reshape(-1)
    if len(rain_flat) != len(gpd_grid):
        raise ValueError(
            f"격자 개수 불일치: {fname} / "
            f"rain_flat={len(rain_flat)}, gpd_grid={len(gpd_grid)}"
        )
    # grid_id 기준으로 강수량 테이블 생성
    rain_df=pd.DataFrame({
        "grid_id": gpd_grid["grid_id"].values,
        "rain3h": rain_flat
    })
    # 미리 만든 격자-읍면동 매칭표에 강수량 붙이기
    temp=grid_emd_join.merge(
        rain_df,
        on="grid_id",
        how="left"
    )
    # 읍면동에 매칭된 격자만 사용
    temp_valid=temp.dropna(subset=["EMD_CD"]).copy()
    # 읍면동별 평균 3시간 누적강수량
    emd_rain=(
        temp_valid
        .groupby(["EMD_CD", "EMD_NM", "SGG", "sido"], as_index=False)
        .agg(
            rain3h=("rain3h", "mean"),
            cn2a=("cn2a", "first"),
            cn2b=("cn2b", "first"),
            cn2c=("cn2c", "first")
        )
    )
    # 위험단계 부여
    emd_rain["stage"]=emd_rain.apply(classify_stage, axis=1)
    # 시간정보 추가
    emd_rain["tmef"]=tmef
    # 컬럼 순서 정리
    emd_rain=emd_rain[
        [
            "tmef",
            "EMD_CD", "EMD_NM", "SGG", "sido",
            "rain3h",
            "cn2a", "cn2b", "cn2c",
            "stage"
        ]
    ]
    result_table_list.append(emd_rain)
    # 로그
    stage_count=emd_rain["stage"].value_counts(dropna=False).to_dict()
    log_list.append({
        "tmef": tmef,
        "file": fname,
        "rain_min_grid": np.nanmin(arr),
        "rain_max_grid": np.nanmax(arr),
        "rain_mean_grid": np.nanmean(arr),
        "emd_count": len(emd_rain),
        "관심": stage_count.get("관심", 0),
        "주의": stage_count.get("주의", 0),
        "경고": stage_count.get("경고", 0),
        "위험": stage_count.get("위험", 0)
    })
print("시간별 읍면동 CSV 테이블 생성 완료")


# 8. 전체 결과 CSV 저장
# =====================================================
df_emd_stage_all=pd.concat(result_table_list, ignore_index=True)
# cn2a, cn2b, cn2c 결측 행 삭제
before_count=len(df_emd_stage_all)
df_emd_stage_all=df_emd_stage_all.dropna(
    subset=["cn2a", "cn2b", "cn2c"]
).copy()
after_count=len(df_emd_stage_all)
print("삭제 후 행 수:", after_count)
out_csv = 'C:/Users/lhj15/OneDrive/문서/[NH]/강수위험지도/260428 읍면동 AMC2/emd_stage_all_v3.0.csv'
df_emd_stage_all.to_csv(
    out_csv,
    index=False,
    encoding="utf-8-sig"
)
df_log=pd.DataFrame(log_list)
out_log='C:/Users/lhj15/OneDrive/문서/[NH]/강수위험지도/260428 읍면동 AMC2/emd_stage_process_log_v3.0.csv'
df_log.to_csv(
    out_log,
    index=False,
    encoding="utf-8-sig"
)
print("전체 CSV 저장 완료:", out_csv)
print("처리 로그 저장 완료:", out_log)
