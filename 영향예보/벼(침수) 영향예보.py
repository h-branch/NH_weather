import re
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from pyproj import CRS
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm
from matplotlib.patches import Patch


grid_shp = r"C:\Users\lhj15\OneDrive\문서\[NH]\강수위험지도\#강동호\Threshold_Rainfall.shp"
csv_70 = r"C:\Users\lhj15\OneDrive\문서\[NH]\강수위험지도\70영향한계강우.csv"
emd_shp = r"C:\Users\lhj15\OneDrive\문서\[NH]\이상기후대응팀\#map_basic\2. 읍면동\EMD.shp"
save_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\강수위험지도\벼침수위험_작업")
save_dir.mkdir(parents=True, exist_ok=True)


g_grid = gpd.read_file(grid_shp)
g_grid["gid"] = g_grid["gid"].astype(str).str.strip()
g_grid = g_grid[["gid", "geometry"]].copy()


df = pd.read_csv(csv_70, encoding="euc-kr")
df["gid"] = df["gid"].astype(str).str.strip()
# 컬럼명이 숫자 30, 70이면 문자열 컬럼명으로 정리
df = df.rename(columns={
    "30": "R30",
    "70": "R70",
    30: "R30",
    70: "R70"
})
df["R30"] = pd.to_numeric(df["R30"], errors="coerce")
df["R70"] = pd.to_numeric(df["R70"], errors="coerce")


grid_rain = g_grid.merge(df[["gid", "R30", "R70"]], on="gid", how="left")
grid_rain = gpd.GeoDataFrame(grid_rain, geometry="geometry", crs=g_grid.crs)


emd = gpd.read_file(emd_shp)
# CRS 통일
if emd.crs != grid_rain.crs:
    emd = emd.to_crs(grid_rain.crs)
# 필요한 컬럼만 사용
emd_keep = emd[["EMD_CD", "EMD_NM", "SGG", "geometry"]].copy()
emd_keep["EMD_CD"] = emd_keep["EMD_CD"].astype(str).str.strip()
emd_keep["EMD_NM"] = emd_keep["EMD_NM"].astype(str).str.strip()
emd_keep["SGG"] = emd_keep["SGG"].astype(str).str.strip()
emd_keep = emd_keep.rename(columns={
    "SGG": "SGG_NM"
})


grid_point = grid_rain[["gid", "geometry"]].copy()
grid_point["geometry"] = grid_point.geometry.centroid
join_point = gpd.sjoin(grid_point, emd_keep, how="left", predicate="within")
join_point = join_point.drop(columns=["index_right"], errors="ignore")
# gid 중복 방지
join_point = (join_point.sort_values(["gid", "EMD_CD"]).drop_duplicates(subset=["gid"], keep="first").copy())
grid_admin = grid_rain.merge(join_point[["gid", "EMD_CD", "EMD_NM", "SGG_NM"]], on="gid", how="left")
grid_admin = gpd.GeoDataFrame(grid_admin, geometry="geometry", crs=grid_rain.crs)


unmatched = grid_admin[grid_admin["EMD_CD"].isna()].copy()
print("미매칭 격자 수:", len(unmatched))
if len(unmatched) > 0:
    unmatched_grid = unmatched[["gid", "geometry"]].copy()
    inter = gpd.overlay(
        unmatched_grid,
        emd_keep,
        how="intersection"
    )
    print("교차 결과 수:", len(inter))
    if len(inter) > 0:
        inter["inter_area"] = inter.geometry.area
        inter_best = (
            inter
            .sort_values(["gid", "inter_area"], ascending=[True, False])
            .drop_duplicates(subset=["gid"], keep="first")
            [["gid", "EMD_CD", "EMD_NM", "SGG_NM"]]
            .copy()
        )
        grid_admin = grid_admin.merge(
            inter_best,
            on="gid",
            how="left",
            suffixes=("", "_inter")
        )
        for col in ["EMD_CD", "EMD_NM", "SGG_NM"]:
            grid_admin[col] = grid_admin[col].fillna(grid_admin[f"{col}_inter"])
            grid_admin = grid_admin.drop(columns=[f"{col}_inter"])
        grid_admin = gpd.GeoDataFrame(
            grid_admin,
            geometry="geometry",
            crs=grid_rain.crs
        )
print("2차 보완 후 결측:")
print(grid_admin[["EMD_CD", "EMD_NM", "SGG_NM"]].isna().sum())


sido_map = {
    "11": "서울특별시",
    "26": "부산광역시",
    "27": "대구광역시",
    "28": "인천광역시",
    "29": "광주광역시",
    "30": "대전광역시",
    "31": "울산광역시",
    "36": "세종특별자치시",
    "41": "경기도",
    "42": "강원도",
    "43": "충청북도",
    "44": "충청남도",
    "45": "전라북도",
    "46": "전라남도",
    "47": "경상북도",
    "48": "경상남도",
    "50": "제주특별자치도",
    "51": "강원특별자치도",
    "52": "전북특별자치도"
}
grid_admin["EMD_CD"] = grid_admin["EMD_CD"].astype("string")
grid_admin["EMD_NM"] = grid_admin["EMD_NM"].astype("string")
grid_admin["SGG_NM"] = grid_admin["SGG_NM"].astype("string")
grid_admin["SGG_CD"] = grid_admin["EMD_CD"].str[:5]
grid_admin["SIDO_CD"] = grid_admin["EMD_CD"].str[:2]
grid_admin["SIDO_NM"] = grid_admin["SIDO_CD"].map(sido_map)
# gid 중복 제거
grid_admin = (
    grid_admin
    .sort_values(["gid", "EMD_CD"])
    .drop_duplicates(subset=["gid"], keep="first")
    .copy()
)
grid_admin = gpd.GeoDataFrame(
    grid_admin,
    geometry="geometry",
    crs=grid_rain.crs
)
# 유효 격자
grid_admin_valid = grid_admin.dropna(
    subset=["EMD_CD", "R30", "R70"]
).copy()
grid_admin_valid["gid"] = grid_admin_valid["gid"].astype(str).str.strip()
print("====================================")
print("전체 격자 수:", len(grid_admin))
print("유효 격자 수:", len(grid_admin_valid))
print("제외 격자 수:", len(grid_admin) - len(grid_admin_valid))
print("gid 중복 수:", grid_admin["gid"].duplicated().sum())
print("====================================")
print("행정구역 결측:")
print(grid_admin[["EMD_CD", "EMD_NM", "SGG_CD", "SGG_NM", "SIDO_CD", "SIDO_NM"]].isna().sum())
print("====================================")
print("기준강우량 결측:")
print(grid_admin[["R30", "R70"]].isna().sum())
grid_admin_valid.head()


out_gpkg = save_dir / "grid_admin_R30_R70.gpkg"
out_csv = save_dir / "grid_admin_R30_R70.csv"
grid_admin.to_file(
    out_gpkg,
    driver="GPKG"
)
grid_admin.drop(columns="geometry").to_csv(
    out_csv,
    index=False,
    encoding="utf-8-sig"
)
print("저장 완료")
print(out_gpkg)
print(out_csv)


# =====================================================
# 1. 기준격자 고정
# =====================================================
grid_base = grid_admin_valid.copy()
grid_base["gid"] = grid_base["gid"].astype(str).str.strip()
# 필요한 컬럼만 정리
grid_base = grid_base[[
    "gid", "geometry",
    "R30", "R70",
    "EMD_CD", "EMD_NM",
    "SGG_NM", "SGG_CD",
    "SIDO_CD", "SIDO_NM"
]].copy()
print("기준격자 수:", len(grid_base))
print(grid_base.head())


# =====================================================
# 2. 기준시각 설정
# =====================================================
# 자동 설정: 현재 시각 기준
#issue_time = pd.Timestamp.now(tz="Asia/Seoul").floor("h").tz_localize(None)
# 테스트용으로 직접 지정하고 싶으면 아래 사용
issue_time = pd.Timestamp("2025-07-20 09:00")
print("기준시각:", issue_time)


download_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\벼침수위험_작업\api_download")
aws_save_dir = download_dir / "AWS_OBJECTIVE_1H"
aws_files = sorted(aws_save_dir.glob("*.txt"))
print("AWS 파일 수:", len(aws_files))
print("첫 파일:", aws_files[0])
# 파일 앞부분 확인
with open(aws_files[0], "r", encoding="utf-8", errors="replace") as f:
    for i in range(20):
        print(i, f.readline().rstrip())


def read_aws_grid_txt(file_path):
    """
    AWS 객관분석 격자 txt 파일 읽기 수정본.
    헤더/설명문에 포함된 숫자 때문에 값 개수가 초과되는 문제를 피하기 위해,
    데이터로 보이는 숫자 라인을 우선 읽고, 그래도 안 맞으면 마지막 expected개를 사용합니다.
    """
    file_path = Path(file_path)
    text = file_path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    if len(lines) == 0:
        raise ValueError(f"빈 파일입니다: {file_path.name}")
    header_text = "\n".join(lines[:30])
    nx_match = re.search(r"NX\s*=\s*(\d+)", header_text)
    ny_match = re.search(r"NY\s*=\s*(\d+)", header_text)
    if nx_match is None or ny_match is None:
        nx_match = re.search(r"NX\s*=\s*(\d+)", text)
        ny_match = re.search(r"NY\s*=\s*(\d+)", text)
    if nx_match is None or ny_match is None:
        raise ValueError(f"NX, NY를 찾지 못했습니다: {file_path.name}")
    nx = int(nx_match.group(1))
    ny = int(ny_match.group(1))
    expected = nx * ny
    values = []
    for line in lines:
        s = line.strip()
        if s == "":
            continue
        # 영문, 한글, =, :가 있으면 헤더/설명문으로 보고 제외
        if re.search(r"[A-Za-z가-힣=:]", s):
            continue
        nums = re.findall(r"[-+]?\d+(?:\.\d+)?", s)
        if len(nums) > 0:
            values.extend([float(x) for x in nums])
    values = np.array(values, dtype=float)
    if values.size != expected:
        all_nums = re.findall(r"[-+]?\d+(?:\.\d+)?", text)
        all_values = np.array([float(x) for x in all_nums], dtype=float)
        if all_values.size < expected:
            raise ValueError(
                f"값 개수 부족: {file_path.name}, "
                f"읽은 값={all_values.size}, 기대 값={expected}"
            )
        if all_values.size > expected:
            print(
                f"값 개수 초과: {file_path.name}, "
                f"읽은 값={all_values.size}, 기대 값={expected}, "
                f"마지막 {expected}개 사용"
            )
            values = all_values[-expected:]
        else:
            values = all_values
    values[values <= -90] = np.nan
    arr = values.reshape(ny, nx)
    return arr


arr = read_aws_grid_txt(aws_files[0])
print(arr.shape)
print(np.nanmin(arr), np.nanmax(arr))


# =====================================================
# AWS 객관분석 격자와 기준격자 gid 매칭표 생성
# gid_to_aws 생성
# =====================================================
# -----------------------------------------------------
# 1. 기준격자 확인
# -----------------------------------------------------
# grid_base가 있으면 grid_base 사용
# 없으면 grid_admin_valid 사용
if "grid_base" in globals():
    grid_for_match = grid_base.copy()
elif "grid_admin_valid" in globals():
    grid_for_match = grid_admin_valid.copy()
else:
    raise NameError("grid_base 또는 grid_admin_valid가 없습니다. 먼저 기준격자-읍면동 매칭을 완료해야 합니다.")
grid_for_match["gid"] = grid_for_match["gid"].astype(str).str.strip()
print("매칭 대상 기준격자 수:", len(grid_for_match))
print("기준격자 CRS:", grid_for_match.crs)
# -----------------------------------------------------
# 2. AWS 객관분석 격자 정보 설정
# -----------------------------------------------------
# 기상청 문의 회신 기준:
# 투영법: LCC
# 격자수: X=681, Y=681
# 기준격자점: X=154, Y=580
# 기준위경도: N38.0 / E126.0
aws_nx = 681
aws_ny = 681
ref_x = 154
ref_y = 580
aws_res = 1000  # 1km 격자
aws_crs = CRS.from_proj4(
    "+proj=lcc +lat_1=30 +lat_2=60 +lat_0=38 "
    "+lon_0=126 +x_0=0 +y_0=0 "
    "+a=6371008.77 +b=6371008.77 +units=m +no_defs"
)
# -----------------------------------------------------
# 3. AWS 격자점 생성
# -----------------------------------------------------
rows = []
for iy in range(1, aws_ny + 1):
    for ix in range(1, aws_nx + 1):
        x = (ix - ref_x) * aws_res
        y = (iy - ref_y) * aws_res
        rows.append([ix, iy, x, y])
aws_grid = pd.DataFrame(
    rows,
    columns=["aws_x", "aws_y", "x_lcc", "y_lcc"]
)
aws_gdf = gpd.GeoDataFrame(
    aws_grid,
    geometry=gpd.points_from_xy(
        aws_grid["x_lcc"],
        aws_grid["y_lcc"]
    ),
    crs=aws_crs
)
# 기준격자 CRS로 변환
aws_gdf = aws_gdf.to_crs(grid_for_match.crs)
# -----------------------------------------------------
# 4. 기준격자 중심점 생성
# -----------------------------------------------------
grid_center = grid_for_match[["gid", "geometry"]].copy()
grid_center["gid"] = grid_center["gid"].astype(str).str.strip()
grid_center["geometry"] = grid_center.geometry.centroid
# -----------------------------------------------------
# 5. 기준격자 gid ↔ AWS 객관분석 격자 최근접 매칭
# -----------------------------------------------------
gid_to_aws = gpd.sjoin_nearest(
    grid_center,
    aws_gdf[["aws_x", "aws_y", "geometry"]],
    how="left",
    distance_col="dist_aws_m"
)
gid_to_aws = gid_to_aws.drop(columns=["index_right"], errors="ignore")
# 일반 DataFrame으로 변환
gid_to_aws = pd.DataFrame(
    gid_to_aws.drop(columns="geometry")
)
gid_to_aws["gid"] = gid_to_aws["gid"].astype(str).str.strip()
gid_to_aws["aws_x"] = gid_to_aws["aws_x"].astype(int)
gid_to_aws["aws_y"] = gid_to_aws["aws_y"].astype(int)
# -----------------------------------------------------
# 6. 검증
# -----------------------------------------------------
print("====================================")
print("기준격자 수:", len(grid_for_match))
print("gid_to_aws 행 수:", len(gid_to_aws))
print("gid 중복 수:", gid_to_aws["gid"].duplicated().sum())
print("aws_x 결측:", gid_to_aws["aws_x"].isna().sum())
print("aws_y 결측:", gid_to_aws["aws_y"].isna().sum())
print("====================================")
print("AWS 매칭거리 통계")
print(gid_to_aws["dist_aws_m"].describe())
print("====================================")
gid_to_aws.head()


# =====================================================
# gid_to_aws 저장
# =====================================================
gid_to_aws_path = save_dir / "gid_to_aws_grid.csv"
gid_to_aws.to_csv(
    gid_to_aws_path,
    index=False,
    encoding="utf-8-sig"
)
print("gid_to_aws 저장 완료:", gid_to_aws_path)


# =====================================================
# gid_to_aws 불러오기
# =====================================================
gid_to_aws_path = save_dir / "gid_to_aws_grid.csv"
gid_to_aws = pd.read_csv(
    gid_to_aws_path,
    encoding="utf-8-sig"
)
gid_to_aws["gid"] = gid_to_aws["gid"].astype(str).str.strip()
gid_to_aws["aws_x"] = gid_to_aws["aws_x"].astype(int)
gid_to_aws["aws_y"] = gid_to_aws["aws_y"].astype(int)
print("gid_to_aws 행 수:", len(gid_to_aws))
gid_to_aws.head()


# =====================================================
# AWS 파일명 시간 추출 함수
# 반드시 AWS 전체 파일 읽기 전에 실행
# =====================================================
def parse_yyyymmddhhmm_from_name(file_path):
    """
    파일명에서 YYYYMMDDHHMM 형식 시간을 추출합니다.
    예:
    AWS_OBJECTIVE_1H_202507110900.txt
    → 2025-07-11 09:00
    """
    name = Path(file_path).name
    m = re.search(r"(\d{12})", name)
    if m is None:
        raise ValueError(f"파일명에서 YYYYMMDDHHMM 시간을 찾지 못했습니다: {name}")
    return pd.to_datetime(m.group(1), format="%Y%m%d%H%M")


# =====================================================
# AWS 저장 파일 전체 읽기 → aws_hourly 생성
# =====================================================
aws_files = sorted(aws_save_dir.glob("*.txt"))
print("AWS 저장 파일 수:", len(aws_files))
if len(aws_files) == 0:
    raise FileNotFoundError(f"AWS 저장 파일이 없습니다: {aws_save_dir}")
# gid_to_aws 확인
if "gid_to_aws" not in globals():
    raise NameError("gid_to_aws가 없습니다. 먼저 AWS 격자 매칭표를 생성해야 합니다.")
gid_to_aws_use = gid_to_aws[["gid", "aws_x", "aws_y"]].copy()
gid_to_aws_use["gid"] = gid_to_aws_use["gid"].astype(str).str.strip()
gid_to_aws_use["aws_x"] = gid_to_aws_use["aws_x"].astype(int)
gid_to_aws_use["aws_y"] = gid_to_aws_use["aws_y"].astype(int)
aws_hourly_list = []
aws_read_errors = []
for f in aws_files:
    print("AWS 읽는 중:", f.name)
    try:
        tm = parse_yyyymmddhhmm_from_name(f)
        arr = read_aws_grid_txt(f)
        tmp = gid_to_aws_use.copy()
        tmp["rain_1h"] = arr[
            tmp["aws_y"].to_numpy() - 1,
            tmp["aws_x"].to_numpy() - 1
        ]
    except Exception as e:
        print("  → 오류, NaN 처리:", e)
        try:
            tm = parse_yyyymmddhhmm_from_name(f)
        except Exception:
            tm = pd.NaT
        tmp = gid_to_aws_use.copy()
        tmp["rain_1h"] = np.nan
        aws_read_errors.append({
            "file": f.name,
            "time": tm,
            "error": str(e)
        })
    tmp["time"] = tm
    tmp["source"] = "AWS"
    tmp = tmp[["gid", "time", "rain_1h", "source"]]
    aws_hourly_list.append(tmp)
aws_hourly = pd.concat(aws_hourly_list, ignore_index=True)
aws_hourly["gid"] = aws_hourly["gid"].astype(str).str.strip()
aws_hourly["time"] = pd.to_datetime(aws_hourly["time"])
aws_hourly = aws_hourly.sort_values(["gid", "time"]).copy()
print("====================================")
print("aws_hourly 생성 완료")
print("행 수:", len(aws_hourly))
print("시간 수:", aws_hourly["time"].nunique())
print("시간 범위:", aws_hourly["time"].min(), "~", aws_hourly["time"].max())
print("오류 수:", len(aws_read_errors))
print("강우 결측:", aws_hourly["rain_1h"].isna().sum())
print("====================================")
aws_hourly.head()


# =====================================================
# aws_hourly 정상 확인
# =====================================================
print("시간별 행 수 확인:")
print(aws_hourly.groupby("time")["gid"].count().head())
print("\n강우 통계:")
print(aws_hourly["rain_1h"].describe())
print("\n자료 예시:")
display(aws_hourly.head())


# =====================================================
# 동네예보 격자와 기준격자 gid 매칭표 생성
# gid_to_dfs 생성
# =====================================================
# -----------------------------------------------------
# 1. 기준격자 확인
# -----------------------------------------------------
if "grid_base" in globals():
    grid_for_match = grid_base.copy()
elif "grid_admin_valid" in globals():
    grid_for_match = grid_admin_valid.copy()
else:
    raise NameError("grid_base 또는 grid_admin_valid가 없습니다.")
grid_for_match["gid"] = grid_for_match["gid"].astype(str).str.strip()
print("매칭 대상 기준격자 수:", len(grid_for_match))
print("기준격자 CRS:", grid_for_match.crs)
# -----------------------------------------------------
# 2. 동네예보 DFS 격자 정보
# -----------------------------------------------------
dfs_nx = 149
dfs_ny = 253
dfs_res = 5000  # 5km
dfs_crs = CRS.from_proj4(
    "+proj=lcc +lat_1=30 +lat_2=60 +lat_0=38 "
    "+lon_0=126 +x_0=215000 +y_0=680000 "
    "+a=6371008.77 +b=6371008.77 +units=m +no_defs"
)
# -----------------------------------------------------
# 3. 동네예보 격자점 생성
# -----------------------------------------------------
rows = []
for nx in range(1, dfs_nx + 1):
    for ny in range(1, dfs_ny + 1):
        x = nx * dfs_res
        y = ny * dfs_res
        rows.append([nx, ny, x, y])
dfs_grid = pd.DataFrame(
    rows,
    columns=["nx", "ny", "x_m", "y_m"]
)
dfs_gdf = gpd.GeoDataFrame(
    dfs_grid,
    geometry=gpd.points_from_xy(dfs_grid["x_m"], dfs_grid["y_m"]),
    crs=dfs_crs
)
# 기준격자 CRS로 변환
dfs_gdf = dfs_gdf.to_crs(grid_for_match.crs)
# -----------------------------------------------------
# 4. 기준격자 중심점 생성
# -----------------------------------------------------
grid_center = grid_for_match[["gid", "geometry"]].copy()
grid_center["gid"] = grid_center["gid"].astype(str).str.strip()
grid_center["geometry"] = grid_center.geometry.centroid
# -----------------------------------------------------
# 5. gid ↔ DFS nx, ny 최근접 매칭
# -----------------------------------------------------
gid_to_dfs = gpd.sjoin_nearest(
    grid_center,
    dfs_gdf[["nx", "ny", "geometry"]],
    how="left",
    distance_col="dist_dfs_m"
)
gid_to_dfs = gid_to_dfs.drop(columns=["index_right"], errors="ignore")
gid_to_dfs = pd.DataFrame(gid_to_dfs.drop(columns="geometry"))
gid_to_dfs["gid"] = gid_to_dfs["gid"].astype(str).str.strip()
gid_to_dfs["nx"] = gid_to_dfs["nx"].astype(int)
gid_to_dfs["ny"] = gid_to_dfs["ny"].astype(int)
print("====================================")
print("기준격자 수:", len(grid_for_match))
print("gid_to_dfs 행 수:", len(gid_to_dfs))
print("gid 중복 수:", gid_to_dfs["gid"].duplicated().sum())
print("nx 결측:", gid_to_dfs["nx"].isna().sum())
print("ny 결측:", gid_to_dfs["ny"].isna().sum())
print("====================================")
print("동네예보 매칭거리 통계")
print(gid_to_dfs["dist_dfs_m"].describe())
print("====================================")
gid_to_dfs.head()


# =====================================================
# 동네예보 격자와 기준격자 gid 매칭표 생성
# gid_to_dfs 생성
# =====================================================
# -----------------------------------------------------
# 1. 기준격자 확인
# -----------------------------------------------------
if "grid_base" in globals():
    grid_for_match = grid_base.copy()
elif "grid_admin_valid" in globals():
    grid_for_match = grid_admin_valid.copy()
else:
    raise NameError("grid_base 또는 grid_admin_valid가 없습니다.")
grid_for_match["gid"] = grid_for_match["gid"].astype(str).str.strip()
print("매칭 대상 기준격자 수:", len(grid_for_match))
print("기준격자 CRS:", grid_for_match.crs)
# -----------------------------------------------------
# 2. 동네예보 DFS 격자 정보
# -----------------------------------------------------
dfs_nx = 149
dfs_ny = 253
dfs_res = 5000  # 5km
dfs_crs = CRS.from_proj4(
    "+proj=lcc +lat_1=30 +lat_2=60 +lat_0=38 "
    "+lon_0=126 +x_0=215000 +y_0=680000 "
    "+a=6371008.77 +b=6371008.77 +units=m +no_defs"
)
# -----------------------------------------------------
# 3. 동네예보 격자점 생성
# -----------------------------------------------------
rows = []
for nx in range(1, dfs_nx + 1):
    for ny in range(1, dfs_ny + 1):
        x = nx * dfs_res
        y = ny * dfs_res
        rows.append([nx, ny, x, y])
dfs_grid = pd.DataFrame(
    rows,
    columns=["nx", "ny", "x_m", "y_m"]
)
dfs_gdf = gpd.GeoDataFrame(
    dfs_grid,
    geometry=gpd.points_from_xy(dfs_grid["x_m"], dfs_grid["y_m"]),
    crs=dfs_crs
)
# 기준격자 CRS로 변환
dfs_gdf = dfs_gdf.to_crs(grid_for_match.crs)
# -----------------------------------------------------
# 4. 기준격자 중심점 생성
# -----------------------------------------------------
grid_center = grid_for_match[["gid", "geometry"]].copy()
grid_center["gid"] = grid_center["gid"].astype(str).str.strip()
grid_center["geometry"] = grid_center.geometry.centroid
# -----------------------------------------------------
# 5. gid ↔ DFS nx, ny 최근접 매칭
# -----------------------------------------------------
gid_to_dfs = gpd.sjoin_nearest(
    grid_center,
    dfs_gdf[["nx", "ny", "geometry"]],
    how="left",
    distance_col="dist_dfs_m"
)
gid_to_dfs = gid_to_dfs.drop(columns=["index_right"], errors="ignore")
gid_to_dfs = pd.DataFrame(gid_to_dfs.drop(columns="geometry"))
gid_to_dfs["gid"] = gid_to_dfs["gid"].astype(str).str.strip()
gid_to_dfs["nx"] = gid_to_dfs["nx"].astype(int)
gid_to_dfs["ny"] = gid_to_dfs["ny"].astype(int)
print("====================================")
print("기준격자 수:", len(grid_for_match))
print("gid_to_dfs 행 수:", len(gid_to_dfs))
print("gid 중복 수:", gid_to_dfs["gid"].duplicated().sum())
print("nx 결측:", gid_to_dfs["nx"].isna().sum())
print("ny 결측:", gid_to_dfs["ny"].isna().sum())
print("====================================")
print("동네예보 매칭거리 통계")
print(gid_to_dfs["dist_dfs_m"].describe())
print("====================================")
gid_to_dfs.head()


# =====================================================
# gid_to_dfs 저장
# =====================================================
gid_to_dfs_path = save_dir / "gid_to_dfs_grid.csv"
gid_to_dfs.to_csv(
    gid_to_dfs_path,
    index=False,
    encoding="utf-8-sig"
)
print("gid_to_dfs 저장 완료:", gid_to_dfs_path)


# =====================================================
# 동네예보 파일 읽기 함수
# =====================================================
def parse_dfs_tmef_from_name(file_path):
    """
    DFS 파일명에서 tmef 시간 추출
    예: PCP_tmfc202507180800_tmef202507181100.bin
    """
    name = Path(file_path).name
    m = re.search(r"tmef(\d{12})", name)
    if m:
        return pd.to_datetime(m.group(1), format="%Y%m%d%H%M")
    times = re.findall(r"(\d{12})", name)
    if len(times) == 0:
        raise ValueError(f"파일명에서 tmef 시간을 찾지 못했습니다: {name}")
    return pd.to_datetime(times[-1], format="%Y%m%d%H%M")
def read_dfs_grid_file(file_path, nx=149, ny=253):
    """
    동네예보 격자 파일 읽기
    값 개수 = 149 * 253
    """
    file_path = Path(file_path)
    text = file_path.read_text(encoding="utf-8", errors="replace")
    nums = re.findall(r"[-+]?\d+(?:\.\d+)?", text)
    values = np.array([float(x) for x in nums], dtype=float)
    expected = nx * ny
    if values.size != expected:
        raise ValueError(
            f"DFS 값 개수 오류: {file_path.name}, "
            f"읽은 값={values.size}, 기대 값={expected}"
        )
    values[values <= -90] = np.nan
    arr = values.reshape(ny, nx)
    return arr


dfs_save_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\벼침수위험_작업\api_download\DFS_PCP_1H")
print("DFS 저장 폴더:", dfs_save_dir)
print("폴더 존재 여부:", dfs_save_dir.exists())
dfs_files = sorted(list(dfs_save_dir.glob("*.bin")) + list(dfs_save_dir.glob("*.txt")))
print("DFS 파일 수:", len(dfs_files))
print("앞 5개 파일")
for f in dfs_files[:5]:
    print(f.name)
print("뒤 5개 파일")
for f in dfs_files[-5:]:
    print(f.name)


# =====================================================
# DFS 저장 파일 전체 읽기 → dfs_hourly 생성
# =====================================================
dfs_files = sorted(list(dfs_save_dir.glob("*.bin")) + list(dfs_save_dir.glob("*.txt")))
print("DFS 저장 파일 수:", len(dfs_files))
if len(dfs_files) == 0:
    raise FileNotFoundError(f"DFS 저장 파일이 없습니다: {dfs_save_dir}")
if "gid_to_dfs" not in globals():
    raise NameError("gid_to_dfs가 없습니다. 먼저 동네예보 격자 매칭표를 생성해야 합니다.")
gid_to_dfs_use = gid_to_dfs[["gid", "nx", "ny"]].copy()
gid_to_dfs_use["gid"] = gid_to_dfs_use["gid"].astype(str).str.strip()
gid_to_dfs_use["nx"] = gid_to_dfs_use["nx"].astype(int)
gid_to_dfs_use["ny"] = gid_to_dfs_use["ny"].astype(int)
dfs_hourly_list = []
dfs_read_errors = []
for f in dfs_files:
    print("DFS 읽는 중:", f.name)
    try:
        tmef = parse_dfs_tmef_from_name(f)
        arr = read_dfs_grid_file(f, nx=149, ny=253)
        tmp = gid_to_dfs_use.copy()
        tmp["rain_1h"] = arr[
            tmp["ny"].to_numpy() - 1,
            tmp["nx"].to_numpy() - 1
        ]
    except Exception as e:
        print("  → 오류, NaN 처리:", e)
        try:
            tmef = parse_dfs_tmef_from_name(f)
        except Exception:
            tmef = pd.NaT
        tmp = gid_to_dfs_use.copy()
        tmp["rain_1h"] = np.nan
        dfs_read_errors.append({
            "file": f.name,
            "time": tmef,
            "error": str(e)
        })
    tmp["time"] = tmef
    tmp["source"] = "DFS"
    tmp = tmp[["gid", "time", "rain_1h", "source"]]
    dfs_hourly_list.append(tmp)
dfs_hourly = pd.concat(dfs_hourly_list, ignore_index=True)
dfs_hourly["gid"] = dfs_hourly["gid"].astype(str).str.strip()
dfs_hourly["time"] = pd.to_datetime(dfs_hourly["time"])
dfs_hourly = dfs_hourly.sort_values(["gid", "time"]).copy()
print("====================================")
print("dfs_hourly 생성 완료")
print("행 수:", len(dfs_hourly))
print("시간 수:", dfs_hourly["time"].nunique())
print("시간 범위:", dfs_hourly["time"].min(), "~", dfs_hourly["time"].max())
print("오류 수:", len(dfs_read_errors))
print("강우 결측:", dfs_hourly["rain_1h"].isna().sum())
print("====================================")
dfs_hourly.head()


# =====================================================
# dfs_hourly 정상 확인
# =====================================================
print("시간별 행 수:")
print(dfs_hourly.groupby("time")["gid"].count().head())
print("\n강우 통계:")
print(dfs_hourly["rain_1h"].describe())
print("\n자료원:")
print(dfs_hourly["source"].value_counts())
display(dfs_hourly.head())


# =====================================================
# AWS + DFS 1시간 강우 통합
# =====================================================
# 기준시각은 AWS 마지막 시간으로 설정
issue_time = pd.to_datetime(aws_hourly["time"]).max()
print("기준시각:", issue_time)
aws_hourly_use = aws_hourly[["gid", "time", "rain_1h", "source"]].copy()
dfs_hourly_use = dfs_hourly[["gid", "time", "rain_1h", "source"]].copy()
rain_hourly = pd.concat([
    aws_hourly_use[aws_hourly_use["time"] <= issue_time],
    dfs_hourly_use[dfs_hourly_use["time"] > issue_time]
], ignore_index=True)
rain_hourly["gid"] = rain_hourly["gid"].astype(str).str.strip()
rain_hourly["time"] = pd.to_datetime(rain_hourly["time"])
rain_hourly = rain_hourly.sort_values(["gid", "time"]).copy()
print("통합 1시간 강우 행 수:", len(rain_hourly))
print("시간 범위:", rain_hourly["time"].min(), "~", rain_hourly["time"].max())
print(rain_hourly["source"].value_counts())
rain_hourly.head()


# =====================================================
# 3. 3시간 강우량 계산
# rain_3h(t) = rain(t) + rain(t+1) + rain(t+2)
# =====================================================
rain_hourly = rain_hourly.sort_values(["gid", "time"]).copy()
g = rain_hourly.groupby("gid")["rain_1h"]
rain_hourly["rain_t"] = g.shift(0)
rain_hourly["rain_t1"] = g.shift(-1)
rain_hourly["rain_t2"] = g.shift(-2)
rain_hourly["rain_3h"] = (
    rain_hourly["rain_t"] +
    rain_hourly["rain_t1"] +
    rain_hourly["rain_t2"]
)
rain_3h = rain_hourly.copy()
print("3시간 강우 행 수:", len(rain_3h))
print("rain_3h 결측:", rain_3h["rain_3h"].isna().sum())
rain_3h.head()


# =====================================================
# 4. 3시간 강우에 기준강우량 붙이기
# =====================================================
grid_attr = grid_base.drop(columns="geometry").copy()
grid_attr["gid"] = grid_attr["gid"].astype(str).str.strip()
rain_3h = rain_3h.merge(
    grid_attr,
    on="gid",
    how="left"
)
print("R30 결측:", rain_3h["R30"].isna().sum())
print("R70 결측:", rain_3h["R70"].isna().sum())
print("행정구역 결측:", rain_3h["EMD_CD"].isna().sum())
rain_3h.head()


def damage_rate_to_stage(damage_rate):
    """
    피해율 기반 벼 침수위험 등급
    - 20% 미만: 관심
    - 20~50%: 주의
    - 50~70%: 경고
    - 70% 이상: 위험
    """
    if pd.isna(damage_rate):
        return "자료없음"
    elif damage_rate >= 70:
        return "위험"
    elif damage_rate >= 50:
        return "경고"
    elif damage_rate >= 20:
        return "주의"
    else:
        return "관심"
def damage_stage_to_code(stage):
    code_map = {
        "자료없음": -1,
        "관심": 1,
        "주의": 2,
        "경고": 3,
        "위험": 4
    }
    return code_map.get(stage, -1)


# =====================================================
# 1. 3시간 강우자료 선택
# =====================================================
if "rain_3h_all" in globals():
    rain_event = rain_3h_all.copy()
    print("rain_3h_all 사용")
elif "rain_3h" in globals():
    rain_event = rain_3h.copy()
    print("rain_3h 사용")
else:
    raise NameError("rain_3h_all 또는 rain_3h 변수가 없습니다. 먼저 3시간 강우량 자료를 생성하세요.")
rain_event["gid"] = rain_event["gid"].astype(str).str.strip()
rain_event["time"] = pd.to_datetime(rain_event["time"])
print("자료 행 수:", len(rain_event))
print("시간 범위:", rain_event["time"].min(), "~", rain_event["time"].max())
print(rain_event.columns)


# -----------------------------------------------------
# 1. 3시간 강우자료 선택
# -----------------------------------------------------
if "rain_3h_all" in globals():
    rain_event = rain_3h_all.copy()
    print("rain_3h_all 사용")
elif "rain_3h" in globals():
    rain_event = rain_3h.copy()
    print("rain_3h 사용")
else:
    raise NameError("rain_3h_all 또는 rain_3h 변수가 없습니다. 먼저 3시간 강우자료를 생성하세요.")
# -----------------------------------------------------
# 2. 기본 자료형 정리
# -----------------------------------------------------
rain_event["gid"] = rain_event["gid"].astype(str).str.strip()
rain_event["time"] = pd.to_datetime(rain_event["time"], errors="coerce")

if "rain_3h" not in rain_event.columns:
    if "rain_3h_t_t1_t2" in rain_event.columns:
        rain_event = rain_event.rename(columns={"rain_3h_t_t1_t2": "rain_3h"})
    else:
        raise KeyError(f"rain_3h 컬럼이 없습니다. 현재 컬럼: {list(rain_event.columns)}")
# -----------------------------------------------------
# 3. R30, R70이 없으면 grid_base에서 다시 붙이기
# -----------------------------------------------------
if ("R30" not in rain_event.columns) or ("R70" not in rain_event.columns):
    if "grid_base" not in globals():
        raise NameError("R30/R70을 붙이기 위한 grid_base가 없습니다.")
    grid_attr = grid_base.drop(columns="geometry", errors="ignore").copy()
    grid_attr["gid"] = grid_attr["gid"].astype(str).str.strip()
    rain_event = rain_event.merge(
        grid_attr,
        on="gid",
        how="left"
    )
# -----------------------------------------------------
# 4. 숫자형 정리
# -----------------------------------------------------
rain_event["R30"] = pd.to_numeric(rain_event["R30"], errors="coerce")
rain_event["R70"] = pd.to_numeric(rain_event["R70"], errors="coerce")
rain_event["rain_3h"] = pd.to_numeric(rain_event["rain_3h"], errors="coerce")
# -----------------------------------------------------
# 5. R50 선형보간
# -----------------------------------------------------
# R50은 실제 입력자료가 아니므로 R30~R70 사이 선형보간값으로 가정
rain_event["R50"] = rain_event["R30"] + 0.5 * (rain_event["R70"] - rain_event["R30"])
# -----------------------------------------------------
# 6. 월별 기준 침수심 설정
# -----------------------------------------------------
def get_monthly_target_depth(date):
    """
    월별 대표 생육단계에 따라 침수판정 기준 침수심을 설정합니다.
    5~6월: 30cm
    7월  : 50cm
    8~10월: 70cm
    그 외: 70cm
    """
    if pd.isna(date):
        return np.nan
    month = pd.to_datetime(date).month
    if month in [5, 6]:
        return 30
    elif month == 7:
        return 50
    elif month in [8, 9, 10]:
        return 70
    else:
        return 70
def get_monthly_target_name(depth):
    """
    기준 침수심 이름을 반환합니다.
    """
    if pd.isna(depth):
        return "자료없음"
    elif depth == 30:
        return "R30"
    elif depth == 50:
        return "R50"
    elif depth == 70:
        return "R70"
    else:
        return f"R{int(depth)}"
def get_monthly_threshold(row):
    """
    월별 기준 침수심에 해당하는 영향한계강우량을 반환합니다.
    """
    depth = row["target_depth_cm"]
    if pd.isna(depth):
        return np.nan
    elif depth == 30:
        return row["R30"]
    elif depth == 50:
        return row["R50"]
    elif depth == 70:
        return row["R70"]
    else:
        return np.nan
rain_event["target_depth_cm"] = rain_event["time"].apply(get_monthly_target_depth)
rain_event["target_threshold_name"] = rain_event["target_depth_cm"].apply(get_monthly_target_name)
rain_event["R_target"] = rain_event.apply(
    get_monthly_threshold,
    axis=1
)
# -----------------------------------------------------
# 7. 월별 기준 초과 여부
# -----------------------------------------------------
rain_event["over_monthly_threshold"] = (
    rain_event["rain_3h"] >= rain_event["R_target"]
)
# 기존 참고용 컬럼도 유지
rain_event["over_R30"] = rain_event["rain_3h"] >= rain_event["R30"]
rain_event["over_R50"] = rain_event["rain_3h"] >= rain_event["R50"]
rain_event["over_R70"] = rain_event["rain_3h"] >= rain_event["R70"]
# -----------------------------------------------------
# 8. 참고등급
# -----------------------------------------------------
def classify_monthly_ref_stage(row):
    if pd.isna(row["rain_3h"]) or pd.isna(row["R30"]) or pd.isna(row["R70"]):
        return "자료없음"
    elif row["rain_3h"] >= row["R70"]:
        return "R70 이상"
    elif row["rain_3h"] >= row["R50"]:
        return "R50 이상"
    elif row["rain_3h"] >= row["R30"]:
        return "R30 이상"
    else:
        return "기준 미만"
rain_event["threshold_ref_stage"] = rain_event.apply(
    classify_monthly_ref_stage,
    axis=1
)
print("====================================")
print("rain_event 행 수:", len(rain_event))
print("시간 범위:", rain_event["time"].min(), "~", rain_event["time"].max())
print("R30 결측:", rain_event["R30"].isna().sum())
print("R50 결측:", rain_event["R50"].isna().sum())
print("R70 결측:", rain_event["R70"].isna().sum())
print("R_target 결측:", rain_event["R_target"].isna().sum())
print("월별 기준 분포:")
print(rain_event["target_threshold_name"].value_counts(dropna=False))
print("월별 기준 초과 행 수:", rain_event["over_monthly_threshold"].sum())
print("참고 침수심 구간 분포:")
print(rain_event["threshold_ref_stage"].value_counts(dropna=False))
print("====================================")
display(rain_event[[
    "gid", "time", "rain_3h",
    "R30", "R50", "R70",
    "target_depth_cm", "target_threshold_name", "R_target",
    "over_monthly_threshold",
    "over_R30", "over_R50", "over_R70",
    "EMD_NM", "SGG_NM", "SIDO_NM"
]].head())


# =====================================================
# 월별 기준 침수사상 생성 함수
# =====================================================
def make_monthly_threshold_episodes(rain_event, continue_hour=24):
    """
    월별 기준 침수조건 발생 시각을 기준으로 침수사상을 생성합니다.
    기준:
    - over_monthly_threshold == True이면 해당 월 기준 침수조건 발생
    - 5~6월: R30
    - 7월  : R50
    - 8~10월: R70
    - 마지막 발생시각 + continue_hour 이내 다시 발생하면 같은 사상
    - continue_hour 초과 후 다시 발생하면 새로운 사상
    - episode_end는 마지막 발생시각 + continue_hour
    """
    required_cols = [
        "gid", "time", "rain_3h",
        "target_depth_cm",
        "target_threshold_name",
        "R_target",
        "over_monthly_threshold"
    ]
    missing_cols = [c for c in required_cols if c not in rain_event.columns]
    if missing_cols:
        raise KeyError(f"rain_event에 필요한 컬럼이 없습니다: {missing_cols}")
    df = rain_event[required_cols].copy()
    df["gid"] = df["gid"].astype(str).str.strip()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["rain_3h"] = pd.to_numeric(df["rain_3h"], errors="coerce")
    df["R_target"] = pd.to_numeric(df["R_target"], errors="coerce")
    df["target_depth_cm"] = pd.to_numeric(df["target_depth_cm"], errors="coerce")
    df["over_monthly_threshold"] = df["over_monthly_threshold"].fillna(False).astype(bool)
    df = df.dropna(subset=["gid", "time"]).copy()
    df = df.sort_values(["gid", "time"]).copy()
    episodes = []
    for gid, g in df.groupby("gid"):
        ev = g[g["over_monthly_threshold"] == True].copy()
        if len(ev) == 0:
            continue
        start_time = None
        active_until = None
        event_count = 0
        max_rain_3h = np.nan
        min_r_target = np.nan
        max_target_depth_cm = np.nan
        threshold_names = []
        for _, row in ev.iterrows():
            t = row["time"]
            rain = row["rain_3h"]
            r_target = row["R_target"]
            target_depth = row["target_depth_cm"]
            threshold_name = row["target_threshold_name"]
            if start_time is None:
                start_time = t
                active_until = t + pd.Timedelta(hours=continue_hour)
                event_count = 1
                max_rain_3h = rain
                min_r_target = r_target
                max_target_depth_cm = target_depth
                threshold_names = [threshold_name]
            elif t <= active_until:
                active_until = t + pd.Timedelta(hours=continue_hour)
                event_count += 1
                if pd.notna(rain):
                    if pd.isna(max_rain_3h):
                        max_rain_3h = rain
                    else:
                        max_rain_3h = max(max_rain_3h, rain)
                if pd.notna(r_target):
                    if pd.isna(min_r_target):
                        min_r_target = r_target
                    else:
                        min_r_target = min(min_r_target, r_target)
                if pd.notna(target_depth):
                    if pd.isna(max_target_depth_cm):
                        max_target_depth_cm = target_depth
                    else:
                        max_target_depth_cm = max(max_target_depth_cm, target_depth)
                threshold_names.append(threshold_name)
            else:
                duration_hour = (active_until - start_time).total_seconds() / 3600
                episodes.append({
                    "gid": gid,
                    "episode_start": start_time,
                    "episode_end": active_until,
                    "episode_duration_hour": duration_hour,
                    "episode_day": int(np.ceil(duration_hour / 24)),
                    "event_count": event_count,
                    "max_rain_3h": max_rain_3h,
                    "min_R_target": min_r_target,
                    "max_target_depth_cm": max_target_depth_cm,
                    "threshold_name_list": ", ".join(sorted(set(threshold_names)))
                })
                start_time = t
                active_until = t + pd.Timedelta(hours=continue_hour)
                event_count = 1
                max_rain_3h = rain
                min_r_target = r_target
                max_target_depth_cm = target_depth
                threshold_names = [threshold_name]
        # 마지막 사상 저장
        if start_time is not None:
            duration_hour = (active_until - start_time).total_seconds() / 3600
            episodes.append({
                "gid": gid,
                "episode_start": start_time,
                "episode_end": active_until,
                "episode_duration_hour": duration_hour,
                "episode_day": int(np.ceil(duration_hour / 24)),
                "event_count": event_count,
                "max_rain_3h": max_rain_3h,
                "min_R_target": min_r_target,
                "max_target_depth_cm": max_target_depth_cm,
                "threshold_name_list": ", ".join(sorted(set(threshold_names)))
            })
    return pd.DataFrame(episodes)


# =====================================================
# 월별 기준 침수사상 생성 실행
# =====================================================
flood_episodes = make_monthly_threshold_episodes(
    rain_event,
    continue_hour=24
)
print("월별 기준 침수사상 수:", len(flood_episodes))
if len(flood_episodes) > 0:
    print("침수 지속일수 분포:")
    print(flood_episodes["episode_day"].value_counts().sort_index())
    print("\n적용 기준 분포:")
    print(flood_episodes["threshold_name_list"].value_counts(dropna=False))
    display(flood_episodes.head())
else:
    print("월별 기준 침수사상이 없습니다.")


# =====================================================
# 월별 기준 침수사상 결과 사용
# =====================================================
flood_episodes_use = flood_episodes.copy()


# =====================================================
# flood_episodes 이후: 벼 생육시기별 피해율 기준 적용 전 행정구역 붙이기
# =====================================================
if len(flood_episodes_use) > 0:
    admin_cols = [
        "gid",
        "EMD_CD", "EMD_NM",
        "SGG_CD", "SGG_NM",
        "SIDO_CD", "SIDO_NM"
    ]
    admin_base = grid_base[admin_cols].copy()
    admin_base["gid"] = admin_base["gid"].astype(str).str.strip()
    flood_episodes_use["gid"] = flood_episodes_use["gid"].astype(str).str.strip()
    # 기존 행정구역 컬럼이 있으면 제거 후 다시 붙이기
    flood_episodes_use = flood_episodes_use.drop(
        columns=[c for c in admin_cols if c in flood_episodes_use.columns and c != "gid"],
        errors="ignore"
    )
    flood_episodes_use = flood_episodes_use.merge(
        admin_base,
        on="gid",
        how="left"
    )
    print("행정구역 결측:")
    print(flood_episodes_use[["EMD_CD", "EMD_NM", "SGG_NM", "SIDO_NM"]].isna().sum())
else:
    print("월별 기준 침수사상이 없습니다.")


# =====================================================
# 벼 흙탕물 관수 피해율 산정기준
# =====================================================
rice_damage_table = {
    "새끼칠 때": {
        "months": [5, 6],
        "1-2일": 10,
        "3-4일": 30,
        "5-7일": 60,
        "8일이상": 70
    },
    "어린이삭이 생길 때": {
        "months": [7],
        "1-2일": 20,
        "3-4일": 55,
        "5-7일": 90,
        "8일이상": 100
    },
    "이삭이 밸 때": {
        "months": [8],
        "1-2일": 30,
        "3-4일": 70,
        "5-7일": 100,
        "8일이상": 100
    },
    "이삭이 팰 때": {
        "months": [9],
        "1-2일": 25,
        "3-4일": 60,
        "5-7일": 80,
        "8일이상": 100
    },
    "여물 때": {
        "months": [10],
        "1-2일": 20,
        "3-4일": 40,
        "5-7일": 60,
        "8일이상": 80
    }
}
def get_rice_growth_stage_by_month(date):
    """
    침수 발생일의 월을 기준으로 벼 생육시기를 판정합니다.
    """
    if pd.isna(date):
        return "기타"
    date = pd.to_datetime(date)
    month = date.month
    if month in [5, 6]:
        return "새끼칠 때"
    elif month == 7:
        return "어린이삭이 생길 때"
    elif month == 8:
        return "이삭이 밸 때"
    elif month == 9:
        return "이삭이 팰 때"
    elif month == 10:
        return "여물 때"
    else:
        return "기타"
def get_duration_period(days):
    """
    관수 지속일수를 피해율 산정기준 구간으로 변환합니다.
    """
    if pd.isna(days) or days <= 0:
        return "1일미만"
    elif days <= 2:
        return "1-2일"
    elif days <= 4:
        return "3-4일"
    elif days <= 7:
        return "5-7일"
    else:
        return "8일이상"
def get_rice_damage_rate(growth_stage, duration_period):
    """
    생육시기와 관수 지속기간을 이용해 벼 피해율을 반환합니다.
    """
    if duration_period == "1일미만":
        return 0
    if growth_stage not in rice_damage_table:
        return 0
    return rice_damage_table[growth_stage].get(duration_period, 0)
def damage_rate_to_stage(damage_rate):
    """
    피해율 기반 벼 침수위험 등급
    - 20% 미만: 관심
    - 20~50% 미만: 주의
    - 50~70% 미만: 경고
    - 70% 이상: 위험
    """
    if pd.isna(damage_rate):
        return "자료없음"
    elif damage_rate >= 70:
        return "위험"
    elif damage_rate >= 50:
        return "경고"
    elif damage_rate >= 20:
        return "주의"
    else:
        return "관심"
def damage_stage_to_code(stage):
    """
    피해등급을 코드로 변환합니다.
    """
    code_map = {
        "자료없음": 0,
        "관심": 1,
        "주의": 2,
        "경고": 3,
        "위험": 4
    }
    return code_map.get(stage, 0)
print("벼 피해율 산정 함수 준비 완료")


# =====================================================
# 침수사상별 피해율 및 등급 산정
# =====================================================
episode_damage = flood_episodes_use.copy()
if len(episode_damage) == 0:
    print("침수사상이 없어 피해율 산정 대상이 없습니다.")
else:
    episode_damage["episode_start"] = pd.to_datetime(episode_damage["episode_start"])
    episode_damage["episode_end"] = pd.to_datetime(episode_damage["episode_end"])
    # 1) 침수 발생 월 → 벼 생육시기
    episode_damage["growth_stage"] = episode_damage["episode_start"].apply(
        get_rice_growth_stage_by_month
    )
    # 2) 침수 지속일수 → 피해율 기준 구간
    episode_damage["duration_period"] = episode_damage["episode_day"].apply(
        get_duration_period
    )
    # 3) 생육시기 + 지속기간 → 피해율
    episode_damage["damage_rate"] = episode_damage.apply(
        lambda row: get_rice_damage_rate(
            row["growth_stage"],
            row["duration_period"]
        ),
        axis=1
    )
    # 4) 피해율 → 기본 등급
    episode_damage["damage_stage"] = episode_damage["damage_rate"].apply(
        damage_rate_to_stage
    )
    episode_damage["damage_stage_code"] = episode_damage["damage_stage"].apply(
        damage_stage_to_code
    )
    print("피해율 기본 등급 분포:")
    print(episode_damage["damage_stage"].value_counts(dropna=False))


# =====================================================
# R50 단발 오경보 완화 보정
# - R70 포함 사상: 기존 등급 유지
# - R50 단발 사상: 1단계 낮춤
# - R50 반복 사상: 기존 등급 유지
# - R30 단발 사상: 관심으로 제한
# =====================================================
code_to_stage = {
    0: "정상/미발생",
    1: "관심",
    2: "주의",
    3: "경고",
    4: "위험"
}
def adjust_damage_stage_code(row):
    """
    월별 기준 적용 후 최종 등급 보정
    목적:
    - R50 기준은 탐지율은 높지만 오경보가 증가할 수 있음
    - 따라서 R50 단발 초과는 1단계 낮춤
    - R50 반복 또는 R70 포함 사상은 위험도를 유지
    """
    base_code = row["damage_stage_code"]
    event_count = row.get("event_count", 0)
    threshold_name = str(row.get("threshold_name_list", ""))
    # 자료없음 또는 정상
    if pd.isna(base_code):
        return 0
    base_code = int(base_code)
    # R70 포함 사상은 고수심 침수조건이므로 기존 등급 유지
    if "R70" in threshold_name:
        return base_code
    # R50 반복 발생은 기존 등급 유지
    if "R50" in threshold_name and event_count >= 2:
        return base_code
    # R50 단발 발생은 오경보 가능성이 있어 1단계 낮춤
    if "R50" in threshold_name and event_count < 2:
        return max(base_code - 1, 1)
    # R30 단발은 관심으로 제한
    if "R30" in threshold_name and event_count < 2:
        return 1
    # R30 반복은 최대 주의까지만 허용
    if "R30" in threshold_name and event_count >= 2:
        return min(base_code, 2)
    return base_code
episode_damage["damage_stage_code_final"] = episode_damage.apply(
    adjust_damage_stage_code,
    axis=1
)
episode_damage["damage_stage_final"] = episode_damage["damage_stage_code_final"].map(
    code_to_stage
)
print("보정 전 등급 분포:")
print(episode_damage["damage_stage"].value_counts(dropna=False))
print("\n보정 후 최종 등급 분포:")
print(episode_damage["damage_stage_final"].value_counts(dropna=False))
display(episode_damage[[
    "gid",
    "episode_start",
    "episode_end",
    "episode_day",
    "event_count",
    "max_rain_3h",
    "min_R_target",
    "max_target_depth_cm",
    "threshold_name_list",
    "growth_stage",
    "duration_period",
    "damage_rate",
    "damage_stage",
    "damage_stage_code",
    "damage_stage_final",
    "damage_stage_code_final",
    "EMD_NM",
    "SGG_NM",
    "SIDO_NM"
]].head())


# =====================================================
# episode_damage → 읍면동별 최종 피해등급 집계
# =====================================================
if len(episode_damage) == 0:
    emd_damage = pd.DataFrame(columns=[
        "SIDO_CD", "SIDO_NM",
        "SGG_CD", "SGG_NM",
        "EMD_CD", "EMD_NM",
        "max_damage_stage_code",
        "max_damage_rate",
        "max_episode_day",
        "max_rain_3h",
        "affected_grid_count",
        "threshold_name_list",
        "growth_stage_list",
        "duration_period_list",
        "max_damage_stage"
    ])
else:
    df_ep = episode_damage.copy()
    df_ep["EMD_CD"] = df_ep["EMD_CD"].astype(str).str.strip()
    df_ep["damage_stage_code_final"] = pd.to_numeric(
        df_ep["damage_stage_code_final"],
        errors="coerce"
    )
    emd_damage = (
        df_ep
        .groupby(
            ["SIDO_CD", "SIDO_NM", "SGG_CD", "SGG_NM", "EMD_CD", "EMD_NM"],
            as_index=False
        )
        .agg(
            max_damage_stage_code=("damage_stage_code_final", "max"),
            max_damage_rate=("damage_rate", "max"),
            max_episode_day=("episode_day", "max"),
            max_rain_3h=("max_rain_3h", "max"),
            affected_grid_count=("gid", "count"),
            threshold_name_list=("threshold_name_list", lambda x: ", ".join(sorted(set(x.dropna())))),
            growth_stage_list=("growth_stage", lambda x: ", ".join(sorted(set(x.dropna())))),
            duration_period_list=("duration_period", lambda x: ", ".join(sorted(set(x.dropna()))))
        )
    )
    emd_damage["max_damage_stage"] = (
        emd_damage["max_damage_stage_code"].map(code_to_stage)
    )
print("읍면동별 최종 피해등급 분포:")
print(emd_damage["max_damage_stage"].value_counts(dropna=False))
display(emd_damage.head())


# =====================================================
# 읍면동 shp에 피해등급 붙이기
# =====================================================
if "emd_keep" in globals():
    emd_plot = emd_keep.copy()
else:
    emd_plot = gpd.read_file(emd_shp)
    if "SGG" in emd_plot.columns and "SGG_NM" not in emd_plot.columns:
        emd_plot = emd_plot.rename(columns={"SGG": "SGG_NM"})
    emd_plot = emd_plot[["EMD_CD", "EMD_NM", "SGG_NM", "geometry"]].copy()
emd_plot["EMD_CD"] = emd_plot["EMD_CD"].astype(str).str.strip()
# 시군구코드, 시도코드 보정
if "SGG_CD" not in emd_plot.columns:
    emd_plot["SGG_CD"] = emd_plot["EMD_CD"].str[:5]
if "SIDO_CD" not in emd_plot.columns:
    emd_plot["SIDO_CD"] = emd_plot["EMD_CD"].str[:2]
emd_damage["EMD_CD"] = emd_damage["EMD_CD"].astype(str).str.strip()
emd_map = emd_plot.merge(
    emd_damage[[
        "EMD_CD",
        "max_damage_stage_code",
        "max_damage_stage",
        "max_damage_rate",
        "max_episode_day",
        "max_rain_3h",
        "affected_grid_count"
    ]],
    on="EMD_CD",
    how="left"
)
# 침수사상이 없는 읍면동은 정상/미발생으로 처리
emd_map["max_damage_stage_code"] = emd_map["max_damage_stage_code"].fillna(0).astype(int)
emd_map["max_damage_stage"] = emd_map["max_damage_stage"].fillna("정상/미발생")
emd_map["max_damage_rate"] = emd_map["max_damage_rate"].fillna(0)
emd_map["max_episode_day"] = emd_map["max_episode_day"].fillna(0).astype(int)
emd_map["max_rain_3h"] = emd_map["max_rain_3h"].fillna(0)
emd_map["affected_grid_count"] = emd_map["affected_grid_count"].fillna(0).astype(int)
if "threshold_name_list" in emd_map.columns:
    emd_map["threshold_name_list"] = emd_map["threshold_name_list"].fillna("")
if "growth_stage_list" in emd_map.columns:
    emd_map["growth_stage_list"] = emd_map["growth_stage_list"].fillna("")
if "duration_period_list" in emd_map.columns:
    emd_map["duration_period_list"] = emd_map["duration_period_list"].fillna("")
emd_map = gpd.GeoDataFrame(
    emd_map,
    geometry="geometry",
    crs=emd_plot.crs
)
print("지도용 읍면동 개수:", len(emd_map))
print(emd_map["max_damage_stage"].value_counts(dropna=False))


# =====================================================
# 읍면동 피해등급 지도
# + 시군구 경계
# + 주의 이상 시군구명 표시
# =====================================================
plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False
# -----------------------------------------------------
# 색상 설정
# -----------------------------------------------------
colors = [
    "#F2F2F2",  # 자료없음
    "#00B050",  # 관심
    "#FFFF00",  # 주의
    "#FFA500",  # 경고
    "#FF0000"   # 위험
]
cmap = ListedColormap(colors)
bounds = [-0.5, 0.5, 1.5, 2.5, 3.5, 4.5]
norm = BoundaryNorm(bounds, cmap.N)
legend_elements = [
    Patch(facecolor="#F2F2F2", edgecolor="gray", label="정상/미발생"),
    Patch(facecolor="#00B050", edgecolor="black", label="관심"),
    Patch(facecolor="#FFFF00", edgecolor="black", label="주의"),
    Patch(facecolor="#FFA500", edgecolor="black", label="경고"),
    Patch(facecolor="#FF0000", edgecolor="black", label="위험"),
]
# -----------------------------------------------------
# 시군구 경계 생성
# -----------------------------------------------------
emd_map["SGG_CD"] = emd_map["EMD_CD"].str[:5]
if "SGG_NM" not in emd_map.columns:
    if "SGG" in emd_map.columns:
        emd_map = emd_map.rename(columns={"SGG": "SGG_NM"})
    else:
        emd_map["SGG_NM"] = emd_map["SGG_CD"]
sgg_boundary = emd_map.dissolve(
    by=["SGG_CD", "SGG_NM"],
    as_index=False
)
# -----------------------------------------------------
# 주의 이상 시군구 라벨 생성
# -----------------------------------------------------
sgg_label = (
    emd_map
    .groupby(["SGG_CD", "SGG_NM"], as_index=False)
    .agg(
        max_damage_stage_code=("max_damage_stage_code", "max")
    )
)
sgg_label = sgg_label[sgg_label["max_damage_stage_code"] >= 2].copy()
sgg_label_gdf = sgg_boundary.merge(
    sgg_label,
    on=["SGG_CD", "SGG_NM"],
    how="inner"
)
sgg_label_gdf["label_point"] = sgg_label_gdf.geometry.representative_point()
print("주의 이상 시군구 수:", len(sgg_label_gdf))
# -----------------------------------------------------
# 지도 그리기
# -----------------------------------------------------
fig, ax = plt.subplots(figsize=(11, 13))
emd_map.plot(
    column="max_damage_stage_code",
    cmap=cmap,
    norm=norm,
    linewidth=0.05,
    edgecolor="lightgray",
    ax=ax
)
sgg_boundary.boundary.plot(
    ax=ax,
    linewidth=0.8,
    edgecolor="black"
)
for _, row in sgg_label_gdf.iterrows():
    x = row["label_point"].x
    y = row["label_point"].y
    ax.text(
        x,
        y,
        row["SGG_NM"],
        fontsize=8,
        fontweight="bold",
        ha="center",
        va="center",
        color="black",
        bbox={
            "boxstyle": "round,pad=0.2",
            "facecolor": "white",
            "edgecolor": "none",
            "alpha": 0.75
        }
    )
ax.set_title(
    "읍면동별 벼 침수위험 피해등급",
    fontsize=18,
    fontweight="bold"
)
ax.set_axis_off()
ax.legend(
    handles=legend_elements,
    title="피해등급",
    loc="lower left",
    fontsize=11,
    title_fontsize=12,
    frameon=True
)
# =====================================================
# 지도 제목용 생육시기 문구 생성
# =====================================================
growth_stage_text = ", ".join(
    sorted(episode_damage["growth_stage"].dropna().unique())
)
duration_text = ", ".join(
    sorted(episode_damage["duration_period"].dropna().unique())
)
print("지도 적용 생육시기:", growth_stage_text)
print("지도 적용 지속기간:", duration_text)
plt.tight_layout()
plt.show()


# =====================================================
# 주의 이상 시군구 리스트
# 최종 보정 등급 기준
# =====================================================
sgg_damage_list = (
    episode_damage
    .groupby(["SIDO_CD", "SIDO_NM", "SGG_CD", "SGG_NM"], as_index=False)
    .agg(
        max_damage_stage_code=("damage_stage_code_final", "max"),
        max_damage_rate=("damage_rate", "max"),
        max_episode_day=("episode_day", "max"),
        max_rain_3h=("max_rain_3h", "max"),
        affected_grid_count=("gid", "count"),
        first_episode_start=("episode_start", "min"),
        last_episode_end=("episode_end", "max"),
        threshold_name_list=("threshold_name_list", lambda x: ", ".join(sorted(set(x.dropna())))),
        growth_stage_list=("growth_stage", lambda x: ", ".join(sorted(set(x.dropna())))),
        duration_period_list=("duration_period", lambda x: ", ".join(sorted(set(x.dropna()))))
    )
)
sgg_damage_list["max_damage_stage"] = (
    sgg_damage_list["max_damage_stage_code"].map(code_to_stage)
)
# 주의 이상만 추출
sgg_damage_alert = (
    sgg_damage_list
    [sgg_damage_list["max_damage_stage_code"] >= 2]
    .sort_values(
        ["max_damage_stage_code", "max_damage_rate", "affected_grid_count"],
        ascending=[False, False, False]
    )
    .copy()
)
print("주의 이상 시군구 수:", len(sgg_damage_alert))
display(sgg_damage_alert)


out_sgg_list = save_dir / "벼침수위험_주의이상_시군구리스트_250720.csv"
sgg_damage_alert.to_csv(
    out_sgg_list,
    index=False,
    encoding="utf-8-sig"
)
print("시군구 리스트 저장:", out_sgg_list)
