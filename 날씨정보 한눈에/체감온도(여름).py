import os
import re
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import geopandas as gpd
import requests
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.patheffects as pe
from matplotlib.colors import ListedColormap, BoundaryNorm
from pyproj import CRS, Transformer
from shapely.geometry import box

mpl.rcParams["font.family"] = "Malgun Gothic"
mpl.rcParams["axes.unicode_minus"] = False

Key = "R3WTydcASuG1k8nXABrhgA"
short_url = "https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-dfs_shrt_grd?"


# 발표시간 설정
tmfc_dt = datetime(2026, 5, 18, 8)
# 5/17 23시까지
end_dt = datetime(2026, 5, 19, 23)
# 시간 목록
tmef_list = pd.date_range(start=tmfc_dt, end=end_dt, freq="1h")
print("시작:", tmef_list[0])
print("끝:", tmef_list[-1])
print("개수:", len(tmef_list))


# 경로 설정
base_dir = "C:/Users/lhj15/OneDrive/문서/[NH]/체감온도지도/260518"
raw_dir = os.path.join(base_dir, "01_raw")
map_dir = os.path.join(base_dir, "02_map")
csv_dir = os.path.join(base_dir, "03_csv")
os.makedirs(raw_dir, exist_ok=True)
os.makedirs(map_dir, exist_ok=True)
os.makedirs(csv_dir, exist_ok=True)
# shp 경로
sido_path = "C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/#map_basic/ramp_gis/sido_ramp.shp"
emd_path = "C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/#map_basic/2. 읍면동/EMD.shp"
sgg_path = "C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/#map_basic/1. 시군구/SGG.shp"


# =====================================================
# 1. 여름철 체감온도 계산 함수
# =====================================================
def calc_wet_bulb_stull(Ta, RH):
    """
    Stull 습구온도 추정식

    Parameters
    ----------
    Ta : array-like
        기온, ℃
    RH : array-like
        상대습도, %

    Returns
    -------
    Tw : array-like
        습구온도, ℃
    """
    Ta = np.asarray(Ta, dtype=float)
    RH = np.asarray(RH, dtype=float)
    Tw = (
        Ta * np.arctan(0.151977 * np.sqrt(RH + 8.313659))
        + np.arctan(Ta + RH)
        - np.arctan(RH - 1.67633)
        + 0.00391838 * RH ** 1.5 * np.arctan(0.023101 * RH)
        - 4.686035
    )
    return Tw

def calc_summer_heat_index(Ta, RH):
    """
    기상청 여름철 체감온도 계산식

    Parameters
    ----------
    Ta : array-like
        기온, ℃
    RH : array-like
        상대습도, %

    Returns
    -------
    heat : array-like
        여름철 체감온도, ℃
    """
    Tw = calc_wet_bulb_stull(Ta, RH)
    heat = (
        -0.2442
        + 0.55399 * Tw
        + 0.45535 * Ta
        - 0.0022 * Tw ** 2
        + 0.00278 * Tw * Ta
        + 3.0
    )
    return heat


# =====================================================
# 2. 동네예보 bin 파일 읽기 함수
# =====================================================
def read_kma_grid_bin(path, nx=149, ny=253):
    """
    동네예보 149 x 253 격자 bin 파일 읽기
    """
    n_expected = nx * ny
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()
    parts = text.replace("\n", ",").replace("\r", ",").split(",")
    parts = [x.strip() for x in parts]
    parts = [x for x in parts if x != ""]
    values = np.array([float(x) for x in parts], dtype=float)
    if values.size != n_expected:
        raise ValueError(
            f"격자 개수 불일치: {os.path.basename(path)} / "
            f"읽은 값 개수={values.size}, 기대 값 개수={n_expected}"
        )
    arr = values.reshape(ny, nx)
    # 결측 처리
    arr[arr == -99.0] = np.nan
    arr[arr <= -90.0] = np.nan
    return arr


# =====================================================
# 3. TMP, REH 자동 다운로드
# =====================================================
download_log = []
for tmef_dt in tmef_list:
    tmfc = tmfc_dt.strftime("%Y%m%d%H")
    tmef = tmef_dt.strftime("%Y%m%d%H")
    for var in ["TMP", "REH"]:
        option = {
            "tmfc": tmfc,
            "tmef": tmef,
            "vars": var,
            "authKey": Key
        }
        response = requests.get(short_url, params=option)
        out_file = os.path.join(
            raw_dir,
            f"{var}_tmfc{tmfc}_tmef{tmef}.bin"
        )
        with open(out_file, "wb") as f:
            f.write(response.content)
        download_log.append({
            "var": var,
            "tmfc": tmfc,
            "tmef": tmef,
            "status_code": response.status_code,
            "file": out_file
        })
        print(f"다운로드 완료: {var} {tmef} / status={response.status_code}")
df_download_log = pd.DataFrame(download_log)
df_download_log.to_csv(
    os.path.join(csv_dir, "download_log_TMP_REH.csv"),
    index=False,
    encoding="utf-8-sig"
)
print("전체 다운로드 완료")


nx, ny = 149, 253
n_expected = nx * ny
cell_size = 5000

# =====================================================
# 4. KMA 동네예보 격자 생성
# =====================================================
kma_lcc = CRS.from_proj4(
    "+proj=lcc +lat_1=30 +lat_2=60 "
    "+lat_0=38 +lon_0=126 "
    "+x_0=215000 +y_0=680000 "
    "+a=6371008.77 +b=6371008.77 "
    "+units=m +no_defs"
)
wgs84 = CRS.from_epsg(4326)
transformer = Transformer.from_crs(
    kma_lcc,
    wgs84,
    always_xy=True
)
# 1-based 격자 번호
xg = np.arange(1, nx + 1)
yg = np.arange(1, ny + 1)
# 격자 중심 좌표
x_center = xg * cell_size
y_center = yg * cell_size
Xc, Yc = np.meshgrid(x_center, y_center)
NX, NY = np.meshgrid(xg, yg)
lon, lat = transformer.transform(Xc, Yc)
df_grid_base = pd.DataFrame({
    "nx": NX.ravel(),
    "ny": NY.ravel(),
    "x_m": Xc.ravel(),
    "y_m": Yc.ravel(),
    "lon": lon.ravel(),
    "lat": lat.ravel()
})
df_grid_base["grid_id"] = df_grid_base.index
print(df_grid_base.head())
print("격자 수:", len(df_grid_base))


# =====================================================
# 5. 행정구역 shp 불러오기
# =====================================================
gpd_sido = gpd.read_file(sido_path)
gpd_emd = gpd.read_file(emd_path)
gpd_sgg = gpd.read_file(sgg_path)
# 시도명 부여
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
    "50": "제주도"
}
gpd_emd["EMD_CD"] = gpd_emd["EMD_CD"].astype(str)
gpd_emd["sd_cd"] = gpd_emd["EMD_CD"].str[:2]
gpd_emd["sido"] = gpd_emd["sd_cd"].map(sido_map)
# 시군구 shp도 시도코드 부여
if "CODE" in gpd_sgg.columns:
    gpd_sgg["sd_cd"] = gpd_sgg["CODE"].astype(str).str[:2]
    gpd_sgg["sido"] = gpd_sgg["sd_cd"].map(sido_map)
# 좌표계 통일
if gpd_sido.crs != gpd_emd.crs:
    gpd_emd = gpd_emd.to_crs(gpd_sido.crs)
if gpd_sgg.crs != gpd_sido.crs:
    gpd_sgg = gpd_sgg.to_crs(gpd_sido.crs)
print(gpd_emd[["EMD_CD", "EMD_NM", "SGG", "sido"]].head())


# =====================================================
# 6. 격자-읍면동 공간매칭표 생성
# =====================================================
# 읍면동을 KMA LCC로 변환
if gpd_emd.crs != kma_lcc:
    gpd_emd_proj = gpd_emd.to_crs(kma_lcc)
else:
    gpd_emd_proj = gpd_emd.copy()
# 5km 격자 폴리곤 생성
gdf_cell = gpd.GeoDataFrame(
    df_grid_base.copy(),
    geometry=[
        box(
            x - cell_size / 2,
            y - cell_size / 2,
            x + cell_size / 2,
            y + cell_size / 2
        )
        for x, y in zip(df_grid_base["x_m"], df_grid_base["y_m"])
    ],
    crs=kma_lcc
)
# 격자와 읍면동 매칭
match_cols = ["EMD_CD", "EMD_NM", "SGG", "sd_cd", "sido", "geometry"]
gdf_match = gpd.sjoin(
    gdf_cell,
    gpd_emd_proj[match_cols],
    how="left",
    predicate="intersects"
)
gdf_match_valid = gdf_match[gdf_match["EMD_CD"].notna()].copy()
print("전체 격자-읍면동 매칭 수:", len(gdf_match))
print("유효 매칭 수:", len(gdf_match_valid))
print(gdf_match_valid[["grid_id", "EMD_CD", "EMD_NM", "SGG"]].head())


temp_color=['#da87ff', '#c23eff', '#ad07ff', '#9200e4', '#7f00bf',
            '#b5b6df', '#8081c7', '#4c4eb1', '#1f219d', '#000386',
            '#87d9ff', '#47c3ff', '#07abff', '#008dde', '#007777',
            '#69fc69', '#1ef31e', '#00d500', '#00a400', '#00da00',
            '#ffea6e', '#ffdc1f', '#f9cd00', '#e0b900', '#ccaa00',
            '#fa8585', '#f63e3e', '#ee0b0b', '#d60b0b', '#bf0000'
            ]
colormap_temp=ListedColormap(temp_color).with_extremes(over='#333333', under='#ffffff')
colormap_temp.set_bad([0,0,0,0])
bounds_t=np.array([
    10., 11., 12., 13., 14., 15., 
    16., 17., 18., 19., 20.,
    21., 22., 23., 24., 25., 
    26., 27., 28., 29., 30.,
    31., 32., 33., 34., 35.,
    36., 37., 38., 39., 40. 
])
norm_t=BoundaryNorm(boundaries=bounds_t, ncolors=len(colormap_temp.colors))
colormap_temp
ticks_t=bounds_t[:]


# =====================================================
# 8. 시간별 체감온도 계산 + 지도 자동 저장
# =====================================================
result_list = []
process_log = []
tmfc = tmfc_dt.strftime("%Y%m%d%H")
for tmef_dt in tmef_list:
    tmef = tmef_dt.strftime("%Y%m%d%H")
    tmp_file = os.path.join(
        raw_dir,
        f"TMP_tmfc{tmfc}_tmef{tmef}.bin"
    )
    reh_file = os.path.join(
        raw_dir,
        f"REH_tmfc{tmfc}_tmef{tmef}.bin"
    )
    if not os.path.exists(tmp_file):
        print("TMP 파일 없음:", tmp_file)
        continue
    if not os.path.exists(reh_file):
        print("REH 파일 없음:", reh_file)
        continue
    print("처리 중:", tmef)
    # 1) TMP, REH 읽기
    arr_tmp = read_kma_grid_bin(tmp_file, nx=nx, ny=ny)
    arr_reh = read_kma_grid_bin(reh_file, nx=nx, ny=ny)
    # 2) 체감온도 계산
    arr_heat = calc_summer_heat_index(arr_tmp, arr_reh)
    # 3) 격자 테이블 생성
    df_grid = df_grid_base.copy()
    df_grid["tmp"] = arr_tmp.ravel()
    df_grid["reh"] = arr_reh.ravel()
    df_grid["heat"] = arr_heat.ravel()
    # 4) 격자-읍면동 매칭표에 체감온도 붙이기
    temp = gdf_match_valid.merge(
        df_grid[["grid_id", "tmp", "reh", "heat"]],
        on="grid_id",
        how="left"
    )
    # 5) 읍면동별 체감온도 집계
    emd_heat = (
        temp
        .groupby(["EMD_CD", "EMD_NM", "SGG", "sido"], as_index=False)
        .agg(
            tmp_mean=("tmp", "mean"),
            tmp_max=("tmp", "max"),
            reh_mean=("reh", "mean"),
            heat_mean=("heat", "mean"),
            heat_max=("heat", "max")
        )
    )
    # 7) 시간정보 추가
    emd_heat["tmfc"] = tmfc
    emd_heat["tmef"] = tmef
    emd_heat = emd_heat[
        [
            "tmfc", "tmef",
            "EMD_CD", "EMD_NM", "SGG", "sido",
            "tmp_mean", "tmp_max",
            "reh_mean",
            "heat_mean", "heat_max"
        ]
    ]
    result_list.append(emd_heat)
    # 8) 읍면동 shp에 병합
    gpd_emd_heat = gpd_emd.merge(
        emd_heat[["EMD_CD", "heat_mean", "heat_max"]],
        on="EMD_CD",
        how="left"
    )
    # 9) 지도 저장
    fig, ax = plt.subplots(figsize=(9, 9))
    gpd_emd_heat.plot(
        ax=ax,
        column="heat_max",
        cmap=colormap_temp,
        norm=norm_t,
        linewidth=0.1,
        edgecolor="gray",
        legend=False
    )
    gpd_sido.boundary.plot(
        ax=ax,
        color="black",
        linewidth=0.8
    )
    sm = mpl.cm.ScalarMappable(
        cmap=colormap_temp,
        norm=norm_t
    )
    sm.set_array([])
    cbar = fig.colorbar(
        sm,
        ax=ax,
        fraction=0.035,
        pad=0.02
    )
    cbar.set_ticks(bounds_t)
    cbar.set_ticklabels([str(int(v)) for v in bounds_t])
    #cbar.set_label("여름철 체감온도(℃)")
    title_time = pd.to_datetime(tmef, format="%Y%m%d%H").strftime("%Y-%m-%d %H시")
    ax.set_title(
        f"읍면동별 여름철 최대 체감온도\n발효시각: {title_time}",
        fontsize=14
    )
    ax.set_axis_off()
    out_png = os.path.join(
        map_dir,
        f"heat_sens_max_tmef{tmef}.png"
    )
    plt.savefig(
        out_png,
        dpi=200,
        bbox_inches="tight"
    )
    plt.close()
    # 10) 로그
    process_log.append({
        "tmfc": tmfc,
        "tmef": tmef,
        "tmp_min_grid": np.nanmin(arr_tmp),
        "tmp_max_grid": np.nanmax(arr_tmp),
        "reh_min_grid": np.nanmin(arr_reh),
        "reh_max_grid": np.nanmax(arr_reh),
        "heat_min_grid": np.nanmin(arr_heat),
        "heat_max_grid": np.nanmax(arr_heat),
        "emd_count": len(emd_heat),
        "map_file": out_png
    })
    print("지도 저장 완료:", out_png)
print("전체 시간 처리 완료")
