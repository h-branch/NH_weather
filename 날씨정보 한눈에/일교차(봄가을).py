import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import requests
import pyproj
from matplotlib.colors import ListedColormap, BoundaryNorm, Normalize
from pyproj import CRS, Transformer
from mpl_toolkits.axes_grid1 import make_axes_locatable
import matplotlib.patheffects as pe
from shapely.geometry import box
import matplotlib as mpl

mpl.rcParams['font.family'] = 'Malgun Gothic'
mpl.rcParams['axes.unicode_minus'] = False


Key='R3WTydcASuG1k8nXABrhgA'
short_url='https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-dfs_shrt_grd?'

option1={
    'tmfc':'2026042908', # 발표시간(생산)
    'tmef':'2026050206', # 발효시간(미래)
    'vars':'TMN',
    'authKey':Key
}
response1=requests.get(short_url, option1)
tmn='C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/260422 최저기온 및 일교차/ex/n2026050206.bin'
with open(tmn, 'wb') as f1:
    f1.write(response1.content)
  
option2={
    'tmfc':'2026042908', # 발표시간(생산)
    'tmef':'2026050214', # 발효시간(미래)
    'vars':'TMX',
    'authKey':Key
}
response2=requests.get(short_url, option2)
tmx='C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/260422 최저기온 및 일교차/ex/x2026050214.bin'
with open(tmx, 'wb') as f2:
    f2.write(response2.content)


nx, ny = 149, 253
n_expected = nx * ny
with open(tmn, "r", encoding="utf-8") as f1:
    text1 = f1.read()
with open(tmx, "r", encoding="utf-8") as f2:
    text2 = f2.read()
# 쉼표 기준으로 전체 분리
parts1 = text1.split(",")
parts1 = [x.strip() for x in parts1]
parts1 = [x for x in parts1 if x != ""]
parts2 = text2.split(",")
parts2 = [x.strip() for x in parts2]
parts2 = [x for x in parts2 if x != ""]
print("tmn 개수:", len(parts1))
print("tmx 개수:", len(parts2))
print("기대 개수:", n_expected)


vals_tmn = np.array([float(x) for x in parts1], dtype=float)
vals_tmx = np.array([float(x) for x in parts2], dtype=float)
# 개수 확인
if vals_tmn.size != n_expected:
    raise ValueError(f"tmn 값 개수 불일치: {vals_tmn.size} / {n_expected}")
if vals_tmx.size != n_expected:
    raise ValueError(f"tmx 값 개수 불일치: {vals_tmx.size} / {n_expected}")


# 4. 2차원 격자로 reshape
# -----------------------------
arr_tmn = vals_tmn.reshape(ny, nx)
arr_tmx = vals_tmx.reshape(ny, nx)
# -----------------------------
# 5. 결측 처리
# -----------------------------
arr_tmn[arr_tmn == -99.0] = np.nan
arr_tmx[arr_tmx == -99.0] = np.nan
# -----------------------------
# 6. 일교차 계산 = 최고기온 - 최저기온
# -----------------------------
arr_dtr = arr_tmx - arr_tmn
print("일교차 shape:", arr_dtr.shape)
print("일교차 최소:", np.nanmin(arr_dtr))
print("일교차 최대:", np.nanmax(arr_dtr))


cell_size=5000
center_lat=38
center_lon=126
center_grid=(136, 43)
projection=pyproj.Proj(proj='lcc', lat_1=30, lat_2=60, lat_0=center_lat, lon_0=center_lon, datum='WGS84')
j_indices, i_indices = np.meshgrid(np.arange(nx), np.arange(ny))
x=(j_indices-center_grid[1])*cell_size
y=(i_indices-center_grid[0])*cell_size
lon, lat=projection(x, y, inverse=True)
print(lon.shape)
print(lat.shape)


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
    0., 1., 2., 3., 4., 5., 
    6., 7., 8., 9., 10.,
    11., 12., 13., 14., 15., 
    16., 17., 18., 19., 20.,
    21., 22., 23., 24., 25.,
    26., 27., 28., 29., 30. 
])
norm_t=BoundaryNorm(boundaries=bounds_t, ncolors=len(colormap_temp.colors))
colormap_temp
ticks_t=bounds_t[:]


# 동네예보 CRS
kma_lcc = CRS.from_proj4(
    "+proj=lcc +lat_1=30 +lat_2=60 "
    "+lat_0=38 +lon_0=126 "
    "+x_0=215000 +y_0=680000 "
    "+a=6371008.77 +b=6371008.77 "
    "+units=m +no_defs"
)
# 위경도(WGS84)
wgs84 = CRS.from_epsg(4326)
# 역변환기: 투영좌표 -> 위경도
transformer = Transformer.from_crs(kma_lcc, wgs84, always_xy=True)
# 격자 번호 (1-based)
xg = np.arange(1, nx + 1)
yg = np.arange(1, ny + 1)
# 격자 중심 투영좌표
x_center = xg * cell_size
y_center = yg * cell_size
# 2차원 mesh
Xc, Yc = np.meshgrid(x_center, y_center)
NX, NY = np.meshgrid(xg, yg)
# 위경도 변환
lon, lat = transformer.transform(Xc, Yc)
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


sido='C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/260422 최저기온 및 일교차/ramp_gis/sido_ramp.shp'
emd='C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/260422 최저기온 및 일교차/2. 읍면동/EMD.shp'
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
gpd_emd['sd_cd'] = gpd_emd['EMD_CD'].astype(str).str[:2]
gpd_emd['sido'] = gpd_emd['sd_cd'].map(sido_map)


df_grid['val']=arr_dtr.ravel()
print(np.unique(df_grid['val']))


gdf_grid = gpd.GeoDataFrame(
    df_grid.copy(),
    geometry=gpd.points_from_xy(df_grid['lon'], df_grid['lat']),
    crs='EPSG:4326'
)
print(gdf_grid.head())
print(gdf_grid.crs)


# 1) 읍면동 좌표계 맞추기
if gpd_emd.crs != kma_lcc:
    gpd_emd_proj = gpd_emd.to_crs(kma_lcc)
else:
    gpd_emd_proj = gpd_emd.copy()
# 2) 격자 중심점 -> 5km 격자 폴리곤 만들기
grid_size = 5000  # m
gdf_cell = gpd.GeoDataFrame(
    df_grid.copy(),
    geometry=[
        box(x - grid_size/2, y - grid_size/2, x + grid_size/2, y + grid_size/2)
        for x, y in zip(df_grid['x_m'], df_grid['y_m'])
    ],
    crs=kma_lcc
)
# 3) 겹치는 읍면동 모두 매칭
gdf_match = gpd.sjoin(
    gdf_cell,
    gpd_emd_proj[['EMD_CD', 'EMD_NM', 'SGG', 'sd_cd', 'sido', 'geometry']],
    how='left',
    predicate='intersects'
)


gdf_match_valid = gdf_match[gdf_match['EMD_NM'].notna()].copy()
print(gdf_match_valid.head())
print(len(gdf_match_valid))


emd_min = (
    gdf_match_valid.groupby(['EMD_CD', 'EMD_NM', 'SGG'])['val']
    .max()
    .reset_index(name='grid_min')
)
print(emd_min.head())


gpd_emd_min = gpd_emd.merge(
    emd_min[['EMD_CD', 'grid_min']],
    on='EMD_CD',
    how='left'
)
print(gpd_emd_min[['EMD_CD', 'EMD_NM', 'grid_min']].head())


fig, ax = plt.subplots(figsize=(9, 9))
gpd_emd_min.plot(
    ax=ax,
    column='grid_min',
    cmap=colormap_temp,
    norm=norm_t,
    linewidth=0.1,
    edgecolor='gray',
    legend=False   # 자동 범례 끄기
)
gpd_sido.boundary.plot(
    ax=ax,
    color='black',
    linewidth=0.8
)
# ==============================
# 범례 직접 생성
# ==============================
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
# boundaries 또는 직접 지정한 구간값 사용
cbar.set_ticks(bounds_t)   # 예: [-10, -5, 0, 5, 10, 15, 20]
cbar.set_ticklabels([str(int(v)) for v in bounds_t])
#cbar.set_label("최저기온(℃)")
ax.set_axis_off()
plt.show()


sgg='C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/#map_basic/1. 시군구/SGG.shp'
gpd_sgg=gpd.read_file(sgg)
if gpd_sgg.crs != gpd_emd_min.crs:
    gpd_sgg = gpd_sgg.to_crs(gpd_emd_min.crs)
gpd_sgg['sd_cd'] = gpd_sgg['CODE'].astype(str).str[:2]
gpd_sgg['sido'] = gpd_sgg['sd_cd'].map(sido_map)
fig, ax = plt.subplots(figsize=(9, 9))
gpd_sgg_gw = gpd_sgg[gpd_sgg['sd_cd'].astype(str).str.contains('47', na=False)].copy()
gpd_emd_min.plot(
    ax=ax, column='grid_min', cmap=colormap_temp, norm=norm_t, linewidth=0.1, edgecolor='gray', legend=True
)
gpd_emd.boundary.plot(ax=ax, color='white', linewidth=0.2, zorder=5)
gpd_sgg.boundary.plot(ax=ax, color='black', linewidth=0.8, zorder=6)
gpd_sido.boundary.plot(ax=ax, color='yellow', linewidth=3., zorder=7)
# 전북만 선택
#gw = gpd_emd_min[gpd_emd_min['sd_cd'].astype(str).str.contains('48', na=False)]
gw = gpd_emd_min[(gpd_emd_min['sd_cd'].astype(str).str.startswith('47', na=False)) & (gpd_emd_min['SGG'] != '울릉군')].copy()
minx, miny, maxx, maxy = gw.total_bounds
pad_x = (maxx - minx) * 0.05
pad_y = (maxy - miny) * 0.05
ax.set_xlim(minx - pad_x, maxx + pad_x)
ax.set_ylim(miny - pad_y, maxy + pad_y)
gpd_sgg_gw['label_point'] = gpd_sgg_gw.geometry.representative_point()
for _, row in gpd_sgg_gw.iterrows():
    txt = ax.text(
        row['label_point'].x,
        row['label_point'].y,
        row['SGG'],   # <-- 시군구 이름 컬럼명에 맞게 수정
        fontsize=10,
        fontweight='bold',
        ha='center',
        va='center',
        color='black',
        zorder=20
    )
    # QGIS 느낌의 흰색 halo
    txt.set_path_effects([
        pe.withStroke(linewidth=3, foreground='white')
    ])
plt.show()
