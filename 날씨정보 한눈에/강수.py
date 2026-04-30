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
option={
    'tmfc':'2026042917', # 발표시간(생산)
    'tmef':'2026050312', # 발효시간(미래)
    'vars':'PCP',
    'authKey':Key
}
response=requests.get(short_url, option)
f_path='C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/260429 0501 일교차 확인/ex/2026050312.bin'
with open(f_path, 'wb') as f:
    f.write(response.content)


nx, ny = 149, 253
n_expected = nx * ny
with open(f_path, "r", encoding="utf-8") as f:
    text = f.read()
# 쉼표 기준으로 전체 분리
parts = text.split(",")
# 공백/줄바꿈 제거 후 빈 문자열 제거
parts = [x.strip() for x in parts]
parts = [x for x in parts if x != ""]
print("읽은 값 개수:", len(parts))
print("기대 값 개수:", n_expected)
print("앞 10개:", parts[:10])
values = np.array([float(x) for x in parts], dtype=float)
print("float 변환 후 개수:", values.size)
arr = values.reshape(ny, nx)
print("shape:", arr.shape)
print(arr)
arr[arr==-99.0]=np.nan


print(arr.shape)
print(np.nanmin(arr), np.nanmax(arr))
print(np.isnan(arr).sum())
print(np.isfinite(arr).sum())
unique_vals = np.unique(arr[~np.isnan(arr)])
print(unique_vals[:30])
print("유효값 개수:", len(unique_vals))


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


kma_color=['#ffffff', '#00c8ff', '#009bf5', '#0049f5', #청색
           '#00ff00', '#00be00', '#008c00', '#005a00', #녹색
           '#ffff00', '#ffdd1f', '#f9cb00', '#e0b900', '#ccaa00', #황색
           '#ff6600', '#ff3300', '#d20000', '#b40000', #적색
           '#dfa9ff', '#c969ff', '#b429ff', '#9300e4', #자색
           '#b3b4de', '#4c4eb1', '#000390' #남색
]
colormap_rain=ListedColormap(kma_color).with_extremes(over='#333333')
colormap_rain.set_bad([0,0,0,0])
bounds=np.array([
    0., 0.1, 0.5, 1.,
    2., 3., 4., 5., 
    6., 7., 8., 9., 10., 
    15., 20., 25., 30., 
    40., 50., 60., 70., 
    90., 110., 150.
])
norm=BoundaryNorm(boundaries=bounds, ncolors=len(colormap_rain.colors))
colormap_rain
ticks=bounds[:]


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


sido='C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/#map_basic/ramp_gis/sido_ramp.shp'
emd='C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/#map_basic/2. 읍면동/EMD.shp'
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


df_grid['val']=arr.ravel()
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


emd_max = (
    gdf_match_valid.groupby(['EMD_CD', 'EMD_NM', 'SGG'])['val']
    .max()
    .reset_index(name='grid_max')
)
print(emd_max.head())


gpd_emd_max = gpd_emd.merge(
    emd_max[['EMD_CD', 'grid_max']],
    on='EMD_CD',
    how='left'
)
print(gpd_emd_max[['EMD_CD', 'EMD_NM', 'grid_max']].head())


fig, ax = plt.subplots(figsize=(9, 9))
gpd_emd_max.plot(
    ax=ax, column='grid_max', cmap=colormap_rain, norm=norm, linewidth=0.1, edgecolor='gray', legend=True
)
gpd_sido.boundary.plot(ax=ax, color='black', linewidth=0.8)
plt.show()


sgg='C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/#map_basic/1. 시군구/SGG.shp'
gpd_sgg=gpd.read_file(sgg)
if gpd_sgg.crs != gpd_emd_max.crs:
    gpd_sgg = gpd_sgg.to_crs(gpd_emd_max.crs)
gpd_sgg['sd_cd'] = gpd_sgg['CODE'].astype(str).str[:2]
gpd_sgg['sido'] = gpd_sgg['sd_cd'].map(sido_map)
fig, ax = plt.subplots(figsize=(9, 9))
gpd_sgg_jb = gpd_sgg[gpd_sgg['sd_cd'].astype(str).str.contains('48', na=False)].copy()
gpd_emd_max.plot(
    ax=ax, column='grid_max', cmap=colormap_rain, norm=norm, linewidth=0.1, edgecolor='gray', legend=True
)
gpd_emd.boundary.plot(ax=ax, color='white', linewidth=0.2, zorder=5)
gpd_sgg.boundary.plot(ax=ax, color='black', linewidth=0.8, zorder=6)
gpd_sido.boundary.plot(ax=ax, color='black', linewidth=1.5, zorder=7)
# 전북만 선택
jb = gpd_emd_max[gpd_emd_max['sd_cd'].str.contains('48', na=False)]
minx, miny, maxx, maxy = jb.total_bounds
pad_x = (maxx - minx) * 0.05
pad_y = (maxy - miny) * 0.05
ax.set_xlim(minx - pad_x, maxx + pad_x)
ax.set_ylim(miny - pad_y, maxy + pad_y)
gpd_sgg_jb['label_point'] = gpd_sgg_jb.geometry.representative_point()
for _, row in gpd_sgg_jb.iterrows():
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
