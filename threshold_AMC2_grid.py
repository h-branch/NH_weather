import numpy as np
import pandas as pd
import geopandas as gpd


sido='C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/260422 최저기온 및 일교차/ramp_gis/sido_ramp.shp'
emd='C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/260422 최저기온 및 일교차/2. 읍면동/EMD.shp'
gpd_sido=gpd.read_file(sido)
gpd_emd=gpd.read_file(emd)
if gpd_sido.crs != gpd_emd.crs:
    gpd_emd = gpd_emd.to_crs(gpd_sido.crs)

grid='C:/Users/lhj15/OneDrive/문서/[NH]/강수위험지도/영향한계강우량/Threshold_Rainfall.shp'
gpd_grid=gpd.read_file(grid)
keep=['cn2a', 'cn2b', 'cn2c', 'geometry']
gpd_grid=gpd_grid[keep]

gpd_grid=gpd_grid.to_crs(gpd_emd.crs)
# 격자에 고유 ID 부여
gpd_grid = gpd_grid.reset_index(drop=True)
gpd_grid["grid_id"] = gpd_grid.index
# 격자-읍면동 교차 영역 생성
inter = gpd.overlay(
    gpd_grid[["grid_id", "cn2a", "cn2b", "cn2c", "geometry"]],
    gpd_emd[["EMD_CD", "EMD_NM", "geometry"]],
    how="intersection"
)
# 교차 면적 계산
inter["area"] = inter.geometry.area
# 면적가중값 계산
for col in ["cn2a", "cn2b", "cn2c"]:
    inter[f"{col}_weighted"] = inter[col] * inter["area"]
# 읍면동별 면적가중평균
emd_weighted_mean = (
    inter
    .groupby(["EMD_CD", "EMD_NM"], as_index=False)
    .agg(
        area_sum=("area", "sum"),
        cn2a_sum=("cn2a_weighted", "sum"),
        cn2b_sum=("cn2b_weighted", "sum"),
        cn2c_sum=("cn2c_weighted", "sum")
    )
)
emd_weighted_mean["cn2a"] = emd_weighted_mean["cn2a_sum"] / emd_weighted_mean["area_sum"]
emd_weighted_mean["cn2b"] = emd_weighted_mean["cn2b_sum"] / emd_weighted_mean["area_sum"]
emd_weighted_mean["cn2c"] = emd_weighted_mean["cn2c_sum"] / emd_weighted_mean["area_sum"]
emd_weighted_mean = emd_weighted_mean[
    ["EMD_CD", "EMD_NM", "cn2a", "cn2b", "cn2c"]
]
# 읍면동 shp에 붙이기
gpd_emd_weighted = gpd_emd.merge(
    emd_weighted_mean,
    on=["EMD_CD", "EMD_NM"],
    how="left"
)

gpd_emd_weighted.to_file(
    "C:/Users/lhj15/OneDrive/문서/[NH]/강수위험지도/영향한계강우량/gpd_emd_weighted.shp",
    encoding="cp949"
)
