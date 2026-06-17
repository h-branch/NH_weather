import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point
from pathlib import Path

# =====================================================
# 1. 경로 설정
# =====================================================

sgg_path = r"C:\Users\lhj15\OneDrive\문서\[NH]\이상기후대응팀\#map_basic\1. 시군구\SGG.shp"

out_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\온열질환\processed")
out_dir.mkdir(parents=True, exist_ok=True)

out_path = out_dir / "grid_to_sgg.csv"

# =====================================================
# 2. 동네예보 격자 설정
# =====================================================

NX = 149
NY = 253
GRID_SIZE = 5000  # 5km

# 사용자님이 쓰던 동네예보 LCC 기준
kma_lcc = (
    "+proj=lcc "
    "+lat_1=30 "
    "+lat_2=60 "
    "+lat_0=38 "
    "+lon_0=126 "
    "+x_0=215000 "
    "+y_0=680000 "
    "+a=6371008.77 "
    "+b=6371008.77 "
    "+units=m "
    "+no_defs"
)

# =====================================================
# 3. 시군구 shp 읽기
# =====================================================

gdf_sgg = gpd.read_file(sgg_path)

print("시군구 shp 컬럼:")
print(gdf_sgg.columns)
print("시군구 CRS:", gdf_sgg.crs)

# 시군구명을 담은 컬럼 자동 탐색
name_candidates = ["SGG", "SGG_NM", "SIG_KOR_NM", "SIG_NM", "name", "NAME"]
code_candidates = ["CODE", "SGG_CD", "SIG_CD", "code"]

sgg_name_col = None
sgg_code_col = None

for c in name_candidates:
    if c in gdf_sgg.columns:
        sgg_name_col = c
        break

for c in code_candidates:
    if c in gdf_sgg.columns:
        sgg_code_col = c
        break

if sgg_name_col is None:
    raise ValueError("시군구명 컬럼을 찾지 못했습니다. gdf_sgg.columns를 확인하세요.")

if sgg_code_col is None:
    raise ValueError("시군구 코드 컬럼을 찾지 못했습니다. gdf_sgg.columns를 확인하세요.")

print("사용 시군구명 컬럼:", sgg_name_col)
print("사용 시군구코드 컬럼:", sgg_code_col)

# 시도코드 생성
gdf_sgg["SGG_CD"] = gdf_sgg[sgg_code_col].astype(str)
gdf_sgg["SIDO_CD"] = gdf_sgg["SGG_CD"].str[:2]
gdf_sgg["시군구"] = gdf_sgg[sgg_name_col].astype(str)

# 시도명 매핑
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
    "42": "강원특별자치도",
    "43": "충청북도",
    "44": "충청남도",
    "45": "전북특별자치도",
    "46": "전라남도",
    "47": "경상북도",
    "48": "경상남도",
    "50": "제주특별자치도",
}

gdf_sgg["시도"] = gdf_sgg["SIDO_CD"].map(sido_map)

# 북한 등 불필요 지역 제거
gdf_sgg = gdf_sgg[gdf_sgg["시도"].notna()].copy()

# 동네예보 LCC로 변환
gdf_sgg_lcc = gdf_sgg.to_crs(kma_lcc)

# =====================================================
# 4. 동네예보 격자점 생성
# =====================================================

rows = []

for iy in range(NY):
    for ix in range(NX):
        grid_id = iy * NX + ix

        # 격자 중심점 좌표
        x = ix * GRID_SIZE
        y = iy * GRID_SIZE

        rows.append({
            "grid_id": grid_id,
            "ix": ix,
            "iy": iy,
            "geometry": Point(x, y)
        })

gdf_grid = gpd.GeoDataFrame(
    rows,
    geometry="geometry",
    crs=kma_lcc
)

print("격자 수:", len(gdf_grid))

# =====================================================
# 5. 격자점 → 시군구 공간결합
# =====================================================

joined = gpd.sjoin(
    gdf_grid,
    gdf_sgg_lcc[["시도", "시군구", "SGG_CD", "geometry"]],
    how="left",
    predicate="within"
)

# 혹시 경계선 위에 있어 매칭 안 된 격자는 최근접 시군구로 보완
missing = joined[joined["시군구"].isna()].copy()

print("1차 매칭 누락 격자 수:", len(missing))

if len(missing) > 0:
    matched = joined[joined["시군구"].notna()].copy()

    nearest = gpd.sjoin_nearest(
        missing[["grid_id", "ix", "iy", "geometry"]],
        gdf_sgg_lcc[["시도", "시군구", "SGG_CD", "geometry"]],
        how="left",
        distance_col="dist_m"
    )

    # 기존 joined에서 누락분 제거 후 nearest 붙이기
    joined_ok = joined[joined["시군구"].notna()].copy()
    joined = pd.concat([joined_ok, nearest], ignore_index=True)

# =====================================================
# 6. 저장
# =====================================================

grid_to_sgg = joined[[
    "grid_id", "ix", "iy", "시도", "시군구", "SGG_CD"
]].copy()

grid_to_sgg = grid_to_sgg.sort_values("grid_id").reset_index(drop=True)

grid_to_sgg.to_csv(out_path, index=False, encoding="utf-8-sig")

print("저장 완료:", out_path)
print(grid_to_sgg.head())
print(grid_to_sgg.tail())
print("매칭된 시군구 수:", grid_to_sgg["시군구"].nunique())
print("전체 격자 수:", len(grid_to_sgg))
