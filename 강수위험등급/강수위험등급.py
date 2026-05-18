import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm
from matplotlib.patches import Patch

plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False


# =====================================================
# 1. 자료 경로
# =====================================================
f_path = r"C:\Users\lhj15\OneDrive\문서\[NH]\강우빈도\AWS객관분석_시군구별_시강수_23-25년카운트.csv"
sido_path = r"C:\Users\lhj15\OneDrive\문서\[NH]\이상기후대응팀\#map_basic\ramp_gis\sido_ramp.shp"
emd_path = r"C:\Users\lhj15\OneDrive\문서\[NH]\이상기후대응팀\#map_basic\2. 읍면동\EMD.shp"

# =====================================================
# 2. 데이터 읽기
# =====================================================
f = pd.read_csv(f_path)
gpd_sido = gpd.read_file(sido_path)
gpd_emd = gpd.read_file(emd_path)
# CRS 통일
if gpd_sido.crs != gpd_emd.crs:
    gpd_sido = gpd_sido.to_crs(gpd_emd.crs)

# =====================================================
# 3. 필요한 컬럼만 추출
# =====================================================
keep_cols = [
    "SGG_CD",
    "시도",
    "시군구",
    "50mm 초과_70mm 이하",
    "70mm 초과_100mm 이하",
    "100mm 초과"
]
df_selected = f[keep_cols].copy()

# =====================================================
# 4. SGG_CD 정리 함수
#    42150.0 -> 42150
#    42150   -> 42150
# =====================================================
def clean_sgg_code(x):
    if pd.isna(x):
        return np.nan
    return str(int(float(x))).zfill(5)
df_selected["SGG_CD"] = df_selected["SGG_CD"].apply(clean_sgg_code)

# =====================================================
# 5. 종합 등급 생성
#    기준:
#    100mm 초과 발생        -> 심각
#    70~100mm 발생          -> 경계
#    50~70mm 발생 50건 이상 -> 주의
#    50~70mm 발생 50건 미만 -> 관심
# =====================================================
col_50_70 = "50mm 초과_70mm 이하"
col_70_100 = "70mm 초과_100mm 이하"
col_100 = "100mm 초과"
# 숫자형 변환
for col in [col_50_70, col_70_100, col_100]:
    df_selected[col] = pd.to_numeric(df_selected[col], errors="coerce").fillna(0)
def classify_total_stage(row):
    count_50_70 = row[col_50_70]
    count_70_100 = row[col_70_100]
    count_100 = row[col_100]
    if count_100 > 0:
        return pd.Series(["심각", 4])
    elif count_70_100 > 0:
        return pd.Series(["경계", 3])
    elif count_50_70 >= 50:
        return pd.Series(["주의", 2])
    else:
        return pd.Series(["관심", 1])
df_selected[["등급", "stage_code"]] = df_selected.apply(
    classify_total_stage,
    axis=1
)
df_selected.head()

# =====================================================
# 6. 읍면동 shp에서 시군구 코드 생성
#    EMD_CD 8자리 중 앞 5자리가 SGG_CD
# =====================================================
gpd_emd = gpd_emd.copy()
gpd_emd["EMD_CD"] = gpd_emd["EMD_CD"].astype(str).str.strip()
gpd_emd["SGG_CD"] = gpd_emd["EMD_CD"].str[:5]

# =====================================================
# 7. 병합용 데이터 생성
# =====================================================
df_map = df_selected[
    [
        "SGG_CD",
        "시도",
        "시군구",
        col_50_70,
        col_70_100,
        col_100,
        "등급",
        "stage_code"
    ]
].copy()
df_map = df_map.drop_duplicates(subset=["SGG_CD"])

# =====================================================
# 8. 읍면동 지도에 시군구 단위 등급 붙이기
#    1차: SGG_CD 코드 기준 병합
#    2차: 행정구가 있는 도시 보정
# =====================================================
# -----------------------------------------------------
# 8-1. 읍면동 SHP에서 SGG_CD 생성
# -----------------------------------------------------
gpd_emd = gpd_emd.copy()
gpd_emd["EMD_CD"] = gpd_emd["EMD_CD"].astype(str).str.strip()
gpd_emd["SGG"] = gpd_emd["SGG"].astype(str).str.strip()
# 읍면동 코드 앞 5자리 = 시군구 코드
gpd_emd["SGG_CD"] = gpd_emd["EMD_CD"].str[:5]
# -----------------------------------------------------
# 8-2. CSV 쪽 SGG_CD 정리
# -----------------------------------------------------
def clean_sgg_code(x):
    if pd.isna(x):
        return np.nan
    return str(int(float(x))).zfill(5)
df_selected["SGG_CD"] = df_selected["SGG_CD"].apply(clean_sgg_code)
df_selected["시도"] = df_selected["시도"].astype(str).str.strip()
df_selected["시군구"] = df_selected["시군구"].astype(str).str.strip()
# -----------------------------------------------------
# 8-3. 병합용 데이터 생성
# -----------------------------------------------------
df_map = df_selected[
    [
        "SGG_CD",
        "시도",
        "시군구",
        col_50_70,
        col_70_100,
        col_100,
        "등급",
        "stage_code"
    ]
].copy()
df_map = df_map.drop_duplicates(subset=["SGG_CD"])
# -----------------------------------------------------
# 8-4. 1차 병합: SGG_CD 기준
# -----------------------------------------------------
gpd_map = gpd_emd.merge(
    df_map,
    on="SGG_CD",
    how="left"
)
print("1차 코드 병합 후")
print("읍면동 전체 개수:", len(gpd_map))
print("등급 매칭 성공 개수:", gpd_map["stage_code"].notna().sum())
print("등급 매칭 실패 개수:", gpd_map["stage_code"].isna().sum())

# =====================================================
# 9. 행정구가 있는 도시 보정
#    예:
#    전주시완산구 -> 전주시
#    전주시덕진구 -> 전주시
#    청주시상당구 -> 청주시
#    포항시남구   -> 포항시
#    창원시의창구 -> 창원시
# =====================================================
def make_parent_sgg_name(s):
    """
    읍면동 SHP의 SGG명이 행정구 단위일 때
    상위 시 이름으로 변환하는 함수입니다.

    예:
    전주시완산구 -> 전주시
    청주시상당구 -> 청주시
    포항시남구   -> 포항시
    창원시성산구 -> 창원시
    삼척시       -> 삼척시
    고성군       -> 고성군
    """
    if pd.isna(s):
        return np.nan
    s = str(s).strip()
    # '시'가 포함되어 있고, 뒤에 '구'가 붙는 행정구 형태이면
    # 첫 번째 '시'까지 잘라서 상위 시 이름으로 사용
    if ("시" in s) and s.endswith("구"):
        return s[:s.find("시") + 1]
    return s
# 읍면동 SHP 쪽 상위 시군구명 생성
gpd_map["시군구_보정"] = gpd_map["SGG"].apply(make_parent_sgg_name)
# CSV 쪽 시군구명 기준 보정용 테이블
df_name_map = df_selected[
    [
        "시군구",
        "시도",
        col_50_70,
        col_70_100,
        col_100,
        "등급",
        "stage_code"
    ]
].copy()
df_name_map = df_name_map.drop_duplicates(subset=["시군구"])
# 보정용 컬럼명 변경
df_name_map = df_name_map.rename(
    columns={
        "시도": "시도_name",
        col_50_70: f"{col_50_70}_name",
        col_70_100: f"{col_70_100}_name",
        col_100: f"{col_100}_name",
        "등급": "등급_name",
        "stage_code": "stage_code_name"
    }
)
# 시군구명 기준으로 2차 병합
gpd_map = gpd_map.merge(
    df_name_map,
    left_on="시군구_보정",
    right_on="시군구",
    how="left",
    suffixes=("", "_drop")
)
# -----------------------------------------------------
# 1차 코드 병합에서 실패한 곳만 2차 이름 병합 결과로 채우기
# -----------------------------------------------------
mask = gpd_map["stage_code"].isna() & gpd_map["stage_code_name"].notna()
gpd_map.loc[mask, "시도"] = gpd_map.loc[mask, "시도_name"]
gpd_map.loc[mask, col_50_70] = gpd_map.loc[mask, f"{col_50_70}_name"]
gpd_map.loc[mask, col_70_100] = gpd_map.loc[mask, f"{col_70_100}_name"]
gpd_map.loc[mask, col_100] = gpd_map.loc[mask, f"{col_100}_name"]
gpd_map.loc[mask, "등급"] = gpd_map.loc[mask, "등급_name"]
gpd_map.loc[mask, "stage_code"] = gpd_map.loc[mask, "stage_code_name"]
# 불필요한 보정용 컬럼 정리
drop_cols = [
    "시도_name",
    f"{col_50_70}_name",
    f"{col_70_100}_name",
    f"{col_100}_name",
    "등급_name",
    "stage_code_name",
    "시군구_drop"
]
drop_cols = [c for c in drop_cols if c in gpd_map.columns]
gpd_map = gpd_map.drop(columns=drop_cols)
print("2차 행정구 보정 후")
print("등급 매칭 성공 개수:", gpd_map["stage_code"].notna().sum())
print("등급 매칭 실패 개수:", gpd_map["stage_code"].isna().sum())
# 아직도 자료없음인 지역 확인
unmatched = (
    gpd_map[gpd_map["stage_code"].isna()]
    [["EMD_CD", "EMD_NM", "SGG", "SGG_CD", "시군구_보정"]]
    .drop_duplicates()
    .sort_values(["SGG", "EMD_NM"])
)
print("아직 자료없음 읍면동 수:", len(unmatched))
print(unmatched.head(50))

# =====================================================
# 10. 색상 설정
# =====================================================
stage_colors = [
    "#00B050",  # 관심
    "#FFFF00",  # 주의
    "#FFA500",  # 경계
    "#FF0000"   # 심각
]
cmap_stage = ListedColormap(stage_colors)
norm_stage = BoundaryNorm(
    [0.5, 1.5, 2.5, 3.5, 4.5],
    cmap_stage.N
)

# =====================================================
# 11. 지도 그리기
# =====================================================
fig, ax = plt.subplots(figsize=(12, 14))
# 읍면동 단위 색칠
gpd_map.plot(
    ax=ax,
    column="stage_code",
    cmap=cmap_stage,
    norm=norm_stage,
    linewidth=0.04,
    edgecolor="lightgray",
    legend=False,
    missing_kwds={
        "color": "white",
        "edgecolor": "lightgray",
        "hatch": "///",
        "label": "자료없음"
    },
    zorder=1
)
# 읍면동 경계
gpd_map.boundary.plot(
    ax=ax,
    color="lightgray",
    linewidth=0.04,
    zorder=2
)
# 시도 경계
gpd_sido.boundary.plot(
    ax=ax,
    color="black",
    linewidth=1.2,
    zorder=5
)
# 범례
legend_elements = [
    Patch(facecolor="#00B050", edgecolor="black", label="관심: 50~70mm 50건 미만 발생"),
    Patch(facecolor="#FFFF00", edgecolor="black", label="주의: 50~70mm 50건 이상 발생"),
    Patch(facecolor="#FFA500", edgecolor="black", label="경계: 70~100mm 발생"),
    Patch(facecolor="#FF0000", edgecolor="black", label="심각: 100mm 초과 발생"),
    Patch(facecolor="white", edgecolor="lightgray", hatch="///", label="자료없음")
]
ax.legend(
    handles=legend_elements,
    title="1시간 강수량에 따른 위험등급",
    loc="lower left",
    fontsize=10,
    title_fontsize=11,
    frameon=True
)
ax.set_title(
    "최근 3개년 1시간 강수량 기반 강수 위험등급 산출",
    fontsize=16
)
ax.set_axis_off()
plt.tight_layout()
plt.show()

# =====================================================
# 지도 표출 전: 읍면동 단위 최종 매칭표 확인
# =====================================================
df_check_emd = gpd_map[
    [
        "EMD_CD",
        "EMD_NM",
        "SGG",          # SHP상 시군구명
        "SGG_CD",       # EMD_CD 앞 5자리에서 만든 시군구 코드
        "시도",         # CSV에서 붙은 시도명
        "시군구",       # CSV에서 붙은 시군구명
        "시군구_보정",  # 행정구 보정명
        col_50_70,
        col_70_100,
        col_100,
        "등급",
        "stage_code"
    ]
].copy()
# 매칭 여부 확인용 컬럼
df_check_emd["매칭상태"] = np.where(
    df_check_emd["stage_code"].isna(),
    "자료없음",
    "매칭완료"
)
# 보기 좋게 정렬
df_check_emd = df_check_emd.sort_values(
    by=["매칭상태", "시도", "시군구", "SGG", "EMD_NM"]
).reset_index(drop=True)
df_check_emd.head(100)

out_dir = r"C:\Users\lhj15\OneDrive\문서\[NH]\강우빈도"
df_check_emd.to_csv(
    out_dir + r"\읍면동단위_병합결과_지도표출전_v1.0.csv",
    index=False,
    encoding="utf-8-sig"
)
