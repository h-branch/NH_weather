import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


sgg='C:/Users/lhj15/OneDrive/문서/[NH]/이상기후대응팀/#map_basic/1. 시군구/SGG.shp'
gpd_sgg=gpd.read_file(sgg)

# 이상저온
txt=['순창','임실','정읍','남원','순천','광양','곡성','고흥','구례','보성','화순','담양',
     '장흥','하동','진주','사천','산청','함안','창원','고성','함양','여수','완도',

     '신안','고흥','해남','무안','강진','함평','장흥','보성','완도','나주','영암','진도',
     '합천','남해','창녕','함안','의령','하동','산청','밀양','진주','거창','사천','창원',
     '제주','서귀포','달성',

     '고창','부안','임실','정읍','완주','남원','장수','김제','순창','무안','신안','해남',
     '나주','함평','장성','영광','영암','강진','담양','진도','화순','목포','고령','김천',
     '문경','안동','상주','의성','예천','구미','성주','영암','영주','칠곡','함양','합천',
     '산청','창녕','거창','의령','진주','군위','달성',
     
     '태안','서산','홍성','보령','당진','단양','의성',
     
     '남양주','평택','김포','이천','안성','여주','영동','괴산','충주','음성','청주','보은',
     '괴산','금산','천안','부여','공주','아산','논산','예산','당진','남원','김제','진안',
     '무주','장수','임실','순창','고창','전주','정읍','익산','강진','해남','곡성','구례',
     '나주','영암','함평','장성','포항','안동','영주','영천','군위','의성','청송','성주',
     '봉화','상주','김천','예천','문경','영양','하동','거창','밀양','함양']

# 동명이칭 ex. 광주, 중구, 서구, 동구
# 앞 2글자 key
df_regions["sgg_key"] = df_regions["지역명"].str[:2]

# 2. gpd_sgg 매칭용 테이블 생성
gpd_sgg_count = gpd_sgg.copy()
gpd_sgg_count["CODE"] = gpd_sgg_count["CODE"].astype(str)
gpd_sgg_match = gpd_sgg_count[["CODE", "SGG", "SD_CD", "geometry"]].copy()
gpd_sgg_match["sgg_key"] = gpd_sgg_match["SGG"].str[:2]

# 3. 앞 2글자 기준 1차 매칭
df_matched = df_regions.merge(
    gpd_sgg_match[["CODE", "SGG", "SD_CD", "sgg_key"]],
    on="sgg_key",
    how="left"
)

# 4. 동명이칭 예외 처리
#    광주  → 경기도 광주시만 사용, CODE 앞 2자리 41
#    중구/서구/동구 → 대전광역시만 사용, CODE 앞 2자리 30
# 광주는 경기도 광주시만 남김
mask_bad_gwangju = ((df_matched["지역명"] == "광주") & (df_matched["CODE"].astype(str).str[:2] != "41"))
# 중구, 서구, 동구는 대전광역시만 남김
mask_bad_daejeon_gu = ((df_matched["지역명"].isin(["중구", "서구", "동구"])) & (df_matched["CODE"].astype(str).str[:2] != "30"))
# 예외 조건에 맞지 않는 후보 제거
df_matched = df_matched[
    ~(mask_bad_gwangju | mask_bad_daejeon_gu)
].copy()

# 5. 매칭 안 된 지역 확인
unmatched = df_matched[df_matched["SGG"].isna()]
print("매칭 안 된 지역")
display(unmatched[["지역명", "sgg_key"]].drop_duplicates())

# 6. 시군구별 언급횟수 집계
count_table = (
    df_matched
    .dropna(subset=["CODE", "SGG"])
    .groupby(["CODE", "SGG"], as_index=False)
    .size()
    .rename(columns={"size": "언급횟수"})
    .sort_values("언급횟수", ascending=False)
)
print("언급횟수 표")
display(count_table.head(50))

# 7. gpd_sgg에 언급횟수 붙이기
gpd_sgg_count = gpd_sgg_count.merge(
    count_table,
    on=["CODE", "SGG"],
    how="left"
)
gpd_sgg_count["언급횟수"] = gpd_sgg_count["언급횟수"].fillna(0).astype(int)

# 8. 등급 분류
def classify_count(x):
    if x >= 3:
        return "3번 이상"
    elif x == 2:
        return "2번"
    elif x == 1:
        return "1번"
    else:
        return "0번"

gpd_sgg_count["등급"] = gpd_sgg_count["언급횟수"].apply(classify_count)

# 9. 색상 지정
color_map = {
    "3번 이상": "#e31a1c",  # 적색
    "2번": "#ffcc00",       # 황색
    "1번": "#1f78b4",       # 청색
    "0번": "#d9d9d9"        # 회색
}
gpd_sgg_count["color"] = gpd_sgg_count["등급"].map(color_map)

# 10. 지도 그리기
fig, ax = plt.subplots(figsize=(10, 10))
gpd_sgg_count.plot(
    ax=ax,
    color=gpd_sgg_count["color"],
    edgecolor="black",
    linewidth=0.3
)
# ax.set_title("시군구별 언급 횟수", fontsize=15)
ax.set_axis_off()
legend_elements = [
    mpatches.Patch(color="#e31a1c", label="3번 이상"),
    mpatches.Patch(color="#ffcc00", label="2번"),
    mpatches.Patch(color="#1f78b4", label="1번"),
    mpatches.Patch(color="#d9d9d9", label="0번")
]
# ax.legend(
#     handles=legend_elements,
#     title="언급횟수",
#     loc="lower left"
# )
plt.show()
