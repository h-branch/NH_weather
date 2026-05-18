import requests
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import geopandas as gpd

Key='R3WTydcASuG1k8nXABrhgA'
aws_grid_url='https://apihub.kma.go.kr/api/typ01/cgi-bin/aws/nph-aws_min_obj?'


def aws_grid_download():
# ※코드 수행 전 기간, 저장 간격 수정 필요※
    _start=datetime(2025,9,1,0,0) #기간 시작 시점
    _end=datetime(2025,9,30,23,0) #기간 종료 시점
    _int=timedelta(hours=1) #저장 간격
    while _start<=_end:
        _temp=_start.strftime('%Y%m%d%H%M')
        _option={
            'obs': 'rn_60m', #rn_60m, rn_03h, rn_06h
            'tm': _temp,
            'stn': '0',
            'obj': 'mq',
            'map': 'D3',
            'grid': '1',
            'gov': '',
            'authKey': Key
        }
        _response=requests.get(aws_grid_url, _option)
        _f_path=f'C:/Users/lhj15/OneDrive/문서/[NH]\강우빈도/hourly/AwsGrid_{_temp}.txt'
        with open(_f_path, 'wb') as _f:
            _f.write(_response.content)
        print(f'데이터 저장 완료: {_f_path}')
        _start += _int
download=aws_grid_download()


# =====================================================
# AWS 객관분석 강수자료
# 시군구별 강수 구간 전체 총 카운트 산정
# =====================================================
import os
import re
from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd
from pyproj import CRS

# =====================================================
# 1. 경로 설정
# =====================================================
data_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\강우빈도\hourly")
sgg_shp = r"C:\Users\lhj15\OneDrive\문서\[NH]\이상기후대응팀\#map_basic\1. 시군구\SGG.shp"
# 결과 저장 경로
out_csv = data_dir.parent / "AWS객관분석_시군구별_시강수_23년카운트.csv"

# =====================================================
# 2. AWS 객관분석 격자 기본 설정
# =====================================================
nx = 681
ny = 681
grid_size = 1000
crs_aws = CRS.from_proj4(
    "+proj=lcc "
    "+lat_1=30 "
    "+lat_2=60 "
    "+lat_0=38 "
    "+lon_0=126 "
    "+x_0=0 "
    "+y_0=0 "
    "+a=6371008.77 "
    "+b=6371008.77 "
    "+units=m "
    "+no_defs"
)
# 공간결합 결과가 이상하면 이 값 보정 필요
x_start = -340000
y_start = -340000

# =====================================================
# 3. AWS Grid txt 읽기 함수
# =====================================================
def read_aws_grid_txt(file_path, nx=681, ny=681):
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
    # 첫 줄 header 제외
    data_text = "".join(lines[1:])
    # 콤마, 공백, 줄바꿈 기준 분리
    parts = re.split(r"[,\s]+", data_text)
    parts = [p for p in parts if p != ""]
    values = np.array([float(p) for p in parts], dtype=float)
    expected_size = nx * ny
    if values.size != expected_size:
        raise ValueError(
            f"격자 개수가 맞지 않습니다: {os.path.basename(file_path)}\n"
            f"읽은 개수: {values.size:,}, 기대 개수: {expected_size:,}"
        )
    arr = values.reshape(ny, nx)
    # 음수 결측값 제거
    arr[arr <= -90] = np.nan
    # 양수 이상값 제거
    # 일강수량 기준 1000mm 초과는 비정상값으로 처리
    arr[arr > 1000] = np.nan
    return arr

# =====================================================
# 4. AWS 격자 중심점 GeoDataFrame 생성
# =====================================================
def make_aws_grid_points(nx=681, ny=681, grid_size=1000, x_start=-340000, y_start=-340000):
    xs = x_start + np.arange(nx) * grid_size
    ys = y_start + np.arange(ny) * grid_size
    xx, yy = np.meshgrid(xs, ys)
    df_grid = pd.DataFrame({
        "grid_id": np.arange(nx * ny),
        "x": xx.ravel(),
        "y": yy.ravel()
    })
    gdf_grid = gpd.GeoDataFrame(
        df_grid,
        geometry=gpd.points_from_xy(df_grid["x"], df_grid["y"]),
        crs=crs_aws
    )
    return gdf_grid

# =====================================================
# 5. 시군구 경계 읽기
# =====================================================
gpd_sgg = gpd.read_file(sgg_shp)
print("시군구 shp 컬럼:")
print(gpd_sgg.columns)
# 사용자님 SGG.shp 기준
sgg_code_col = "CODE"
sgg_name_col = "SGG"
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
    "50": "제주특별자치도"
}
# 중요: SD_CD 쓰지 말고 CODE 앞 2자리 사용
gpd_sgg[sgg_code_col] = gpd_sgg[sgg_code_col].astype(str).str.strip()
gpd_sgg["sido_cd"] = gpd_sgg[sgg_code_col].str[:2]
gpd_sgg["sido"] = gpd_sgg["sido_cd"].map(sido_map)
gpd_sgg["sgg"] = gpd_sgg[sgg_name_col].astype(str).str.strip()
print("시군구 확인:")
print(gpd_sgg[[sgg_code_col, "sido_cd", "sido", "sgg"]].head(20))
print("sido 결측 개수:", gpd_sgg["sido"].isna().sum())
print("시도 목록:")
print(gpd_sgg["sido"].value_counts(dropna=False))
if gpd_sgg["sido"].isna().all():
    raise ValueError(
        "sido가 전부 NaN입니다. CODE 앞 2자리와 sido_map 매핑을 확인해야 합니다."
    )

# =====================================================
# 6. 격자점과 시군구 공간결합
# =====================================================
gdf_grid = make_aws_grid_points(
    nx=nx,
    ny=ny,
    grid_size=grid_size,
    x_start=x_start,
    y_start=y_start
)
# 좌표계 맞추기
gdf_grid = gdf_grid.to_crs(gpd_sgg.crs)
gdf_join = gpd.sjoin(
    gdf_grid,
    gpd_sgg[[sgg_code_col, "sido", "sgg", "geometry"]],
    how="inner",
    predicate="within"
)
grid_sgg = gdf_join[["grid_id", sgg_code_col, "sido", "sgg"]].copy()
print("시군구에 포함된 격자 수:", len(grid_sgg))
print(grid_sgg.head())
print("시도별 격자 수:")
print(grid_sgg["sido"].value_counts(dropna=False))
if len(grid_sgg) == 0:
    raise ValueError("grid_sgg가 0개입니다. AWS 격자 좌표와 시군구 shp가 겹치지 않습니다.")
if grid_sgg["sido"].isna().all():
    raise ValueError("grid_sgg의 sido가 전부 NaN입니다. 시도코드 매핑이 잘못되었습니다.")

# =====================================================
# 7. 전체 txt 파일 목록 가져오기
# =====================================================
txt_files = []
for p in data_dir.rglob("*"):
    if p.is_file() and p.suffix.lower() == ".txt":
        txt_files.append(str(p))
# 중복 제거
txt_files = sorted(set(txt_files))
print("최종 처리 파일 수:", len(txt_files))
for f in txt_files[:10]:
    print(f)
if len(txt_files) == 0:
    raise FileNotFoundError(
        f"처리할 txt 파일을 찾지 못했습니다.\n"
        f"확인 경로: {data_dir}"
    )

# =====================================================
# 8. 전체 파일 반복 처리
# =====================================================
result_list = []
error_list = []
for file_path in txt_files:
    fname = os.path.basename(file_path)
    print("처리 중:", fname)
    try:
        # 1) AWS 객관분석 격자 읽기
        arr = read_aws_grid_txt(file_path, nx=nx, ny=ny)
        # 2) 1차원 배열 변환
        rain_flat = arr.ravel()
        # 3) grid_id와 강수량 연결
        df_rain = pd.DataFrame({
            "grid_id": np.arange(nx * ny),
            "rain": rain_flat
        })
        # 4) 시군구 정보 결합
        df = grid_sgg.merge(df_rain, on="grid_id", how="left")
        # 5) 결측 제거
        df = df.dropna(subset=["rain"]).copy()
        if len(df) == 0:
            print("  - 유효 강수자료 없음, 건너뜀:", fname)
            error_list.append({
                "file_name": fname,
                "error": "유효 강수자료 없음"
            })
            continue
        # =================================================
        # 비중복 구간별 카운트
        # =================================================
        #df["cnt_50_under"] = df["rain"] <= 50
        df["cnt_30_50"] = (df["rain"] > 30) & (df["rain"] <= 50)
        df["cnt_50_70"] = (df["rain"] > 50) & (df["rain"] <= 70)
        df["cnt_70_100"] = (df["rain"] > 70) & (df["rain"] <= 100)
        df["cnt_100_over"] = df["rain"] > 100
        # =================================================
        # 누적 초과 기준 카운트
        # =================================================
        df["over_50"] = df["rain"] > 50
        df["over_70"] = df["rain"] > 70
        df["over_100"] = df["rain"] > 100
        # 파일 1개에 대한 시군구별 집계
        temp = (
            df.groupby([sgg_code_col, "sido", "sgg"], as_index=False, dropna=False)
            .agg(
                # 비중복 구간
                #cnt_50_under=("cnt_50_under", "sum"),
                cnt_30_50=("cnt_30_50", "sum"),
                cnt_50_70=("cnt_50_70", "sum"),
                cnt_70_100=("cnt_70_100", "sum"),
                cnt_100_over=("cnt_100_over", "sum"),
                # 누적 초과 기준
                over_50=("over_50", "sum"),
                over_70=("over_70", "sum"),
                over_100=("over_100", "sum"),
                # 검토용
                valid_grid_count=("rain", "count"),
                rain_sum=("rain", "sum"),
                max_rain=("rain", "max")
            )
        )
        result_list.append(temp)
    except Exception as e:
        print("  - 오류 발생, 건너뜀:", fname)
        print("  - 오류 내용:", e)
        error_list.append({
            "file_name": fname,
            "error": str(e)
        })
        continue

# =====================================================
# 9. 파일별 중간 결과 합치기
# =====================================================
if len(result_list) == 0:
    raise ValueError(
        "처리된 결과가 없습니다.\n"
        "가능 원인:\n"
        "1) txt 파일은 찾았지만 모든 파일 읽기에 실패함\n"
        "2) grid_sgg 공간결합 결과가 비어 있음\n"
        "3) AWS 격자 좌표가 시군구 shp와 맞지 않음\n"
        "4) 모든 강수값이 결측 또는 이상값으로 제거됨"
    )
df_file_result = pd.concat(result_list, ignore_index=True)
print("파일별 중간 결과 행 수:", len(df_file_result))
print(df_file_result.head())
if len(df_file_result) == 0:
    raise ValueError("df_file_result가 0행입니다. 파일별 집계 결과가 없습니다.")

# =====================================================
# 10. 시군구별 전체 총 카운트
# =====================================================
df_total = (
    df_file_result
    .groupby([sgg_code_col, "sido", "sgg"], as_index=False, dropna=False)
    .agg(
        # 비중복 구간 총합
        #cnt_50_under=("cnt_50_under", "sum"),
        cnt_30_50=("cnt_30_50", "sum"),
        cnt_50_70=("cnt_50_70", "sum"),
        cnt_70_100=("cnt_70_100", "sum"),
        cnt_100_over=("cnt_100_over", "sum"),
        # 누적 초과 기준 총합
        over_50=("over_50", "sum"),
        over_70=("over_70", "sum"),
        over_100=("over_100", "sum"),
        # 검토용
        valid_grid_count=("valid_grid_count", "sum"),
        rain_sum=("rain_sum", "sum"),
        max_rain=("max_rain", "max")
    )
)
if len(df_total) == 0:
    raise ValueError("df_total이 0행입니다. groupby 결과가 비어 있습니다.")
# 전체 평균강수량 계산
df_total["mean_rain"] = df_total["rain_sum"] / df_total["valid_grid_count"]
# 컬럼명 정리
df_total = df_total.rename(columns={
    sgg_code_col: "SGG_CD",
    "sido": "시도",
    "sgg": "시군구",
    #"cnt_50_under": "50mm 이하",
    "cnt_30_50": "30mm 초과_50mm 이하",
    "cnt_50_70": "50mm 초과_70mm 이하",
    "cnt_70_100": "70mm 초과_100mm 이하",
    "cnt_100_over": "100mm 초과",
    "over_50": "50mm 초과_누적",
    "over_70": "70mm 초과_누적",
    "over_100": "100mm 초과_누적",
    "valid_grid_count": "유효격자_전체개수",
    "rain_sum": "전체기간_강수량합계",
    "max_rain": "전체기간_최대강수량",
    "mean_rain": "전체기간_평균강수량"
})
# 보기 좋게 정렬
df_total = df_total[
    [
        "SGG_CD",
        "시도",
        "시군구",
        #"50mm 이하",
        "30mm 초과_50mm 이하",
        "50mm 초과_70mm 이하",
        "70mm 초과_100mm 이하",
        "100mm 초과",
        "50mm 초과_누적",
        "70mm 초과_누적",
        "100mm 초과_누적",
        "유효격자_전체개수",
        "전체기간_강수량합계",
        "전체기간_최대강수량",
        "전체기간_평균강수량"
    ]
]
print("최종 결과 행 수:", len(df_total))
print("최종 결과 미리보기:")
print(df_total.head())

# =====================================================
# 11. CSV 저장
# =====================================================
out_csv.parent.mkdir(parents=True, exist_ok=True)
df_total.to_csv(
    out_csv,
    index=False,
    encoding="utf-8-sig"
)
print("전체 총 카운트 저장 완료")
print("저장 경로:", out_csv)
print("파일 존재 여부:", out_csv.exists())
if out_csv.exists():
    print("파일 크기:", os.path.getsize(out_csv), "bytes")

# =====================================================
# 12. 오류 로그 저장
# =====================================================
if len(error_list) > 0:
    df_error = pd.DataFrame(error_list)
    out_error_csv = data_dir.parent / "AWS객관분석_처리오류로그.csv"
    df_error.to_csv(
        out_error_csv,
        index=False,
        encoding="utf-8-sig"
    )
    print("오류 로그 저장 완료")
    print("오류 로그 경로:", out_error_csv)
    print("오류 로그 존재 여부:", out_error_csv.exists())
else:
    print("오류 파일 없음")
