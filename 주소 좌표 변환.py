import pandas as pd
import requests
import time
from pathlib import Path
import json


# =====================================================
# 1. 기본 설정
# =====================================================
KAKAO_REST_API_KEY = "5d8c3a0e42e83977dc6726b14519ab28"
input_path = Path("C:/Users/lhj15/OneDrive/문서/[NH]/무더위한파쉼터/local_nh(2605).csv")
output_path = Path("C:/Users/lhj15/OneDrive/문서/[NH]/무더위한파쉼터/local_nh_2605_geocoded.csv")


# =====================================================
# 2. CSV 읽기
# =====================================================
df = pd.read_csv(input_path, encoding="euc-kr")
print("원본 컬럼:")
print(df.columns)
print(df.head())
# 필수 컬럼 확인
required_cols = ["nm", "loc"]
for col in required_cols:
    if col not in df.columns:
        raise ValueError(f"{col} 컬럼이 없습니다. 현재 컬럼: {df.columns.tolist()}")


# =====================================================
# 3. 주소 문자열 정리
# =====================================================
df["nm"] = df["nm"].astype(str).str.strip()
df["loc"] = df["loc"].astype(str).str.strip()
# 주소검색용
df["address_query"] = df["loc"]
# 장소검색 보완용
df["place_query"] = df["loc"] + " " + df["nm"]


# =====================================================
# 4. 카카오 주소검색 API
# =====================================================
def geocode_kakao_address(address):
    """
    도로명주소 또는 지번주소를 카카오 주소검색 API에 넣어서
    위경도 좌표를 가져오는 함수입니다.
    """
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {
        "Authorization": f"KakaoAK {KAKAO_REST_API_KEY}"
    }
    params = {
        "query": address
    }
    try:
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=10
        )
        if response.status_code != 200:
            return {
                "geo_status": "API_ERROR",
                "matched_address": None,
                "road_address": None,
                "jibun_address": None,
                "lon": None,
                "lat": None,
                "error_msg": f"status_code={response.status_code}, response={response.text}"
            }
        data = response.json()
        documents = data.get("documents", [])
        if len(documents) == 0:
            return {
                "geo_status": "NO_RESULT",
                "matched_address": None,
                "road_address": None,
                "jibun_address": None,
                "lon": None,
                "lat": None,
                "error_msg": None
            }
        item = documents[0]
        road = item.get("road_address")
        jibun = item.get("address")
        road_address = road.get("address_name") if road else None
        jibun_address = jibun.get("address_name") if jibun else None
        return {
            "geo_status": "OK_ADDRESS",
            "matched_address": item.get("address_name"),
            "road_address": road_address,
            "jibun_address": jibun_address,
            "lon": item.get("x"),
            "lat": item.get("y"),
            "error_msg": None
        }
    except Exception as e:
        return {
            "geo_status": "EXCEPTION",
            "matched_address": None,
            "road_address": None,
            "jibun_address": None,
            "lon": None,
            "lat": None,
            "error_msg": str(e)
        }


# =====================================================
# 5. 카카오 장소검색 API
# 주소검색 실패 시 보완용
# =====================================================
def search_kakao_place(query):
    """
    주소검색이 실패한 경우,
    주소 + 농협명을 키워드로 장소검색해서 좌표를 보완하는 함수입니다.
    """
    url = "https://dapi.kakao.com/v2/local/search/keyword.json"
    headers = {
        "Authorization": f"KakaoAK {KAKAO_REST_API_KEY}"
    }
    params = {
        "query": query,
        "size": 5,
        "sort": "accuracy"
    }
    try:
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=10
        )
        if response.status_code != 200:
            return {
                "geo_status": "PLACE_API_ERROR",
                "matched_place_name": None,
                "matched_address": None,
                "road_address": None,
                "jibun_address": None,
                "lon": None,
                "lat": None,
                "phone": None,
                "place_url": None,
                "error_msg": f"status_code={response.status_code}, response={response.text}"
            }
        data = response.json()
        documents = data.get("documents", [])
        if len(documents) == 0:
            return {
                "geo_status": "PLACE_NO_RESULT",
                "matched_place_name": None,
                "matched_address": None,
                "road_address": None,
                "jibun_address": None,
                "lon": None,
                "lat": None,
                "phone": None,
                "place_url": None,
                "error_msg": None
            }
        item = documents[0]
        return {
            "geo_status": "OK_PLACE",
            "matched_place_name": item.get("place_name"),
            "matched_address": item.get("address_name"),
            "road_address": item.get("road_address_name"),
            "jibun_address": item.get("address_name"),
            "lon": item.get("x"),
            "lat": item.get("y"),
            "phone": item.get("phone"),
            "place_url": item.get("place_url"),
            "error_msg": None
        }
    except Exception as e:
        return {
            "geo_status": "PLACE_EXCEPTION",
            "matched_place_name": None,
            "matched_address": None,
            "road_address": None,
            "jibun_address": None,
            "lon": None,
            "lat": None,
            "phone": None,
            "place_url": None,
            "error_msg": str(e)
        }


# =====================================================
# 6. 전체 행 위경도 변환
# =====================================================
results = []
for idx, row in df.iterrows():
    address = str(row["address_query"]).strip()
    place_query = str(row["place_query"]).strip()
    # 1차: 주소검색
    result = geocode_kakao_address(address)
    # 2차: 주소검색 실패 시 장소검색 보완
    if result["geo_status"] != "OK_ADDRESS":
        place_result = search_kakao_place(place_query)
        # 장소검색 결과로 대체
        if place_result["geo_status"] == "OK_PLACE":
            result = place_result
        else:
            # 실패 원인 확인을 위해 장소검색 상태도 저장
            result["place_geo_status"] = place_result["geo_status"]
            result["place_error_msg"] = place_result["error_msg"]
    result["used_address_query"] = address
    result["used_place_query"] = place_query
    results.append(result)
    print(
        idx,
        "|",
        row["nm"],
        "|",
        address,
        "|",
        result["geo_status"],
        "|",
        result.get("road_address"),
        "|",
        result.get("lon"),
        result.get("lat")
    )
    # API 과다 호출 방지
    time.sleep(0.12)


# =====================================================
# 7. 결과 병합
# =====================================================
result_df = pd.DataFrame(results)
df_out = pd.concat(
    [
        df.reset_index(drop=True),
        result_df.reset_index(drop=True)
    ],
    axis=1
)


# =====================================================
# 8. 좌표 숫자형 변환
# =====================================================
df_out["lon"] = pd.to_numeric(df_out["lon"], errors="coerce")
df_out["lat"] = pd.to_numeric(df_out["lat"], errors="coerce")


# =====================================================
# 9. 최종 주소 컬럼 생성
# =====================================================
df_out["final_address"] = df_out["road_address"].fillna("")
df_out.loc[
    df_out["final_address"] == "",
    "final_address"
] = df_out["jibun_address"]
df_out.loc[
    df_out["final_address"].isna(),
    "final_address"
] = df_out["loc"]


# =====================================================
# 10. 검증용 컬럼
# =====================================================
df_out["has_xy"] = df_out[["lon", "lat"]].notna().all(axis=1)
# 원래 주소의 시군구 일부가 결과주소에 포함되는지 간단 확인용
df_out["address_match_simple"] = df_out.apply(
    lambda r: str(r["loc"])[:6] in str(r["final_address"]),
    axis=1
)


# =====================================================
# 11. 저장
# =====================================================
df_out.to_csv(output_path, index=False, encoding="utf-8-sig")
print("저장 완료:", output_path)
print()
print("상태별 개수:")
print(df_out["geo_status"].value_counts(dropna=False))
print()
print("좌표 확보 개수:")
print(df_out["has_xy"].sum(), "/", len(df_out))


# =====================================================
# 1. 경로 설정
# =====================================================
input_path = Path("C:/Users/lhj15/OneDrive/문서/[NH]/무더위한파쉼터/local_nh_2605_geocoded.csv")
output_path = Path("C:/Users/lhj15/OneDrive/문서/[NH]/무더위한파쉼터/local_branches.js")


# =====================================================
# 2. CSV 읽기
# =====================================================
df = pd.read_csv(input_path, encoding="utf-8-sig")
print("컬럼 확인:")
print(df.columns)
print(df.head())


# =====================================================
# 3. 좌표 숫자형 변환
# =====================================================
df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
# 좌표 없는 행 제거
df = df.dropna(subset=["lon", "lat"]).copy()


# =====================================================
# 4. 없는 컬럼이 있어도 에러 안 나게 준비
# =====================================================
for col in ["road_address", "jibun_address", "final_address", "phone", "place_url"]:
    if col not in df.columns:
        df[col] = ""
# final_address가 비어 있으면 loc 사용
if "loc" in df.columns:
    df["final_address"] = df["final_address"].fillna("")
    df.loc[df["final_address"] == "", "final_address"] = df["loc"]


# =====================================================
# 5. Worker에서 쓸 데이터 구조 만들기
# =====================================================
branch_df = pd.DataFrame({
    "name": df["nm"].fillna("").astype(str),
    "branch_name": df["nm"].fillna("").astype(str),
    "road_address": df["road_address"].fillna("").astype(str),
    "jibun_address": df["jibun_address"].fillna("").astype(str),
    "final_address": df["final_address"].fillna("").astype(str),
    "lon": df["lon"],
    "lat": df["lat"],
    "phone": df["phone"].fillna("").astype(str),
    "place_url": df["place_url"].fillna("").astype(str),
})


# =====================================================
# 6. JS 파일로 저장
# =====================================================
records = branch_df.to_dict(orient="records")
js_text = "const BRANCHES = "
js_text += json.dumps(records, ensure_ascii=False, indent=2)
js_text += ";\n"
output_path.write_text(js_text, encoding="utf-8")
print("저장 완료:", output_path)
print("농축협 개수:", len(records))
