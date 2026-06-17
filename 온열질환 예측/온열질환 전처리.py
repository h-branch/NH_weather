from pathlib import Path
import pandas as pd 
import numpy as np
import re
import os

de_setter = set(["감시체계 자료해석 시 유의사항", "총괄"])

keys = set()

# 경로 주의: lab.ipynb/*.xls가 아니라 폴더/*.xls 형태여야 합니다.
folder = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\온열질환")

flist = sorted(
    list(folder.glob("*.xls")) +
    list(folder.glob("*.xlsx"))
)

# pandas에서 읽을 수 있도록 문자열 경로로 변환
flist = [str(f) for f in flist]

print("파일 개수:", len(flist))
print(flist[:5])

for f in flist:
    for sheet in pd.ExcelFile(f).sheet_names:
        if sheet not in de_setter:
            keys.add(sheet)

book = {k: pd.DataFrame() for k in keys}

for k in keys:
    for f in flist:
        if k in pd.ExcelFile(f).sheet_names:
            
            df = pd.read_excel(f, sheet_name=k)

            # =====================================================
            # 1. 병합셀 헤더 처리
            # =====================================================

            top_cols = (pd.Series(df.columns).astype(str).replace(r"^Unnamed.*", np.nan, regex=True).ffill().fillna("date").astype(str))
            sub_cols = (df.iloc[0].astype(str).str.replace(" ", "", regex=False).str.replace("\n", "", regex=False).str.replace("\r", "", regex=False))

            # 길이와 인덱스 문제 방지
            new_columns = []

            for i in range(len(df.columns)):
                if i == 0:
                    new_columns.append("date")
                else:
                    top = str(top_cols.iloc[i]).strip()
                    sub = str(sub_cols.iloc[i]).strip()
                    new_columns.append(f"{top}_{sub}")

            df.columns = new_columns

            # =====================================================
            # 2. 실제 자료 행만 남기기
            # =====================================================
            df = df[2:].copy()

            df.rename(columns={df.columns[0]: "date"}, inplace=True)

            # =====================================================
            # 3. 합계, 소계 제거
            # =====================================================
            df = df.loc[:, ~df.columns.str.startswith("합계")]
            df = df.loc[:, ~df.columns.str.contains("소계", na=False)]

            # =====================================================
            # 4. 날짜 정리
            # =====================================================
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df[df["date"].notna()].copy()

            # =====================================================
            # 5. 숫자형 변환
            # =====================================================
            for c in df.columns:
                if c != "date":
                    df[c] = (
                        df[c]
                        .astype(str)
                        .str.replace(",", "", regex=False)
                        .str.replace("-", "0", regex=False)
                        .str.strip()
                    )
                    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

            # =====================================================
            # 6. 환자수 + 추정 사망자수 합산
            # =====================================================
            new_df = pd.DataFrame()
            new_df["date"] = df["date"]

            sgg_list = sorted(
                set(
                    c.replace("_환자수", "").replace("_추정사망자수", "")
                    for c in df.columns
                    if c != "date"
                )
            )

            for sgg in sgg_list:
                patient_col = f"{sgg}_환자수"
                death_col = f"{sgg}_추정사망자수"

                patient = df[patient_col] if patient_col in df.columns else 0
                death = df[death_col] if death_col in df.columns else 0

                new_df[sgg] = patient + death

            df = new_df.copy()

            # =====================================================
            # 7. 컬럼 정렬
            # =====================================================
            cols = ["date"] + sorted([c for c in df.columns if c != "date"])
            df = df[cols]

            # =====================================================
            # 8. 강원도 명칭 통일
            # =====================================================
            _k_ = "강원특별자치도" if k == "강원도" else k

            if _k_ not in book:
                book[_k_] = pd.DataFrame()

            book[_k_] = pd.concat([book[_k_], df], ignore_index=True)

# 강원도 키가 있을 때만 제거
if "강원도" in book:
    book.pop("강원도")


for k in book.keys():
    print(k, book[k].shape) 
    book[k].set_index('date')
    book[k].drop_duplicates(inplace=True, keep='first', subset=['date'])
    book[k].sort_values(by='date', inplace=True)


for k in book.keys():
    cols = book[k].columns
    col_set = set()
    for c in cols:
        col_set.add(c[:2])
    if len(col_set) != len(cols):
        print(cols)


for k in book:
    for c in book[k].columns:
        if c != 'date':
            if book[k][c].sum() == 0:
                print(k, c)


# 저장할 폴더
out_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\온열질환")

# 폴더가 없으면 생성
out_dir.mkdir(parents=True, exist_ok=True)

for k, df in book.items():
    # 파일명에 들어가면 안 되는 문자 제거
    safe_name = re.sub(r'[\\/:*?"<>|]', "_", str(k))
    
    out_path = out_dir / f"{safe_name}.csv"
    
    df.to_csv(
        out_path,
        index=False,
        encoding="utf-8-sig"
    )
    
    print("저장 완료:", out_path.resolve())
    print("파일 존재 여부:", out_path.exists())


print("현재 작업 폴더:", os.getcwd())


# 저장 폴더
out_dir = Path(r"C:\Users\lhj15\OneDrive\문서\[NH]\온열질환")
out_dir.mkdir(parents=True, exist_ok=True)

long_list = []

for sido, df in book.items():
    temp = df.copy()

    # 시도 컬럼 추가
    temp["시도"] = sido

    # wide → long 변환
    temp_long = temp.melt(
        id_vars=["date", "시도"],
        var_name="시군구",
        value_name="patient_total"
    )

    long_list.append(temp_long)

# 전체 합치기
df_all = pd.concat(long_list, ignore_index=True)

# 날짜 정리
df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce")
df_all = df_all[df_all["date"].notna()].copy()

# 환자수 숫자형 정리
df_all["patient_total"] = pd.to_numeric(
    df_all["patient_total"],
    errors="coerce"
).fillna(0).astype(int)

# 정렬
df_all = df_all.sort_values(
    ["date", "시도", "시군구"]
).reset_index(drop=True)

# 저장
out_path = out_dir / "온열질환자_시군구_일별_전체.csv"

df_all.to_csv(
    out_path,
    index=False,
    encoding="utf-8-sig"
)

print("저장 완료:", out_path)
print("파일 존재 여부:", out_path.exists())


df = df_all.copy()

df["date"] = pd.to_datetime(df["date"])
df["year"] = df["date"].dt.year

# 발생 여부
df["occurred"] = (df["patient_total"] >= 1).astype(int)

print("전체 행 수:", len(df))
print("환자 발생 행 수:", df["occurred"].sum())
print("환자 미발생 행 수:", (df["occurred"] == 0).sum())
print("발생 비율:", df["occurred"].mean())


year_summary = (
    df.groupby("year")
    .agg(
        total_rows=("patient_total", "size"),
        event_rows=("occurred", "sum"),
        total_patients=("patient_total", "sum"),
        event_rate=("occurred", "mean")
    )
    .reset_index()
)

print(year_summary)
