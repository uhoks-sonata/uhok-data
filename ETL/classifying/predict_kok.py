# -*- coding: utf-8 -*-
"""
KOK_CLASSIFY: CLS_ING이 NULL인 행을 배치로 예측/업데이트
- 학습 파일: classifying/train_cls_kok_model.py 에서 저장한 .joblib 파이프라인 사용
- 예측 라벨: 1=식재료, 0=완제품
"""
import os
import re
import argparse
import numpy as np
import pandas as pd
import joblib
from typing import List, Tuple
import ETL.utils.utils as utils
from sklearn.base import BaseEstimator, TransformerMixin
from scipy.sparse import csr_matrix

# 학습 때 사용한 키워드(훈련 스크립트와 동일해야 함)
finished_product_keywords = [
    "라면","컵라면","즉석밥","도시락","김밥","피자","햄버거","샌드위치",
    "과자","스낵","초콜릿","사탕","껌","케이크","빵","도넛","베이커리",
    "음료","주스","커피","차","아이스크림","빙수","유산균","요구르트","분유","홍삼","생식","치킨","치즈볼",
    "핫도그","가공육","조리완료","레토르트","HMR","즉석","레디밀","국탕","양념갈비","새우장","쿠키","양갱",
    "송편","수프","전병","약과","젤리","오란다","쉐이크","즉석국","즉석탕","즉석요리","냉면","쫄면","막국수",
    "떡갈비","밀키트","백숙","삼계탕","닭발","한과","볶음밥","떡볶이","족발","영양바","육포","장아찌","원두",
    "메밀소바","쉐프","셰프",
    "%탕","%전골","%국","%죽","%게장","%볶음","%무침","%말이","%구이","%구운","%인분","%곰탕",
    "%양념","%칩","%떡","양념%","%국밥","%불고기","%전","%찌개","%찐","%볶음밥",
]
ingredient_keywords = [
    "쌀","콩","보리","밀가루","전분","고춧가루","참기름","간장","된장","고추장",
    "소금","설탕","양념","조미료","다시마","멸치","건새우","다시팩","원두","커피원두","만두",
    "야채","채소","고사리","버섯","시금치","나물","마늘","양파","감자","고구마",
    "소고기","돼지고기","닭고기","정육","계란","달걀","두부","생선","굴비","장어","오징어",
    "새우","낙지","홍합","꽃게","해물","육수","국물","원재료","곡물","견과","깨",
    "오렌지","곶감","절단","코인","가래떡","망고","동치미","피스타치오","레몬즙","차돌박이","김치",
]

def split_finished_keywords():
    suffix = [kw[1:] for kw in finished_product_keywords if kw.startswith("%") and len(kw) > 1]
    base = [kw for kw in finished_product_keywords if not kw.startswith("%")]
    return base, suffix

def token_suffix_match(text, suffixes):
    if not text:
        return False
    tokens = text.split()
    strip_chars = ".,)/)]}»”’\"'"
    for t in tokens:
        tt = t.rstrip(strip_chars)
        for suf in suffixes:
            if tt.endswith(suf):
                return True
    return False

class keywordfeature(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.base_finished, self.suffix_finished = split_finished_keywords()
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        rows = []
        for raw in X:
            s = normalize(raw)  # <- predict_kok.py에 이미 있는 normalize 사용
            finished_hit = sum(s.count(kw) for kw in self.base_finished) + int(token_suffix_match(s, self.suffix_finished))
            ingredient_hit = sum(s.count(kw) for kw in ingredient_keywords)
            any_finished = int(finished_hit > 0)
            any_ingredient = int(ingredient_hit > 0)
            conflict = int(any_finished and any_ingredient)
            score = float(finished_hit - ingredient_hit)
            rows.append([
                finished_hit, ingredient_hit, any_finished, any_ingredient,
                conflict, score, len(s.split()), len(s)
            ])
        return csr_matrix(np.array(rows, dtype=float))
    
def normalize(s: str) -> str:
    s = "" if s is None else str(s)
    return re.sub(r"\s+", " ", s.strip())

def fetch_rows(cur, query: str, batch_size: int) -> pd.DataFrame:
    cur.execute(query + f" LIMIT {batch_size}")
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

def update_many_numeric(cur, table: str, col: str, pairs: List[Tuple[int, int]], id_col: str = "KOK_PRODUCT_ID"):
    if not pairs:
        return
    ids = ",".join(str(int(pid)) for _, pid in pairs)
    case_expr = " ".join([f"WHEN {int(pid)} THEN {int(val)}" for val, pid in pairs])
    sql = f"""
        UPDATE {table}
        SET {col} = CASE {id_col} {case_expr} END
        WHERE {id_col} IN ({ids})
    """
    cur.execute(sql)

def to_int_labels(y_pred) -> np.ndarray:
    out = []
    for v in y_pred:
        s = str(v).strip()
        if s == "식재료": out.append(1)
        elif s == "완제품": out.append(0)
        else:
            try: out.append(int(v))
            except: out.append(0)
    return np.array(out, dtype=int)

def run(model_path: str, table: str, batch_size: int, dry_run: bool):
    if not os.path.isfile(model_path):
        raise FileNotFoundError(f"모델 파일이 없습니다: {model_path}")

    # keywordfeature가 pickle에 포함되어 있어 import 경로가 필요합니다.
    # -> 모델 저장한 모듈(classifying.train_cls_kok_model)이 같은 패키지에 있어야 함.
    pipe = joblib.load(model_path)
    if not hasattr(pipe, "predict"):
        raise ValueError("로드한 객체가 sklearn 파이프라인이 아닙니다.")

    conn, cur = utils.con_to_maria_service()
    try:
        total_upd = 0
        while True:
            df = fetch_rows(cur, f"""
                SELECT PRODUCT_ID, PRODUCT_NAME
                FROM {table}
                WHERE CLS_ING IS NULL
                  AND PRODUCT_NAME IS NOT NULL
                ORDER BY PRODUCT_ID
            """, batch_size)
            if df.empty:
                break

            texts = df["PRODUCT_NAME"].astype(str).map(normalize).tolist()
            pred = pipe.predict(texts)
            y_int = to_int_labels(pred)

            pairs = [(int(y_int[i]), int(df.iloc[i]["PRODUCT_ID"])) for i in range(len(df))]
            print(f"[KOK] [ING] batch {len(df)} rows → set CLS_ING")

            if not dry_run:
                update_many_numeric(cur, table, "CLS_ING", pairs, id_col="PRODUCT_ID")
                conn.commit()
            total_upd += len(pairs)

        print(f"[KOK] [ING] 총 업데이트: {total_upd} 행")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="KOK_CLASSIFY")
    ap.add_argument("--model", default=os.path.join(BASE_DIR, "artifacts", "kok_finished_vs_ingredient.joblib"))
    ap.add_argument("--batch-size", type=int, default=5000)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    run(args.model, args.table, args.batch_size, args.dry_run)