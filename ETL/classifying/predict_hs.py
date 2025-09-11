# -*- coding: utf-8 -*-
import os, re, json, joblib, argparse
import numpy as np
import pandas as pd
from typing import List, Tuple, Any, Dict, Optional, Callable
from scipy.sparse import hstack, csr_matrix
from sklearn.base import BaseEstimator, TransformerMixin
import ETL.utils.utils as utils
# 학습 때 사용한 키워드 
finished_product_keywords = [
    "라면","컵라면","즉석밥","도시락","김밥","피자","햄버거","샌드위치",
    "과자","스낵","초콜릿","사탕","껌","케이크","빵","도넛","베이커리",
    "음료","주스","커피","차","아이스크림","빙수","유산균","요구르트","분유","홍삼",
    "핫도그","가공육","조리완료","레토르트","HMR","즉석","레디밀",
    "즉석국","즉석탕","즉석찌개","즉석요리","냉면","세트","쫄면",
    "%탕","%전골","%국","%죽","%게장","%볶음",
]
ingredient_keywords = [
    "쌀","콩","보리","밀가루","전분","고춧가루","참기름","간장","된장","고추장",
    "소금","설탕","양념","조미료","다시마","멸치","건새우","다시팩","원두","커피원두",
    "야채","채소","고사리","버섯","시금치","나물","마늘","양파","감자","고구마",
    "소고기","돼지고기","닭고기","정육","계란","달걀","두부","생선","굴비","장어","오징어",
    "새우","낙지","홍합","꽃게","해물","육수","국물","원재료","곡물","견과","깨",
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
            s = str(raw).strip()
            s = re.sub(r"\s+", " ", s)
            finished_hit = sum(s.count(kw) for kw in self.base_finished) + int(token_suffix_match(s, self.suffix_finished))
            ingredient_hit = sum(s.count(kw) for kw in ingredient_keywords)
            any_finished = int(finished_hit > 0)
            any_ingredient = int(ingredient_hit > 0)
            conflict = int(any_finished and any_ingredient)
            score = float(finished_hit - ingredient_hit)
            num_tokens = len(s.split())
            num_chars = len(s)
            rows.append([
                finished_hit, ingredient_hit,
                any_finished, any_ingredient,
                conflict, score,
                num_tokens, num_chars
            ])
        return csr_matrix(np.array(rows, dtype=float))
# ============== 공통 전처리/부가피처 (food-개별 pkl 폴백용) ==============
def normalize(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.strip()
    return re.sub(r"\s+", " ", s)

def keyword_feats(names: List[str], food_keywords: List[str], notfood_keywords: List[str]) -> csr_matrix:
    is_food, cnt_food, is_not, cnt_not = [], [], [], []
    for t in names:
        f = sum(kw in t for kw in food_keywords)
        n = sum(kw in t for kw in notfood_keywords)
        is_food.append(int(f > 0))
        cnt_food.append(f)
        is_not.append(int(n > 0))
        cnt_not.append(n)
    extra = np.column_stack([
        is_food, cnt_food, is_not, cnt_not,
        np.array(cnt_food) - np.array(cnt_not),
        (np.array(is_food) & np.array(is_not)).astype(int)
    ]).astype(float)
    return csr_matrix(extra)

# ============== 모델 로더 ==============
def load_food_predictor(
    food_model_path: str,
    food_art_dir: str
) -> Callable[[List[str]], Tuple[np.ndarray, Optional[np.ndarray]]]:
    """
    반환: predict_fn(texts) -> (pred_labels_int_0_1, confidences_or_None)
    1 = 식품, 0 = 비식품
    """
    # 1) .joblib 파이프라인 시도
    if os.path.isfile(food_model_path):
        obj = joblib.load(food_model_path)
        # 파이프라인/추정기 그대로 사용
        if hasattr(obj, "predict"):
            pipe: BaseEstimator = obj
            def _pred(texts: List[str]):
                texts_n = [normalize(t) for t in texts]
                y_pred = pipe.predict(texts_n)
                # y_pred가 문자열/불리언일 수도 있으므로 0/1로 정규화
                y_int = []
                for v in y_pred:
                    try:
                        y_int.append(int(v))
                    except Exception:
                        s = str(v).strip().lower()
                        y_int.append(1 if s in {"식품","1","true","t","y"} else 0)
                y_int = np.array(y_int, dtype=int)
                conf = pipe.predict_proba(texts_n).max(axis=1) if hasattr(pipe, "predict_proba") else None
                return y_int, conf
            return _pred
        # 번들(dict) 케이스 (우리가 제안한 저장 포맷)
        if isinstance(obj, dict) and "model" in obj and "word_vec" in obj and "char_vec" in obj:
            model = obj["model"]
            word_vec = obj["word_vec"]
            char_vec = obj["char_vec"]
            food_keywords = obj.get("food_keywords", [])
            notfood_keywords = obj.get("notfood_keywords", [])
            def _pred(texts: List[str]):
                texts_n = [normalize(t) for t in texts]
                Xw = word_vec.transform(texts_n)
                Xc = char_vec.transform(texts_n)
                Xkw = keyword_feats(texts_n, food_keywords, notfood_keywords)
                X = hstack([Xw, Xc, Xkw])
                y = model.predict(X)
                y_int = np.array([int(v) if str(v).isdigit() else (1 if str(v).strip()=="식품" else 0) for v in y], dtype=int)
                conf = model.predict_proba(X).max(axis=1) if hasattr(model, "predict_proba") else None
                return y_int, conf
            return _pred

    # 2) 개별 pkl 폴백 (tfidf_word.pkl / tfidf_char.pkl / linear_svm_calibrated.pkl / keyword_meta.json)
    word_pkl = os.path.join(food_art_dir, "tfidf_word.pkl")
    char_pkl = os.path.join(food_art_dir, "tfidf_char.pkl")
    model_pkl = os.path.join(food_art_dir, "linear_svm_calibrated.pkl")
    meta_json = os.path.join(food_art_dir, "keyword_meta.json")
    if not (os.path.isfile(word_pkl) and os.path.isfile(char_pkl) and os.path.isfile(model_pkl) and os.path.isfile(meta_json)):
        raise FileNotFoundError(
            f"푸드 모델/아티팩트가 없습니다. joblib({food_model_path}) 또는 pkl 디렉토리({food_art_dir})를 확인하세요."
        )
    word_vec = joblib.load(word_pkl)
    char_vec = joblib.load(char_pkl)
    model = joblib.load(model_pkl)
    with open(meta_json, encoding="utf-8") as f:
        meta = json.load(f)
    food_keywords = meta["food_keywords"]
    notfood_keywords = meta["notfood_keywords"]

    def _pred(texts: List[str]):
        texts_n = [normalize(t) for t in texts]
        Xw = word_vec.transform(texts_n)
        Xc = char_vec.transform(texts_n)
        Xkw = keyword_feats(texts_n, food_keywords, notfood_keywords)
        X = hstack([Xw, Xc, Xkw])
        y = model.predict(X)
        y_int = np.array([int(v) if str(v).isdigit() else (1 if str(v).strip()=="식품" else 0) for v in y], dtype=int)
        conf = model.predict_proba(X).max(axis=1) if hasattr(model, "predict_proba") else None
        return y_int, conf

    return _pred

def load_ing_predictor(ing_model_path: str) -> Callable[[List[str]], Tuple[np.ndarray, Optional[np.ndarray]]]:
    """
    반환: predict_fn(texts) -> (pred_labels_int_0_1, confidences_or_None)
    1 = 식재료, 0 = 완제품
    (ing 모델은 우리가 파이프라인 .joblib로 저장한 걸 가정)
    """
    if not os.path.isfile(ing_model_path):
        raise FileNotFoundError(f"ING 모델(.joblib)이 없습니다: {ing_model_path}")
    pipe = joblib.load(ing_model_path)
    if not hasattr(pipe, "predict"):
        raise ValueError("ING 모델이 sklearn 파이프라인/추정기가 아닙니다.")
    def _pred(texts: List[str]):
        texts_n = [normalize(t) for t in texts]
        y = pipe.predict(texts_n)
        # 문자열 라벨("식재료"/"완제품") → 1/0
        y_int = []
        for v in y:
            s = str(v).strip()
            if s == "식재료":
                y_int.append(1)
            elif s == "완제품":
                y_int.append(0)
            else:
                # 혹시 숫자로 학습됐다면
                try: y_int.append(int(v))
                except: y_int.append(0)
        y_int = np.array(y_int, dtype=int)
        conf = pipe.predict_proba(texts_n).max(axis=1) if hasattr(pipe, "predict_proba") else None
        return y_int, conf
    return _pred

# ============== DB IO ==============
def fetch_rows(cur, query: str, batch_size: int) -> pd.DataFrame:
    cur.execute(query + f" LIMIT {batch_size}")
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)
    

def update_many_numeric(cur, table: str, col: str, pairs: List[Tuple[int, int]], id_col: str = "PRODUCT_ID"):
    """
    pairs: [(value, product_id), ...]
    마리아DB 커서의 플레이스홀더가 구현체에 따라 다를 수 있어 정수만 문자열 포맷으로 안전 처리.
    """
    if not pairs:
        return
    # 정수만 업데이트하므로 직접 포맷 (SQL 인젝션 위험 없음: 둘 다 int 캐스팅)
    ids = ",".join(str(int(pid)) for _, pid in pairs)
    # 임시 테이블 사용 없이 CASE WHEN으로 한 번에 갱신
    case_expr = " ".join([f"WHEN {int(pid)} THEN {int(val)}" for val, pid in pairs])
    sql = f"""
        UPDATE {table}
        SET {col} = CASE {id_col} {case_expr} END
        WHERE {id_col} IN ({ids})
    """
    cur.execute(sql)

# ============== 메인 파이프라인 ==============
def run(food_model: str, food_art_dir: str, ing_model: str, table: str, batch_size: int, dry_run: bool):
    conn, cur = utils.con_to_maria_service()
    try:
        # 1) 모델 로드
        food_predict = load_food_predictor(food_model, food_art_dir)
        ing_predict  = load_ing_predictor(ing_model)

        # 2) CLS_FOOD NULL → 예측/업데이트
        total_food_upd = 0
        while True:
            df = fetch_rows(cur, f"""
                SELECT PRODUCT_ID, PRODUCT_NAME
                FROM {table}
                WHERE CLS_FOOD IS NULL AND PRODUCT_NAME IS NOT NULL
                ORDER BY PRODUCT_ID
            """, batch_size)
            if df.empty:
                break
            texts = df["PRODUCT_NAME"].astype(str).map(normalize).tolist()
            y_pred, conf = food_predict(texts)  # 1=식품, 0=비식품
            pairs = [(int(y_pred[i]), int(df.iloc[i]["PRODUCT_ID"])) for i in range(len(df))]
            print(f"[HS] [FOOD] batch {len(df)} rows → set CLS_FOOD")
            if not dry_run:
                update_many_numeric(cur, table, "CLS_FOOD", pairs, id_col="PRODUCT_ID")
                conn.commit()
            total_food_upd += len(pairs)
        print(f"[HS] [FOOD] 총 업데이트: {total_food_upd} 행")

        # 3) CLS_FOOD=1 AND CLS_ING NULL → 예측/업데이트
        total_ing_upd = 0
        while True:
            df = fetch_rows(cur, f"""
                SELECT PRODUCT_ID, PRODUCT_NAME
                FROM {table}
                WHERE CLS_FOOD = 1 AND CLS_ING IS NULL
                  AND PRODUCT_NAME IS NOT NULL
                ORDER BY PRODUCT_ID
            """, batch_size)
            if df.empty:
                break
            texts = df["PRODUCT_NAME"].astype(str).map(normalize).tolist()
            y_pred, conf = ing_predict(texts)   # 1=식재료, 0=완제품
            pairs = [(int(y_pred[i]), int(df.iloc[i]["PRODUCT_ID"])) for i in range(len(df))]
            print(f"[HS] [ING] batch {len(df)} rows → set CLS_ING")
            if not dry_run:
                update_many_numeric(cur, table, "CLS_ING", pairs, id_col="PRODUCT_ID")
                conn.commit()
            total_ing_upd += len(pairs)
        print(f"[HS] [ING] 총 업데이트: {total_ing_upd} 행")

    finally:
        cur.close()
        conn.close()

# ============== CLI ==============
if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DEFAULT_TABLE = "HOMESHOPPING_CLASSIFY"
    DEFAULT_ART = os.path.join(BASE_DIR, "artifacts")
    DEFAULT_FOOD_JOBLIB = os.path.join(DEFAULT_ART, "cls_food_linear_svm.joblib")  # 있으면 사용
    DEFAULT_ING_JOBLIB  = os.path.join(DEFAULT_ART, "finished_vs_ingredient_linear_svc.joblib")

    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default=DEFAULT_TABLE)
    ap.add_argument("--food-model", default=DEFAULT_FOOD_JOBLIB, help=".joblib 파이프라인(없으면 --food-art-dir 폴백)")
    ap.add_argument("--food-art-dir", default=DEFAULT_ART, help="tfidf_word/char/model pkl + keyword_meta.json 경로")
    ap.add_argument("--ing-model", default=DEFAULT_ING_JOBLIB, help="완제품/식재료 .joblib 파이프라인 경로")
    ap.add_argument("--batch-size", type=int, default=5000)
    ap.add_argument("--dry-run", action="store_true", help="실제 UPDATE/COMMIT 없이 시뮬레이션")
    args = ap.parse_args()

    run(
        food_model=args.food_model,
        food_art_dir=args.food_art_dir,
        ing_model=args.ing_model,
        table=args.table,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )