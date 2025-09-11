# ing_train_linear_svm.py
# -*- coding: utf-8 -*-

import re
import argparse
import numpy as np
import pandas as pd
from typing import List, Tuple
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.calibration import CalibratedClassifierCV
from sklearn.svm import LinearSVC
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
import joblib
import os
import ETL.utils.utils as utils

# --------------------- 키워드 피처(사전 정의) ---------------------
finished_product_keywords = [
    "라면","컵라면","즉석밥","도시락","김밥","피자","햄버거","샌드위치",
    "과자","스낵","초콜릿","사탕","껌","케이크","빵","도넛","베이커리",
    "음료","주스","커피","차","아이스크림","빙수","유산균","요구르트","분유","홍삼",
    "핫도그","가공육","조리완료","레토르트","HMR","즉석","레디밀",
    "즉석국","즉석탕","즉석찌개","즉석요리","냉면","세트","쫄면",
    "%탕","%전골","%국","%죽","%게장","%볶음","레자몽",
]

ingredient_keywords = [
    "쌀","콩","보리","밀가루","전분","고춧가루","참기름","간장","된장","고추장",
    "소금","설탕","양념","조미료","다시마","멸치","건새우","다시팩","원두","커피원두",
    "야채","채소","고사리","버섯","시금치","나물","마늘","양파","감자","고구마",
    "소고기","돼지고기","닭고기","정육","계란","달걀","두부","생선","굴비","장어","오징어",
    "새우","낙지","홍합","꽃게","해물","육수","국물","원재료","곡물","견과","깨",
]

# --------------------- 유틸 ---------------------
def normalize(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s

def split_finished_keywords() -> Tuple[List[str], List[str]]:
    suffix = [kw[1:] for kw in finished_product_keywords if kw.startswith("%") and len(kw) > 1]
    base = [kw for kw in finished_product_keywords if not kw.startswith("%")]
    return base, suffix

def token_suffix_match(text: str, suffixes: List[str]) -> bool:
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

def rule_label(name: str) -> str | None:
    s = normalize(name)
    base_finished, suffix_finished = split_finished_keywords()
    if any(kw in s for kw in base_finished) or token_suffix_match(s, suffix_finished):
        return "완제품"
    if any(kw in s for kw in ingredient_keywords):
        return "식재료"
    return None

# --------------------- 부가 피처 ---------------------
class keywordfeature(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.base_finished, self.suffix_finished = split_finished_keywords()
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        rows = []
        for raw in X:
            s = normalize(raw)
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

def build_feature_union():
    word_vec = TfidfVectorizer(ngram_range=(1,2), token_pattern=r"(?u)\b\w+\b", dtype=np.float32)
    char_vec = TfidfVectorizer(analyzer="char", ngram_range=(3,5), dtype=np.float32)
    return FeatureUnion([("word", word_vec), ("char", char_vec), ("kw", keywordfeature())])

def build_calibrated_svm():
    base = LinearSVC(class_weight="balanced", max_iter=8000, random_state=42)
    try:
        return CalibratedClassifierCV(estimator=base, method="sigmoid", cv=3)
    except TypeError:
        return CalibratedClassifierCV(base_estimator=base, method="sigmoid", cv=3)

def load_food_from_db():
    """
    식품만 로드: CLS_FOOD=1 AND PRODUCT_NAME NOT NULL
    + (가능하면 CLS_ING 같이 가져와 수동라벨 씨앗으로 사용)
    """
    conn, cur = utils.con_to_maria_service()
    try:
        cur.execute("""
            SELECT PRODUCT_ID, PRODUCT_NAME, CLS_FOOD, CLS_ING
            FROM HOMESHOPPING_CLASSIFY
            WHERE PRODUCT_NAME IS NOT NULL
              AND CLS_FOOD = 1
        """)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()
        conn.close()
    return df

# --------------------- 메인 ---------------------
def main(args):
    # 0) 데이터 로드
    df = load_food_from_db()
    if "PRODUCT_NAME" not in df.columns:
        raise ValueError("컬럼 'PRODUCT_NAME'이 필요합니다.")
    food_df = df.copy()
    food_df["__TEXT__"] = food_df["PRODUCT_NAME"].fillna("").astype(str).map(normalize)

    print(f"DB 로드: 식품 행 수 = {len(food_df):,}")

    # 1) 수동 라벨(CLS_ING)을 씨앗으로 사용: 1=식재료, 0=완제품, 그 외/NULL -> None
    def map_cls_ing(v):
        if pd.isna(v):
            return None
        try:
            vi = int(v)
            return "식재료" if vi == 1 else "완제품"
        except Exception:
            s = str(v).strip().lower()
            if s in {"1", "true", "t", "y", "ingredient", "ing"}:
                return "식재료"
            if s in {"0", "false", "f", "n", "finished", "prod"}:
                return "완제품"
            return None

    food_df["MANUAL_LABEL"] = food_df.get("CLS_ING").map(map_cls_ing) if "CLS_ING" in food_df.columns else None

    # 2) 규칙 의사라벨
    food_df["RULE_LABEL"] = food_df["__TEXT__"].map(rule_label)

    # 3) 씨앗 라벨 = 수동 라벨 우선, 없으면 규칙 라벨 사용
    food_df["SEED_LABEL"] = food_df["MANUAL_LABEL"].fillna(food_df["RULE_LABEL"])

    seed = food_df.dropna(subset=["SEED_LABEL"]).copy()
    unlabeled = food_df[food_df["SEED_LABEL"].isna()].copy()

    if seed.empty:
        raise ValueError("씨앗 라벨이 없습니다. CLS_ING을 채우거나 규칙 키워드를 보강하세요.")

    print("Seed 분포:", seed["SEED_LABEL"].value_counts().to_dict())
    print("규칙 미매칭(수동라벨도 없는) 개수:", len(unlabeled))

    X_seed = seed["__TEXT__"].tolist()
    y_seed = seed["SEED_LABEL"].tolist()

    feats = build_feature_union()
    clf = build_calibrated_svm()
    pipe = Pipeline([("features", feats), ("clf", clf)])

    # 4) 씨앗 내부 홀드아웃 평가
    X_train, X_test, y_train, y_test = train_test_split(
        X_seed, y_seed, test_size=args.test_size, random_state=42, stratify=y_seed
    )
    pipe.fit(X_train, y_train)
    y_pred = pipe.predict(X_test)
    print("\n[Seed hold-out] Classification report\n", classification_report(y_test, y_pred, digits=3))
    print("Confusion matrix\n", confusion_matrix(y_test, y_pred))

    # 5) 미라벨에 대한 예측 -> 고신뢰만 채택해 증강
    if len(unlabeled) > 0:
        proba = pipe.predict_proba(unlabeled["__TEXT__"].tolist())
        pred  = pipe.predict(unlabeled["__TEXT__"].tolist())
        conf  = proba.max(axis=1)
        unlabeled["PRED_LABEL"] = pred
        unlabeled["CONF"] = conf
        selected = unlabeled[unlabeled["CONF"] >= args.conf_threshold].copy()
        print(f"\n▶ 미라벨 {len(unlabeled):,} 중 고신뢰(≥{args.conf_threshold}) 채택: {len(selected):,}")
        X_final = X_seed + selected["__TEXT__"].tolist()
        y_final = y_seed + selected["PRED_LABEL"].tolist()
    else:
        print("\n미라벨 없음 → 씨앗만으로 최종 학습")
        X_final, y_final = X_seed, y_seed

    # 6) 최종 학습(전체 피처+분류기 파이프라인)
    final_pipe = Pipeline([("features", build_feature_union()), ("clf", build_calibrated_svm())])
    final_pipe.fit(X_final, y_final)

    # 7) 모델 저장
    os.makedirs(os.path.dirname(args.out_model), exist_ok=True)
    joblib.dump(final_pipe, args.out_model)
    print(f"\n[저장 완료] {args.out_model}")

    # # 8) (선택) 결과 CSV 저장
    # if args.out_labeled:
    #     proba_all = final_pipe.predict_proba(food_df["__TEXT__"].tolist())
    #     pred_all  = final_pipe.predict(food_df["__TEXT__"].tolist())
    #     conf_all  = proba_all.max(axis=1)

    #     food_df["FINAL_PRED"] = pred_all
    #     food_df["CONF"] = conf_all
    #     food_df["LABEL_SOURCE"] = np.where(
    #         food_df["MANUAL_LABEL"].notna(), "manual",
    #         np.where(food_df["RULE_LABEL"].notna(), "rule",
    #                  np.where(food_df["CONF"] >= args.conf_threshold, "pseudo_highconf", "pseudo_lowconf"))
    #     )

    #     base_cols = [c for c in [
    #         "PRODUCT_ID","PRODUCT_NAME","CLS_FOOD","CLS_ING"
    #     ] if c in food_df.columns]
    #     save_df = food_df[base_cols + ["MANUAL_LABEL","RULE_LABEL","SEED_LABEL","FINAL_PRED","CONF","LABEL_SOURCE"]].copy()
    #     os.makedirs(os.path.dirname(args.out_labeled), exist_ok=True)
    #     save_df.to_csv(args.out_labeled, index=False, encoding="utf-8-sig")
    #     print(f"[라벨 덧붙인 CSV 저장 완료] {args.out_labeled}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DEFAULT_MODEL = os.path.join(BASE_DIR, "artifacts", "finished_vs_ingredient_linear_svc.joblib")
    # DEFAULT_OUT = os.path.join(BASE_DIR, "artifacts", "ing_labeled.csv")

    parser.add_argument("--out-model", default=DEFAULT_MODEL)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--conf-threshold", type=float, default=0.8)
    # parser.add_argument("--out-labeled", default=DEFAULT_OUT)
    args = parser.parse_args()
    main(args)