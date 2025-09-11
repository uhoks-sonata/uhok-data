# -*- coding: utf-8 -*-
"""
KOK_CLASSIFY용 완제품/식재료 분류 모델 학습
- 규칙 의사라벨 + TF-IDF(word/char) + 키워드 피처 + LinearSVC + Calibrated(proba)
- CLS_ING(1=식재료, 0=완제품) 수동라벨이 있으면 우선 사용, 없으면 규칙라벨로 씨앗 생성
- Self-training: unlabeled에서 고신뢰만 채택 후 재학습
"""
import re
import os
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
import ETL.utils.utils as utils

# --------------------- 키워드 ---------------------
finished_product_keywords = [
    "라면","컵라면","즉석밥","도시락","김밥","피자","햄버거","샌드위치",
    "과자","스낵","초콜릿","사탕","껌","케이크","빵","도넛","베이커리",
    "음료","주스","커피","차","아이스크림","빙수","유산균","요구르트","분유","홍삼","생식","치킨","치즈볼",
    "핫도그","가공육","조리완료","레토르트","HMR","즉석","레디밀","국탕","양념갈비","새우장","쿠키","양갱",
    "송편","수프","전병","약과","젤리","오란다","쉐이크","즉석국","즉석탕","즉석요리","냉면","쫄면","막국수",
    "떡갈비","밀키트","백숙","삼계탕","닭발","한과","볶음밥","떡볶이","족발","영양바","육포","장아찌","원두",
    "메밀소바","쉐프","셰프","보충제","프로틴",
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

# --------------------- 유틸/규칙 ---------------------
def normalize(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.strip()
    return re.sub(r"\s+", " ", s)

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
            rows.append([finished_hit, ingredient_hit, any_finished, any_ingredient, conflict, score, len(s.split()), len(s)])
        return csr_matrix(np.array(rows, dtype=float))

def build_feature_union():
    word_vec = TfidfVectorizer(ngram_range=(1,2), token_pattern=r"(?u)\b\w+\b")
    char_vec = TfidfVectorizer(analyzer="char", ngram_range=(3,5))
    return FeatureUnion([("word", word_vec), ("char", char_vec), ("kw", keywordfeature())])

def build_calibrated_svm():
    base = LinearSVC(class_weight="balanced", max_iter=20000, random_state=42)
    try:
        return CalibratedClassifierCV(estimator=base, method="sigmoid", cv=3)
    except TypeError:
        return CalibratedClassifierCV(base_estimator=base, method="sigmoid", cv=3)

# --------------------- DB 로드 ---------------------
def load_kok_from_db(table: str = "KOK_CLASSIFY") -> pd.DataFrame:
    conn, cur = utils.con_to_maria_service()
    try:
        cur.execute(f"""
            SELECT PRODUCT_ID, PRODUCT_NAME, CLS_ING
            FROM {table}
            WHERE PRODUCT_NAME IS NOT NULL
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
    df = load_kok_from_db(args.table)
    if "PRODUCT_NAME" not in df.columns:
        raise ValueError("컬럼 'PRODUCT_NAME'이 필요합니다.")
    df["__TEXT__"] = df["PRODUCT_NAME"].astype(str).map(normalize)

    # 수동라벨(CLS_ING) 매핑
    def map_cls_ing(v):
        if pd.isna(v): return None
        try:
            vi = int(v)
            return "식재료" if vi == 1 else "완제품"
        except Exception:
            s = str(v).strip().lower()
            if s in {"1","true","t","y","ing","ingredient"}: return "식재료"
            if s in {"0","false","f","n","fin","finished","product"}: return "완제품"
            return None

    df["MANUAL_LABEL"] = df.get("CLS_ING").map(map_cls_ing) if "CLS_ING" in df.columns else None
    df["RULE_LABEL"]   = df["__TEXT__"].map(rule_label)
    df["SEED_LABEL"]   = df["MANUAL_LABEL"].fillna(df["RULE_LABEL"])

    seed = df.dropna(subset=["SEED_LABEL"]).copy()
    unlabeled = df[df["SEED_LABEL"].isna()].copy()
    if seed.empty:
        raise ValueError("씨앗 라벨이 없습니다. 규칙/키워드를 보강하거나 CLS_ING 일부를 채워주세요.")

    print(f"총 {len(df):,} / seed {len(seed):,} / unlabeled {len(unlabeled):,}")
    print("Seed 분포:", seed["SEED_LABEL"].value_counts().to_dict())

    X_seed = seed["__TEXT__"].tolist()
    y_seed = seed["SEED_LABEL"].tolist()

    pipe = Pipeline([("features", build_feature_union()), ("clf", build_calibrated_svm())])
    X_tr, X_te, y_tr, y_te = train_test_split(X_seed, y_seed, test_size=args.test_size, random_state=42, stratify=y_seed)
    pipe.fit(X_tr, y_tr)
    y_pred = pipe.predict(X_te)
    print("\n[Seed hold-out] report\n", classification_report(y_te, y_pred, digits=3))
    print("Confusion matrix\n", confusion_matrix(y_te, y_pred))

    if len(unlabeled) > 0:
        proba = pipe.predict_proba(unlabeled["__TEXT__"].tolist())
        pred  = pipe.predict(unlabeled["__TEXT__"].tolist())
        conf  = proba.max(axis=1)
        unlabeled["PRED_LABEL"] = pred
        unlabeled["CONF"] = conf
        selected = unlabeled[unlabeled["CONF"] >= args.conf_threshold].copy()
        print(f"\n▶ unlabeled {len(unlabeled):,} 중 고신뢰(≥{args.conf_threshold}) 채택: {len(selected):,}")
        X_final = X_seed + selected["__TEXT__"].tolist()
        y_final = y_seed + selected["PRED_LABEL"].tolist()
    else:
        print("\n▶ unlabeled 없음 → seed만으로 최종학습")
        X_final, y_final = X_seed, y_seed

    final_pipe = Pipeline([("features", build_feature_union()), ("clf", build_calibrated_svm())])
    final_pipe.fit(X_final, y_final)

    os.makedirs(os.path.dirname(args.out_model), exist_ok=True)
    joblib.dump(final_pipe, args.out_model)
    print(f"\n[저장 완료] {os.path.abspath(args.out_model)}")

    if args.out_labeled:
        proba_all = final_pipe.predict_proba(df["__TEXT__"].tolist())
        pred_all  = final_pipe.predict(df["__TEXT__"].tolist())
        conf_all  = proba_all.max(axis=1)
        out = df.copy()
        out["FINAL_PRED"] = pred_all
        out["CONF"] = conf_all
        out["LABEL_SOURCE"] = np.where(out["MANUAL_LABEL"].notna(),"manual",
                               np.where(out["RULE_LABEL"].notna(),"rule","pseudo"))
        out_cols = [c for c in ["PRODUCT_ID","PRODUCT_NAME","CLS_ING"] if c in out.columns]
        out[out_cols + ["FINAL_PRED","CONF","LABEL_SOURCE"]].to_csv(args.out_labeled, index=False, encoding="utf-8-sig")
        print(f"[라벨 CSV 저장] {os.path.abspath(args.out_labeled)}")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="KOK_CLASSIFY")
    ap.add_argument("--out-model", default=os.path.join(BASE_DIR, "artifacts", "kok_finished_vs_ingredient.joblib"))
    ap.add_argument("--test-size", type=float, default=0.2)
    ap.add_argument("--conf-threshold", type=float, default=0.7)
    ap.add_argument("--out-labeled", default="")
    args = ap.parse_args()
    main(args)