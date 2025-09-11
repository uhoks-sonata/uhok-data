import ETL.utils.utils as utils
import re, json, joblib, argparse, numpy as np, pandas as pd
from scipy.sparse import hstack, csr_matrix, issparse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC
from sklearn.calibration import CalibratedClassifierCV
import os

# 키워드 사전 정의
food_keywords = [
    "쌀","김치","라면","즉석","밥","국","탕","찌개","반찬","떡","과자","간식","소스","조미료","양념","김",
    "생선","정육","고기","햄","어묵","육포","우유","치즈","계란","달걀","두부","요구르트","커피","차","음료",
    "주스","홍삼","분유","유산균","곱창","블루베리","냉면","육수","유기농","아이스크림","다시마","인분",
    "두유","빙수","오곡","단팥","훈제","장어","멸치","해물","다시팩","엑스트라버진","엑스트라 버진","양갱",
    "만두","풀무원","올가","피자","고춧가루","핫도그","수산물","오징어","도넛","자일리톨","자숙","구이",
    "손질","아구찜","코코넛오일","복분자","닭발","매콤","젓","명란","슬라이스","낙지","숙성","고사리","문어",
    "데친","셰프","소갈비","굴비",
    ]
notfood_keywords = [
        "기능성","접이식","유산균","앰플","찜기","셋업","토너","알로에","순금","세라믹","프라이팬","쌀통","쿠션",
        "LG","삼성","우산","조리기","푸마","크로커다일","브라","제약","마스크","아디다스","드로즈","트렁크",
        "글루타치온","팬티","용기","립스틱","밍크","팬츠","보험","냉장고","약품","프로틴","루테인","데비마이어",
        "혈압","혈당","기억력","개월분","날씬","밥솥","24K","18K","한국금자산관리", "거래소", "투어",
        "이불","타파웨어","웨어","하의","상의","제조기","버버리","크로스백","무스탕","재킷", "집업","차량",
        "행주","티슈","키트",
    ]
# 유틸
def normalize(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s

def keyword_feats(names):
    is_food, cnt_food, is_not, cnt_not = [], [], [], []
    for t in names:
        f = sum(kw in t for kw in food_keywords)
        n = sum(kw in t for kw in notfood_keywords)
        is_food.append(int(f > 0)) # 식품 키워드 존재 여부
        cnt_food.append(f) # 식품 키워드 개수
        is_not.append(int(n > 0)) # 비식품 키워드 존재 여부
        cnt_not.append(n) # 비식품 키워드 개수
    extra = np.column_stack([
        is_food, cnt_food, is_not, cnt_not, 
        np.array(cnt_food) - np.array(cnt_not), # 식품 비식품 키둬드 개수 차이
        (np.array(is_food) & np.array(is_not)).astype(int)
    ]).astype(float)
    return csr_matrix(extra)
# DB에서 데이터 호출
def load_from_db():
    conn_s, cur_s = utils.con_to_maria_service()
    cur_s.execute("SELECT * FROM HOMESHOPPING_CLASSIFY WHERE CLS_FOOD IS NOT NULL")
    cols = [desc[0] for desc in cur_s.description]  # 컬럼명 가져오기
    rows = cur_s.fetchall()
    cur_s.close()
    conn_s.close()
    return pd.DataFrame(rows, columns=cols)
# 훈련 정의
def training_process(args):
    # 1. DB 데이터 로드
    df = load_from_db()
    texts = df[args.text_col].astype(str).map(normalize).tolist()
 # ✅ 라벨 생성: 숫자/문자 모두 대응
    lbl = df[args.label_col]
    if pd.api.types.is_numeric_dtype(lbl):
        y = (pd.to_numeric(lbl, errors="coerce").fillna(0) > 0).astype(int).values
    else:
        s = lbl.astype(str).str.strip().str.lower()
        y = s.isin({"식품", "1", "true", "t", "y"}).astype(int).values

    # 분포 점검 + 단일 클래스 방지
    n_pos = int((y == 1).sum())
    n_neg = int((y == 0).sum())
    print(f"Label counts -> pos(1): {n_pos}, neg(0): {n_neg}")
    if n_pos == 0 or n_neg == 0:
        raise ValueError("두 클래스가 모두 있어야 학습 가능합니다. CLS_FOOD 값을 확인하세요.")
    word_vec = TfidfVectorizer(analyzer="word", ngram_range=(1,2), min_df=2, max_features=120_000)
    char_vec = TfidfVectorizer(analyzer="char", ngram_range=(2,5), min_df=2, max_features=120_000)
    Xw = word_vec.fit_transform(texts)
    Xc = char_vec.fit_transform(texts)

    # 3. 키워드 부가 특성
    Xkw = keyword_feats(texts)

    # 4. 전체 특성 합치기
    X = hstack([Xw, Xc, Xkw]) if issparse(Xw) else np.hstack([Xw, Xc, Xkw.A])

    # 5. Linear SVM 학습 + 확률보정
    base = LinearSVC(C=1.0, class_weight="balanced", random_state=42)
    model = CalibratedClassifierCV(estimator=base, method="sigmoid", cv=3)
    model.fit(X,y)

    # 6. 결과 저장
    os.makedirs(args.out_dir, exist_ok = True)
    joblib.dump(word_vec, args.out_dir + "/tfidf_word.pkl")
    joblib.dump(char_vec, args.out_dir + "/tfidf_char.pkl")
    joblib.dump(model, args.out_dir + "/linear_svm_calibrated.pkl")
    with open(args.out_dir + "/keyword_meta.json", "w", encoding="utf-8") as f:
        json.dump({"food_keywords" : food_keywords, "notfood_keywords" : notfood_keywords}, f, ensure_ascii=False, indent=2)
    
    print("Save artifacts to:", args.out_dir)
# 훈련 진행 코드
def train_cls_food():
    p = argparse.ArgumentParser()
    p.add_argument("--text_col", default="PRODUCT_NAME")  # 상품명 컬럼
    p.add_argument("--label_col", default="CLS_FOOD")     # 식품/비식품 분류 컬럼
    # 모델 저장 경로 설정
    BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
    DEFAULT_OUT_DIR = os.path.join(BASE_DIR, "artifacts") 
    p.add_argument("--out_dir", default=DEFAULT_OUT_DIR)  
    args = p.parse_args()
    training_process(args)

if __name__ == "__main__":
    train_cls_food()