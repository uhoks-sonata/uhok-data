# classifying/train_main.py
# 통합 학습 실행기: HS(FOOD), HS(ING), KOK(ING) 순차 학습
import os
import sys
import subprocess
from pathlib import Path

def run(cmd):
    print("\n>>", " ".join(cmd))
    subprocess.run(cmd, check=True)

def main():
    base_dir = Path(__file__).resolve().parent
    py = sys.executable
    art = base_dir / "artifacts"
    art.mkdir(parents=True, exist_ok=True)

    # 1) HS: FOOD (out_dir 사용)
    run([
        py, "-m", "classifying.train_cls_hs_food_model",
        "--text_col", "PRODUCT_NAME",
        "--label_col", "CLS_FOOD",
        "--out_dir", str(art)
    ])

    # 2) HS: ING (.joblib 저장)
    hs_ing_joblib = art / "finished_vs_ingredient_linear_svc.joblib"
    run([
        py, "-m", "classifying.train_cls_hs_ingr_model",
        "--out-model", str(hs_ing_joblib)
    ])

    # 3) KOK: ING (.joblib 저장)
    kok_joblib = art / "kok_finished_vs_ingredient.joblib"
    run([
        py, "-m", "classifying.train_cls_kok_model",
        "--out-model", str(kok_joblib)
    ])

    print("\n[SUCCESS] 모든 학습 완료")
    print(f"- HS FOOD artifacts dir : {art}")
    print(f"- HS ING joblib         : {hs_ing_joblib}")
    print(f"- KOK ING joblib        : {kok_joblib}")

if __name__ == "__main__":
    main()