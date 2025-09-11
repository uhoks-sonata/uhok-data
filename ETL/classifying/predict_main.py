# classifying/predict_main.py
# 통합 예측 실행기: HS → KOK 순으로 DB 업데이트
import os
import sys
import argparse
import subprocess
from pathlib import Path

def run(cmd):
    print("\n>>", " ".join(cmd))
    subprocess.run(cmd, check=True)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--hs-table", default="HOMESHOPPING_CLASSIFY")
    p.add_argument("--kok-table", default="KOK_CLASSIFY")
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--skip-hs", action="store_true")
    p.add_argument("--skip-kok", action="store_true")
    args = p.parse_args()

    base_dir = Path(__file__).resolve().parent
    py = sys.executable
    art = base_dir / "artifacts"

    # paths
    hs_ing_joblib = art / "finished_vs_ingredient_linear_svc.joblib"
    kok_joblib    = art / "kok_finished_vs_ingredient.joblib"

    # 1) HS 예측 (FOOD → ING)
    if not args.skip_hs:
        cmd = [
            py, "-m", "ETL.classifying.predict_hs",
            "--table", args.hs_table,
            "--food-art-dir", str(art),
            "--ing-model", str(hs_ing_joblib),
            "--batch-size", str(args.batch_size),
        ]
        if args.dry_run:
            cmd.append("--dry-run")
        run(cmd)

    # 2) KOK 예측 (ING)
    if not args.skip_kok:
        cmd = [
            py, "-m", "ETL.classifying.predict_kok",
            "--table", args.kok_table,
            "--model", str(kok_joblib),
            "--batch-size", str(args.batch_size),
        ]
        if args.dry_run:
            cmd.append("--dry-run")
        run(cmd)

    print("\n[SUCCESS] 예측/업데이트 완료")

if __name__ == "__main__":
    main()