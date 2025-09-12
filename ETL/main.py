import ETL.ingestion.crawl_homeshop as cr_hs
import ETL.ingestion.crawl_kok as cr_k
import ETL.preprocessing.preprocessing_hs as pr_hs
import ETL.preprocessing.preprocessing_kok as pr_k
import ETL.embedding.embedding as emb
import ETL.utils.utils as utils
from concurrent.futures import ProcessPoolExecutor, wait, ALL_COMPLETED
import multiprocessing
import traceback
import os
import sys
from ETL.classifying import fct_to_cls, predict_main
import time
import ETL.insert_recipe as insert_recipe
# ODS -> FCT -> VEC 테이블 생성
def create_tables():
    conn_o, cur_o = utils.con_to_maria_ods()
    conn_s, cur_s = utils.con_to_maria_service()

    # 함수 존재 여부 확인 및 생성
    cur_o.execute("""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_NAME = 'STR_TO_NUM'
        AND ROUTINE_TYPE = 'FUNCTION'
        AND ROUTINE_SCHEMA = 'ODS_DB';
    """)
    ods_func_check = cur_o.fetchone()[0]
    if ods_func_check == 0:
        stn_func_query = '''
            CREATE FUNCTION STR_TO_NUM(str VARCHAR(255))
            RETURNS FLOAT
            DETERMINISTIC
            BEGIN
            DECLARE only_num VARCHAR(255);
            DECLARE num_val DECIMAL(20,2);

            SET only_num = REGEXP_REPLACE(str, '[^0-9.]', '');

            IF only_num REGEXP '^[0-9]*\\.?[0-9]+$' THEN
                SET num_val = CAST(only_num AS DECIMAL(20,2));

                IF num_val >= 2147483647 THEN
                RETURN 0;
                ELSE
                RETURN num_val;
                END IF;
            ELSE
                RETURN NULL;
            END IF;
            END;
            '''
        cur_o.execute(stn_func_query)
        print('STR_TO_NUM 함수 생성')
    else:
        print('STR_TO_NUM 함수 확인')
    cur_o.close()
    conn_o.close()
    cur_s.close()
    conn_s.close()
    print('Creating_all_tables')
    try:
        cr_k.create_tables_ods_kok()
    except:
        print('Error in [ create_tables_ods_kok ]', traceback.format_exc())
    try:    
        cr_hs.create_ods_hs()
    except:
        print('Error in [ create_ods_hs ]', traceback.format_exc())
    try:
        pr_k.create_tables_kok_fct()
    except:
        print('Error in [ create_tables_kok_fct ]', traceback.format_exc())
    try:
        pr_hs.create_fct_homeshopping()
    except:
        print('Error in [ create_fct_homeshopping ]', traceback.format_exc())
    try:
        emb.create_vec_table()
    except:
        print('Error in [ create_vec_table ]', traceback.format_exc())        
    try:
        fct_to_cls.create_cls_tables()
    except:
        print('Error in [ create_cls_tables ]', traceback.format_exc())

# 콕 데이터 크롤링 및 ODS 적재
def cr_kok():
    try:
        cr_k.main()
    except:
        print('Error in [ crawl_kok_price ]', traceback.format_exc())
# 콕 데이터 전처리 및 FCT 적재
def pr_kok():
    try:
        pr_k.preprocess_kok()
    except:
        print('Error in [ preprocess_kok ]', traceback.format_exc())
# 콕 데이터 임베딩 벡터 생성 및 VEC 적재
def emb_kok():
    try:
        emb.kok_embed()
    except:
        print('Error in [ kok_embed ]', traceback.format_exc())        
# 홈앤쇼핑 크롤링
def cr_hns():
    try:
        cr_hs.crawl_hns()
    except:
        print('Error in [ crawl_hns ]', traceback.format_exc())
    try:        
        cr_hs.crawl_hns_detail(1)
    except:
        print('Error in [ crawl_hns_detail ]', traceback.format_exc())
# 현대홈쇼핑 크롤링
def cr_hd():
    try:
        cr_hs.crawl_hyundai()
    except:
        print('Error in [ crawl_hyundai ]', traceback.format_exc())
    for i in (2,3):
        try:
            cr_hs.crawl_hyundai_detail(i)
        except:
            print(f'Error in [ crawl_hyundai_detail {i} ]', traceback.format_exc())
# NS홈쇼핑 크롤링
def cr_ns():
    try:
        cr_hs.crawl_ns()
    except:
        print('Error in [ crawl_ns ]', traceback.format_exc())
    for i in (4,5):
        try:
            cr_hs.crawl_ns_detail(i)
        except:
            print(f'Error in [ crawl_ns_detail {i} ]', traceback.format_exc())
# 전체 홈쇼핑 데이터 전처리 및 FCT 적재
def pr_homeshop():
    try:
        pr_hs.main()
    except:
        print('Error in [ pr_homeshop_main ]', traceback.format_exc())

# 홈쇼핑 데이터 임베딩 벡터 생성 및 VEC 적재
def emb_homeshop():
    try:
        emb.homeshop_embed()
    except:
        print('Error in [ homeshop_embed ]', traceback.format_exc())  
# 홈쇼핑 식품/비식품 분류, 콕&홈쇼핑 식재료/완제품 분류
def pred_all():
    try:
        fct_to_cls.main()
        predict_main.main()
    except:
        print('Error in [ pred_all ]', traceback.format_exc())    

# 함수 병렬 실행
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "2"))

def _safe_submit(ex, fn, name):
    """작업 제출 + 이름 출력용 헬퍼."""
    print(f"[SUBMIT] {name}")
    return ex.submit(fn)

def main():
    whole_start = time.time()
    # Windows/리눅스 모두 안전하게
    if sys.platform.startswith("win"):
        multiprocessing.freeze_support()
    try:
        multiprocessing.set_start_method("spawn", force=True)  # 리눅스에서도 안전
    except RuntimeError:
        pass

    # 1) 테이블 생성 (메인 프로세스에서 1회)
    try:
        print("[INIT] Creating all tables...")
        create_tables()
        print("[INIT] done")
    except Exception:
        print("[INIT] FAILED:\n", traceback.format_exc())
        # 테이블 없으면 이후 단계가 실패할 수 있으니 여기서 종료할지 말지 선택
        return
    ods_kok_cnt1, ods_hs_cnt1, fct_kok_cnt1, fct_hs_cnt1, emb_cnt1, cls_cnt1 = utils.data_counting()
    # 2) 수집 단계: 4개 병렬
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        # 독립 크롤링 4개 제출
        cr_start = time.time()

        f_cr_ns  = _safe_submit(ex, cr_ns,  "cr_ns")
        f_cr_hd  = _safe_submit(ex, cr_hd,  "cr_hd")

        # 3) KOK 체인: cr_kok → pr_kok → emb_kok
        # cr_kok 완료 기다린 뒤 다음 단계 제출
        wait([f_cr_hd, f_cr_ns], return_when=ALL_COMPLETED)

        f_cr_kok = _safe_submit(ex, cr_kok, "cr_kok")
        f_cr_hns = _safe_submit(ex, cr_hns, "cr_hns")

        wait([f_cr_kok, f_cr_hns], return_when=ALL_COMPLETED)

        cr_end = time.time()
        cr_process_time = f'{cr_end - cr_start:.4f}'
        print(f"✅ [DONE] CRAWLING---------------- Process time : {cr_process_time}")
        
        pr_start = time.time()

        f_pr_hs   = _safe_submit(ex, pr_homeshop,   "pr_homeshop")
        f_pr_kok  = _safe_submit(ex, pr_kok,  "pr_kok")

        wait([f_pr_kok, f_pr_hs], return_when=ALL_COMPLETED)

        pr_end = time.time()
        pr_process_time = f'{pr_end - pr_start:.4f}'
        print(f"✅ [DONE] PREPROCESSING----------- Process time : {pr_process_time}")

        emb_start = time.time()

        f_emb_kok = _safe_submit(ex, emb_kok, "emb_kok")
        f_emb_hs  = _safe_submit(ex, emb_homeshop,  "emb_homeshop")

        wait([f_emb_kok, f_emb_hs], return_when=ALL_COMPLETED)

        emb_end = time.time()
        emb_process_time = f'{emb_end - emb_start:.4f}'
        print(f"✅ [DONE] EMBEDDING--------------- Process time : {emb_process_time}")

        pred_start = time.time()

        f_pred_all = _safe_submit(ex, pred_all,  "pred_all")

        wait([f_pred_all], return_when=ALL_COMPLETED)

        pred_end = time.time()
        pred_process_time = f'{pred_end - pred_start:.4f}'
        print(f"✅ [DONE] CLASSFYING-------------- Process time : {pred_process_time}")
    whole_end = time.time()
    whole_process_time = f'{whole_end - whole_start:.4f}'
    print(f"🎯 [DONE] PIPELINE FINISHED. PROCESS TIME : {whole_process_time} sec")
    print(f"  ⌚ CRAWLING PROCESS TIME      : {cr_process_time} sec")
    print(f"  ⌚ PREPROCESSING PROCESS TIME : {pr_process_time} sec")
    print(f"  ⌚ EMBEDDING PROCESS TIME     : {emb_process_time} sec")
    print(f"  ⌚ CLASSFYING PROCESS TIME    : {pred_process_time} sec")
    
    ods_kok_cnt2, ods_hs_cnt2, fct_kok_cnt2, fct_hs_cnt2, emb_cnt2, cls_cnt2 = utils.data_counting()
    ods_kok_cnt = ['cnt_ods_kok_price','cnt_ods_kok_product','cnt_ods_kok_image','cnt_ods_kok_detail','cnt_ods_kok_review']
    ods_hs_cnt = ['cnt_ods_hs_list','cnt_ods_hs_product','cnt_ods_hs_image','cnt_ods_hs_detail']
    fct_kok_cnt = ['cnt_fct_kok_price','cnt_fct_kok_product','cnt_fct_kok_image','cnt_fct_kok_detail','cnt_fct_kok_review']
    fct_hs_cnt = ['cnt_fct_hs_list','cnt_fct_hs_product','cnt_fct_hs_image','cnt_fct_hs_detail']
    emb_cnt = ['cnt_hs_vec','cnt_kok_vec']
    cls_cnt = ['cnt_kok_cls','cnt_hs_cls']

    print('🆗 CHECK CNT : ODS')
    utils.print_cnt(ods_kok_cnt, ods_kok_cnt1, ods_kok_cnt2)
    utils.print_cnt(ods_hs_cnt, ods_hs_cnt1, ods_hs_cnt2)
    print('🆗 CHECK CNT : FCT')
    utils.print_cnt(fct_kok_cnt, fct_kok_cnt1, fct_kok_cnt2)
    utils.print_cnt(fct_hs_cnt, fct_hs_cnt1, fct_hs_cnt2)
    print('🆗 CHECK CNT : EMB')
    utils.print_cnt(emb_cnt, emb_cnt1, emb_cnt2)
    print('🆗 CHECK CNT : CLS')
    utils.print_cnt(cls_cnt, cls_cnt1, cls_cnt2)

if __name__ == "__main__":
    insert_recipe.main()
    main()
