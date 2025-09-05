import mariadb
import psycopg2
import sys
import os
from typing import Any
import re
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()

def replace_nan_with_none(df: pd.DataFrame) -> pd.DataFrame:
    """
    DataFrame 내 NaN/NaT 값을 전부 None으로 바꿔줌 (DB insert 안전화).
    """
    return df.where(pd.notna(df), None)

def parse_dsn(dsn):
    """일반 DSN 문자열을 dict로 변환"""
    url = urlparse(dsn)
    return {
        "user": url.username,
        "password": url.password,
        "host": url.hostname,
        "port": url.port,
        "database": url.path.lstrip("/"),
    }

def con_to_maria_service():
    try:
        config = parse_dsn(os.getenv("MARIADB_SERVICE_URL"))
        conn = mariadb.connect(**config, autocommit=True)
        cur = conn.cursor()
        return conn, cur
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB SERVICE_DB: {e}")
        sys.exit(1)

def con_to_maria_ods():
    try:
        config = parse_dsn(os.getenv("MARIADB_ODS_URL"))
        conn = mariadb.connect(**config, autocommit=True)
        cur = conn.cursor()
        return conn, cur
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB ODS_DB: {e}")
        sys.exit(1)

def con_to_psql(db_name):
    try:
        dsn = os.getenv("POSTGRES_URL") + f'{db_name}'
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        return conn, cur
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        sys.exit(1)

def insert_df_into_db(conn, df, table_name: str, IGNORE = ""):
    if IGNORE == 'IGNORE':
        ig_query = "INSERT IGNORE INTO "
    else:
        ig_query = "INSERT INTO "
        
    df = df.astype(object)
    df = df.where(pd.notna(df), None)
    col_list = list(df.columns)
    placeholders = ', '.join(['%s'] * len(col_list))
    columns_sql = '"' + '", "'.join(map(str, col_list)) + '"'

    if conn.__class__.__module__.startswith("psycopg2"):
        columns_sql = '"' + '", "'.join(map(str, col_list)) + '"'
        query = f'"{table_name}" ({columns_sql}) VALUES ({placeholders})'
    elif conn.__class__.__module__.startswith("mariadb"):
        columns_sql = ','.join(map(str, col_list))
        query = f'{table_name} ({columns_sql}) VALUES ({placeholders})'

    total_query = ig_query + query

    cur = conn.cursor()

    for _, row in df.iterrows():
        try:
            values = [row[col] for col in col_list]
            cur.execute(total_query, values)
        except Exception as e:
            if table_name == 'ODS_KOK_PRICE_INFO':
                pass
            else:
                print(f"❌ Insert error on row {row.to_dict()}: {e}")

    conn.commit()
    cur.close()

def _clean_text(s: Any) -> str | None:
    if not s:  # None, "", 0, False 모두 걸러짐
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s or None

def _num_only(x: str | None) -> int | None:
    if not x: 
        return None
    s = re.sub(r'[^0-9]', '', x)
    return int(s) if s else None

def _https(url: str | None) -> str | None:
    if not url: 
        return None
    return 'https:' + url if url.startswith('//') else url

def def_dataframe():
    df_product = pd.DataFrame(columns=[
            "PRODUCT_ID",
            "HOMESHOPPING_ID",
            "STORE_NAME",
            "PRODUCT_NAME",
            "SALE_PRICE",
            "DC_RATE",
            "DC_PRICE",
            "DELIVERY_FEE",
            "DELIVERY_CO",
            "RETURN_EXCHANGE",
            "TERM"])
    df_img = pd.DataFrame(columns=[
            'PRODUCT_ID', 
            'SORT_ORDER', 
            'IMG_URL'])
    df_detail = pd.DataFrame(columns=[
            'PRODUCT_ID', 
            'DETAIL_COL', 
            'DETAIL_VAL'])
    return df_product, df_img, df_detail

def datetime_to_time(a):
    dt = datetime.strptime(a, "%Y-%m-%d %H:%M:%S")
    time_part = dt.strftime("%H:%M")
    return time_part

def sanitize_for_db(df: pd.DataFrame) -> pd.DataFrame:
    # DB에 넣기 직전: 전 컬럼을 object로 바꾸고, NaN/NaT/pd.NA를 None으로 통일
    out = df.copy()
    out = out.astype(object)
    out = out.where(pd.notna(out), None)
    return out

def str_to_num(s: str) -> float | None:
    """
    문자열에서 숫자/소수점만 추출해 float으로 변환.
    - 유효하지 않으면 None 반환
    - 2147483647 이상이면 0 반환
    """
    if s is None:
        return None
    
    # 숫자와 소수점만 남기기
    only_num = re.sub(r'[^0-9.]', '', s)

    # 숫자 패턴 확인
    if re.fullmatch(r'[0-9]*\.?[0-9]+', only_num):
        try:
            num_val = float(only_num)
        except ValueError:
            return None
        
        if num_val >= 2147483647:
            return 0
        else:
            return num_val
    else:
        return None
    
def data_counting():
    conn_o, cur_o = con_to_maria_ods()
    conn_s, cur_s = con_to_maria_service()
    conn_r, cur_r = con_to_psql("REC_DB")
    # ODS_KOK COUNTING
    def ods_kok_cnt():
        cur_o.execute('''SELECT COUNT(*) FROM ODS_KOK_PRICE_INFO''')
        cnt_ods_kok_price = cur_o.fetchall()[0][0]
        cur_o.execute('''SELECT COUNT(*) FROM ODS_KOK_PRODUCT_INFO''')
        cnt_ods_kok_product = cur_o.fetchall()[0][0]
        cur_o.execute('''SELECT COUNT(*) FROM ODS_KOK_IMAGE_INFO''')
        cnt_ods_kok_image = cur_o.fetchall()[0][0]
        cur_o.execute('''SELECT COUNT(*) FROM ODS_KOK_DETAIL_INFO''')
        cnt_ods_kok_detail = cur_o.fetchall()[0][0]
        cur_o.execute('''SELECT COUNT(*) FROM ODS_KOK_REVIEW_EXAMPLE''')
        cnt_ods_kok_review = cur_o.fetchall()[0][0]
        return [cnt_ods_kok_price if cnt_ods_kok_price else 0,
                cnt_ods_kok_product if cnt_ods_kok_product else 0,
                cnt_ods_kok_image if cnt_ods_kok_image else 0,
                cnt_ods_kok_detail if cnt_ods_kok_detail else 0,
                cnt_ods_kok_review if cnt_ods_kok_review else 0]
    # ODS_HOMESHOP COUNTING
    def ods_hs_cnt():
        cur_o.execute('''SELECT COUNT(*) FROM ODS_HOMESHOPPING_LIST''')
        cnt_ods_hs_list = cur_o.fetchall()[0][0]
        cur_o.execute('''SELECT COUNT(*) FROM ODS_HOMESHOPPING_PRODUCT_INFO''')
        cnt_ods_hs_product = cur_o.fetchall()[0][0]
        cur_o.execute('''SELECT COUNT(*) FROM ODS_HOMESHOPPING_IMG_URL''')
        cnt_ods_hs_image = cur_o.fetchall()[0][0]
        cur_o.execute('''SELECT COUNT(*) FROM ODS_HOMESHOPPING_DETAIL_INFO''')
        cnt_ods_hs_detail = cur_o.fetchall()[0][0]
        return [cnt_ods_hs_list if cnt_ods_hs_list else 0,
                cnt_ods_hs_product if cnt_ods_hs_product else 0,
                cnt_ods_hs_image if cnt_ods_hs_image else 0,
                cnt_ods_hs_detail if cnt_ods_hs_detail else 0]
    # FCT_KOK COUNTING
    def fct_kok_cnt():
        cur_s.execute('''SELECT COUNT(*) FROM FCT_KOK_PRICE_INFO''')
        cnt_fct_kok_price = cur_s.fetchall()[0][0]
        cur_s.execute('''SELECT COUNT(*) FROM FCT_KOK_PRODUCT_INFO''')
        cnt_fct_kok_product = cur_s.fetchall()[0][0]
        cur_s.execute('''SELECT COUNT(*) FROM FCT_KOK_IMAGE_INFO''')
        cnt_fct_kok_image = cur_s.fetchall()[0][0]
        cur_s.execute('''SELECT COUNT(*) FROM FCT_KOK_DETAIL_INFO''')
        cnt_fct_kok_detail = cur_s.fetchall()[0][0]
        cur_s.execute('''SELECT COUNT(*) FROM FCT_KOK_REVIEW_EXAMPLE''')
        cnt_fct_kok_review = cur_s.fetchall()[0][0]
        return [cnt_fct_kok_price if cnt_fct_kok_price else 0,
                cnt_fct_kok_product if cnt_fct_kok_product else 0,
                cnt_fct_kok_image if cnt_fct_kok_image else 0,
                cnt_fct_kok_detail if cnt_fct_kok_detail else 0,
                cnt_fct_kok_review if cnt_fct_kok_review else 0]
    # FCT_HOMESHOP COUNTING
    def fct_hs_cnt():
        cur_s.execute('''SELECT COUNT(*) FROM FCT_HOMESHOPPING_LIST''')
        cnt_fct_hs_list = cur_s.fetchall()[0][0]
        cur_s.execute('''SELECT COUNT(*) FROM FCT_HOMESHOPPING_PRODUCT_INFO''')
        cnt_fct_hs_product = cur_s.fetchall()[0][0]
        cur_s.execute('''SELECT COUNT(*) FROM FCT_HOMESHOPPING_IMG_URL''')
        cnt_fct_hs_image = cur_s.fetchall()[0][0]
        cur_s.execute('''SELECT COUNT(*) FROM FCT_HOMESHOPPING_DETAIL_INFO''')
        cnt_fct_hs_detail = cur_s.fetchall()[0][0]
        return [cnt_fct_hs_list if cnt_fct_hs_list else 0,
                cnt_fct_hs_product if cnt_fct_hs_product else 0,
                cnt_fct_hs_image if cnt_fct_hs_image else 0,
                cnt_fct_hs_detail if cnt_fct_hs_detail else 0]
    # EMB COUNTING
    def emb_cnt():
        cur_r.execute('''SELECT COUNT(*) FROM "HOMESHOPPING_VECTOR_TABLE";''')
        cnt_hs_vec = cur_r.fetchall()[0][0]
        cur_r.execute('''SELECT COUNT(*) FROM "KOK_VECTOR_TABLE";''')
        cnt_kok_vec = cur_r.fetchall()[0][0]
        return [cnt_hs_vec if cnt_hs_vec else 0,
                cnt_kok_vec if cnt_kok_vec else 0]
    # CLS COUNTING
    def cls_cnt():
        cur_s.execute('''SELECT COUNT(*) FROM KOK_CLASSIFY;''')
        cnt_kok_cls = cur_s.fetchall()[0][0]
        cur_s.execute('''SELECT COUNT(*) FROM HOMESHOPPING_CLASSIFY;''')
        cnt_hs_cls = cur_s.fetchall()[0][0]
        return [cnt_kok_cls if cnt_kok_cls else 0,
                cnt_hs_cls if cnt_hs_cls else 0]
    
    try:
        return ods_kok_cnt(), ods_hs_cnt(), fct_kok_cnt(), fct_hs_cnt(), emb_cnt(), cls_cnt()
    except Exception as e:
        print('Error in counting', e)
    finally:
        cur_o.close()
        cur_s.close()
        cur_r.close()
        conn_o.close()
        conn_s.close()
        conn_r.close()

def print_cnt( col_list : list, list1 : list, list2 : list):
    for i in range(len(col_list)):
        print(f"  📋 {col_list[i]} | CHANGE : {list1[i]} ➡  {list2[i]} | DIFF : {list2[i] - list1[i]}")