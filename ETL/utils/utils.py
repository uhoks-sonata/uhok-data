import pymysql
import psycopg2
import sys
import os
from typing import Union, Dict, Any
import re
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from urllib.parse import urlsplit, unquote, parse_qsl, urlparse

load_dotenv()

def replace_nan_with_none(df: pd.DataFrame) -> pd.DataFrame:
    """
    DataFrame 내 NaN/NaT 값을 전부 None으로 바꿔줌 (DB insert 안전화).
    """
    return df.where(pd.notna(df), None)

# def parse_dsn(dsn):
#     """일반 DSN 문자열을 dict로 변환"""
#     url = urlparse(dsn)
#     return {
#         "user": url.username,
#         "password": url.password,
#         "host": url.hostname,
#         "port": url.port,
#         "database": url.path.lstrip("/"),
#     }
def parse_dsn(dsn: Union[str, bytes], *, default_port: int | None = None) -> Dict[str, Any]:
    """
    DSN(URL) 문자열을 안전하게 파싱해서 연결용 dict로 변환.
    - bytes/str 모두 지원
    - mysql/mariadb 기본 포트 자동 설정
    - 퍼센트 인코딩 해제 (user / password / database)
    - 쿼리스트링 파라미터(params) 보존
    - unix_socket 지원 (?unix_socket=... 또는 ?socket=...)
    - 스킴 생략한 'user:pass@host:port/db' 형태도 허용
    """
    if dsn is None:
        raise ValueError("DSN is None")

    if isinstance(dsn, (bytes, bytearray)):
        dsn = dsn.decode("utf-8")  # ★ bytes 방어

    if not isinstance(dsn, str):
        raise TypeError(f"DSN must be str or bytes, got {type(dsn)!r}")

    url = urlsplit(dsn)

    # 스킴이 없어 netloc이 비어있고 path에 '@'가 보이면, URL 재해석 (user:pass@host:port/db)
    if not url.netloc and url.path and "@" in url.path:
        url = urlsplit("dummy://" + dsn)

    scheme = (url.scheme or None)

    username = unquote(url.username or "")
    password = unquote(url.password or "")
    host = url.hostname or None

    # 기본 포트: mysql/mariadb면 3306, 아니면 호출자가 주는 default_port
    port = url.port or default_port
    if port is None and scheme:
        if scheme.startswith(("mysql", "mariadb")):
            port = 3306
        elif scheme.startswith(("postgres", "pg", "postgresql")):
            port = 5432

    # DB 이름: '/db'에서 선두 '/' 제거 및 퍼센트 디코딩
    raw_path = url.path or ""
    database = unquote(raw_path.lstrip("/")) or None
    if database and "/" in database:   # '/db/extra' 같은 경우 첫 세그먼트만
        database = database.split("/", 1)[0]

    # 쿼리스트링 파라미터 보존
    params = dict(parse_qsl(url.query, keep_blank_values=True))
    unix_socket = params.get("unix_socket") or params.get("socket")

    result: Dict[str, Any] = {
        "scheme": scheme,
        "user": username or None,
        "password": password or None,
        "host": host,
        "port": port,
        "database": database,
        "params": params,
    }
    if unix_socket:
        result["unix_socket"] = unix_socket

    # 최소 필수값 체크(소켓 사용 안 하면 host 필요)
    if not result.get("host") and not unix_socket:
        raise ValueError("DSN must include host or unix_socket")

    return result

def con_to_maria_service():
    try:
        # 예: mysql+pymysql://user:pass@192.168.101.55:3306/SERVICE_DB
        dsn = os.getenv("MARIADB_SERVICE_URL")
        config = parse_dsn(dsn)  # host, port, user, password, database 를 반환해야 함

        conn = pymysql.connect(
            host=config["host"],
            port=int(config.get("port", 3306)),
            user=config["user"],
            password=config.get("password", ""),
            database=config.get("database", ""),
            charset="utf8mb4",
            autocommit=True,
        )
        cur = conn.cursor()
        # 문자셋/콜레이션 고정 (원 코드와 동일한 효과)
        cur.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")
        return conn, cur

    except pymysql.MySQLError as e:
        print(f"Error connecting to MariaDB SERVICE_DB: {e}")
        sys.exit(1)

def con_to_maria_ods():
    try:
        dsn = os.getenv("MARIADB_ODS_URL")
        config = parse_dsn(dsn)

        conn = pymysql.connect(
            host=config["host"],
            port=int(config.get("port", 3306)),
            user=config["user"],
            password=config.get("password", ""),
            database=config.get("database", ""),
            charset="utf8mb4",
            autocommit=True,
        )
        cur = conn.cursor()
        cur.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")
        return conn, cur

    except pymysql.MySQLError as e:
        print(f"Error connecting to MariaDB ODS_DB: {e}")
        sys.exit(1)

def con_to_maria_auth():
    try:
        # 예: mysql+pymysql://user:pass@192.168.101.55:3306/SERVICE_DB
        dsn = os.getenv("MARIADB_AUTH_URL")
        config = parse_dsn(dsn)

        conn = pymysql.connect(
            host=config["host"],
            port=int(config.get("port", 3306)),
            user=config["user"],
            password=config.get("password", ""),
            database=config.get("database", ""),
            charset="utf8mb4",
            autocommit=True,
        )
        cur = conn.cursor()
        # 문자셋/콜레이션 고정 (원 코드와 동일한 효과)
        cur.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")
        return conn, cur

    except pymysql.MySQLError as e:
        print(f"Error connecting to MariaDB AUTH_DB: {e}")
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
    else:
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

# 테이블 수리 함수
def deal_with_corruption():
    conn_o, cur_o = con_to_maria_ods()
    conn_s, cur_s = con_to_maria_service()
    conn_a, cur_a = con_to_maria_auth()

    cur_list = [cur_o,cur_s,cur_a]

    for cur in cur_list:
        cur.execute('''SHOW TABLES;''')
        rows = cur.fetchall()
        table_list = [row[0] for row in rows]
        for table in table_list:
            new_table = f'{table}_NEW'
            # try:
            #     cur.execute(f"""CREATE TABLE IF NOT EXISTS {new_table} LIKE {table}""")
            #     print(f'create table {new_table}')
            # except Exception as e:
            #     print(f"[ERROR IN CREAT]: {e}")

            # try:
            #     cur.execute(f"""
            #         INSERT IGNORE INTO {new_table} SELECT * FROM {table}""")
            # except Exception as e:
            #     print(f"[ERROR IN INSERT]: {e}")

            try:
                cur.execute(f"""RENAME TABLE `{table}` TO `{table}_OLD`,
                                `{new_table}` TO `{table}`""")
            except Exception as e:
                print(f"[ERROR IN RENAME]: {e}")
    cur_o.close()
    cur_s.close()
    cur_a.close()
    conn_o.close()
    conn_s.close()
    conn_a.close()