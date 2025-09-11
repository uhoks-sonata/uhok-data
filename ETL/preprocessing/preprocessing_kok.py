import sys
import ETL.utils.utils as utils
import pandas as pd
import numpy as np

def create_tables_kok_fct(): # CREATE FCT TABLES IN SERVICE_DB
    # FCT 테이블 생성 쿼리
    queries = [
    '''
        CREATE TABLE IF NOT EXISTS FCT_KOK_PRODUCT_INFO (
            KOK_PRODUCT_ID INT PRIMARY KEY,
            KOK_STORE_NAME	VARCHAR(100),
            KOK_PRODUCT_NAME	VARCHAR(300),
            KOK_THUMBNAIL	TEXT,
            KOK_PRODUCT_PRICE	INT,
            KOK_REVIEW_SCORE	FLOAT,
            KOK_REVIEW_CNT	INT,
            KOK_5_RATIO	INT,
            KOK_4_RATIO	INT,
            KOK_3_RATIO	INT,
            KOK_2_RATIO	INT,
            KOK_1_RATIO	INT,
            KOK_ASPECT_PRICE	VARCHAR(30),
            KOK_ASPECT_PRICE_RATIO	INT,
            KOK_ASPECT_DELIVERY	VARCHAR(30),
            KOK_ASPECT_DELIVERY_RATIO	INT,
            KOK_ASPECT_TASTE	VARCHAR(30),
            KOK_ASPECT_TASTE_RATIO	INT,
            KOK_SELLER	VARCHAR(100),
            KOK_CO_CEO	VARCHAR(100),
            KOK_CO_REG_NO	VARCHAR(50),
            KOK_CO_EC_REG	VARCHAR(50),
            KOK_TELL	VARCHAR(50),
            KOK_VER_ITEM	VARCHAR(50),
            KOK_VER_DATE	VARCHAR(50),
            KOK_CO_ADDR	VARCHAR(200),
            KOK_RETURN_ADDR	VARCHAR(200),
            KOK_EXCHANGE_ADDR	VARCHAR(200)
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS FCT_KOK_IMAGE_INFO (
            KOK_IMG_ID INT PRIMARY KEY,
            KOK_PRODUCT_ID	INT,
            KOK_IMG_URL	TEXT,
            FOREIGN KEY (KOK_PRODUCT_ID) REFERENCES FCT_KOK_PRODUCT_INFO (KOK_PRODUCT_ID) ON DELETE CASCADE,
            UNIQUE KEY UNIQ_PROD_COL (KOK_PRODUCT_ID, KOK_IMG_URL)
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS FCT_KOK_DETAIL_INFO (
            KOK_DETAIL_COL_ID INT PRIMARY KEY,
            KOK_PRODUCT_ID	INT,
            KOK_DETAIL_COL	TEXT,
            KOK_DETAIL_VAL	TEXT,
            FOREIGN KEY (KOK_PRODUCT_ID) REFERENCES FCT_KOK_PRODUCT_INFO (KOK_PRODUCT_ID) ON DELETE CASCADE,
            UNIQUE KEY UNIQ_PROD_COL (KOK_PRODUCT_ID, KOK_DETAIL_COL, KOK_DETAIL_VAL)
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS FCT_KOK_REVIEW_EXAMPLE (
            KOK_REVIEW_ID INT PRIMARY KEY,
            KOK_PRODUCT_ID	INT,
            KOK_NICKNAME	VARCHAR(30),
            KOK_REVIEW_DATE	VARCHAR(30),
            KOK_REVIEW_SCORE	INT,
            KOK_PRICE_EVAL	VARCHAR(30),
            KOK_DELIVERY_EVAL	VARCHAR(30),
            KOK_TASTE_EVAL	VARCHAR(30),
            KOK_REVIEW_TEXT	TEXT,
            FOREIGN KEY (KOK_PRODUCT_ID) REFERENCES FCT_KOK_PRODUCT_INFO (KOK_PRODUCT_ID) ON DELETE CASCADE,
            UNIQUE KEY UNIQ_PROD_COL (KOK_NICKNAME, KOK_REVIEW_DATE)
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS FCT_KOK_PRICE_INFO (
            KOK_PRICE_ID INT PRIMARY KEY,
            KOK_PRODUCT_ID	INT,
            KOK_DISCOUNT_RATE	INT,
            KOK_DISCOUNTED_PRICE	INT,
            UNIQUE KEY UNIQ_PROD_COL (KOK_PRODUCT_ID, KOK_DISCOUNT_RATE, KOK_DISCOUNTED_PRICE)
        );
    ''']

    # mariaDB : SERVICE_DB 연결
    conn, cur = utils.con_to_maria_service()
    
    # 쿼리 실행
    for query in queries:
        cur.execute(query)
    cur.close()
    conn.close()

def preprocess_kok(): # PREPROCESSING ODS TABLES / INSERT TO FCT TABLES
    conn_o, cur_o = utils.con_to_maria_ods()
    conn_s, cur_s = utils.con_to_maria_service()

    try:
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
        cur_s.execute("""
                SELECT DISTINCT KOK_PRODUCT_ID FROM FCT_KOK_PRODUCT_INFO;
                    """)
        rows = cur_s.fetchall()
        dist_list = [r[0] for r in rows]
        dist_text = ','.join(map(str,dist_list))

        def w_query(dist_text):
            if dist_text :
                where_query = f'''WHERE KOK_PRODUCT_ID NOT IN ({dist_text})'''
            else : 
                where_query = ''
            return where_query
        
        # SELECT query => DataFrame
        def select_from_ods_kok_price_info(dist_text, cur_o):
            query = f'''
                SELECT
                    CAST(KOK_PRICE_ID AS INT) AS KOK_PRICE_ID,
                    CAST(KOK_PRODUCT_ID AS INT) AS KOK_PRODUCT_ID,
                    STR_TO_NUM(KOK_DISCOUNT_RATE) AS KOK_DISCOUNT_RATE,
                    KOK_DISCOUNTED_PRICE
                FROM ODS_KOK_PRICE_INFO;
                '''
            cur_o.execute(query)
            rows = cur_o.fetchall()
            cols = [desc[0] for desc in cur_o.description]  # 컬럼명 가져오기
            df = pd.DataFrame(rows, columns=cols)
            return df        
        def select_from_ods_kok_product_info(dist_text, cur_o):
            query = f'''
                SELECT
                    CAST(KOK_PRODUCT_ID AS INT) AS KOK_PRODUCT_ID,
                    KOK_STORE_NAME,
                    KOK_PRODUCT_NAME,
                    KOK_THUMBNAIL,
                    STR_TO_NUM(KOK_PRODUCT_PRICE) AS KOK_PRODUCT_PRICE,
                    STR_TO_NUM(KOK_REVIEW_SCORE) AS KOK_REVIEW_SCORE,
                    STR_TO_NUM(KOK_REVIEW_CNT) AS KOK_REVIEW_CNT,
                    STR_TO_NUM(KOK_5_RATIO) AS KOK_5_RATIO, 
                    STR_TO_NUM(KOK_4_RATIO) AS KOK_4_RATIO, 
                    STR_TO_NUM(KOK_3_RATIO) AS KOK_3_RATIO, 
                    STR_TO_NUM(KOK_2_RATIO) AS KOK_2_RATIO, 
                    STR_TO_NUM(KOK_1_RATIO) AS KOK_1_RATIO,
                    KOK_ASPECT_PRICE, 
                    STR_TO_NUM(KOK_ASPECT_PRICE_RATIO) AS KOK_ASPECT_PRICE_RATIO,
                    KOK_ASPECT_DELIVERY, 
                    STR_TO_NUM(KOK_ASPECT_DELIVERY_RATIO) AS KOK_ASPECT_DELIVERY_RATIO,
                    KOK_ASPECT_TASTE, 
                    STR_TO_NUM(KOK_ASPECT_TASTE_RATIO) AS KOK_ASPECT_TASTE_RATIO,
                    KOK_SELLER, 
                    KOK_CO_CEO, 
                    KOK_CO_REG_NO, 
                    KOK_CO_EC_REG, 
                    KOK_TELL,
                    KOK_VER_ITEM, 
                    KOK_VER_DATE, 
                    KOK_CO_ADDR, 
                    KOK_RETURN_ADDR, 
                    KOK_EXCHANGE_ADDR
                FROM ODS_KOK_PRODUCT_INFO
                {w_query(dist_text)};
                '''
            cur_o.execute(query)
            rows = cur_o.fetchall()
            cols = [desc[0] for desc in cur_o.description]  # 컬럼명 가져오기
            if not rows:
                df = pd.DataFrame(columns=cols)
                return df
            df = pd.DataFrame(rows, columns=cols, dtype=object)
            df = df.where(pd.notna(df), None)           # NaN/NaT → None
            return df
        def select_from_ods_kok_image_info(dist_text, cur_o):
            query = f'''
                SELECT
                    CAST(KOK_IMG_ID AS INT) AS KOK_IMG_ID,
                    CAST(KOK_PRODUCT_ID AS INT) AS KOK_PRODUCT_ID,
                    KOK_IMG_URL
                FROM ODS_KOK_IMAGE_INFO
                {w_query(dist_text)};
                    '''
            cur_o.execute(query)
            rows = cur_o.fetchall()
            cols = [desc[0] for desc in cur_o.description]  # 컬럼명 가져오기
            df = pd.DataFrame(rows, columns=cols)
            return df     
        def select_from_ods_kok_detail_info(dist_text, cur_o):
            query = f'''
                SELECT
                    CAST(KOK_DETAIL_COL_ID AS INT) AS KOK_DETAIL_COL_ID,
                    CAST(KOK_PRODUCT_ID AS INT) AS KOK_PRODUCT_ID,
                    KOK_DETAIL_COL,
                    KOK_DETAIL_VAL
                FROM ODS_KOK_DETAIL_INFO
                {w_query(dist_text)};
                    '''
            cur_o.execute(query)
            rows = cur_o.fetchall()
            cols = [desc[0] for desc in cur_o.description]  # 컬럼명 가져오기
            df = pd.DataFrame(rows, columns=cols)
            return df      
        def select_from_ods_kok_review_example(dist_text, cur_o):
            query = f'''
                SELECT
                    CAST(KOK_REVIEW_ID AS INT) AS KOK_REVIEW_ID,
                    CAST(KOK_PRODUCT_ID AS INT) AS KOK_PRODUCT_ID,
                    KOK_NICKNAME, 
                    KOK_REVIEW_DATE,
                    KOK_REVIEW_SCORE, 
                    KOK_PRICE_EVAL, 
                    KOK_DELIVERY_EVAL,
                    KOK_TASTE_EVAL, 
                    KOK_REVIEW_TEXT
                FROM ODS_KOK_REVIEW_EXAMPLE
                {w_query(dist_text)};
                    '''
            cur_o.execute(query)
            rows = cur_o.fetchall()
            cols = [desc[0] for desc in cur_o.description]  # 컬럼명 가져오기
            df = pd.DataFrame(rows, columns=cols)
            return df

        # INSERT TO FCT KOK
        utils.insert_df_into_db(
            conn_s, utils.replace_nan_with_none(select_from_ods_kok_price_info(dist_text, cur_o)), 'FCT_KOK_PRICE_INFO', 'IGNORE')
        print('⭕ [KOK] INSERT TO FCT_KOK_PRICE_INFO')
        utils.insert_df_into_db(
            conn_s, utils.replace_nan_with_none(select_from_ods_kok_product_info(dist_text, cur_o)), 'FCT_KOK_PRODUCT_INFO', 'IGNORE')
        print('⭕ [KOK] INSERT TO FCT_KOK_PRODUCT_INFO')
        utils.insert_df_into_db(
            conn_s, utils.replace_nan_with_none(select_from_ods_kok_image_info(dist_text, cur_o)), 'FCT_KOK_IMAGE_INFO', 'IGNORE')
        print('⭕ [KOK] INSERT TO FCT_KOK_IMAGE_INFO')
        utils.insert_df_into_db(
            conn_s, utils.replace_nan_with_none(select_from_ods_kok_detail_info(dist_text, cur_o)), 'FCT_KOK_DETAIL_INFO', 'IGNORE')
        print('⭕ [KOK] INSERT TO FCT_KOK_DETAIL_INFO')
        utils.insert_df_into_db(
            conn_s, utils.replace_nan_with_none(select_from_ods_kok_review_example(dist_text, cur_o)), 'FCT_KOK_REVIEW_EXAMPLE', 'IGNORE')
        print('⭕ [KOK] INSERT TO FCT_KOK_REVIEW_EXAMPLE')
        
        # product_name 에서 store_name 삭제
        cur_s.execute('''
            UPDATE FCT_KOK_PRODUCT_INFO
            SET KOK_PRODUCT_NAME = 
            LTRIM(SUBSTRING(KOK_PRODUCT_NAME, CHAR_LENGTH(KOK_STORE_NAME) + 1))
            WHERE KOK_STORE_NAME IS NOT NULL
                AND KOK_PRODUCT_NAME IS NOT NULL
                AND LEFT(KOK_PRODUCT_NAME, CHAR_LENGTH(KOK_STORE_NAME)) = KOK_STORE_NAME;
                      ''')
        cur_s.execute('''
            UPDATE FCT_KOK_PRODUCT_INFO
            SET KOK_PRODUCT_NAME = KOK_STORE_NAME
            WHERE KOK_PRODUCT_NAME = '';
                      ''')
        print('⭕ [KOK] PREPROCESS KOK_PRODUCT_NAME')
        # FCT_KOK 관련 테이블 간 무결성 확보 (DELETE)
        cur_s.execute('''
            DELETE FROM FCT_KOK_DETAIL_INFO
            WHERE KOK_PRODUCT_ID NOT IN (
                SELECT DISTINCT KOK_PRODUCT_ID FROM FCT_KOK_IMAGE_INFO);
                      ''')
        cur_s.execute('''
            DELETE FROM FCT_KOK_IMAGE_INFO
            WHERE KOK_PRODUCT_ID NOT IN (
                SELECT DISTINCT KOK_PRODUCT_ID FROM FCT_KOK_DETAIL_INFO);
                      ''')
        cur_s.execute('''
            DELETE FROM FCT_KOK_PRODUCT_INFO 
            WHERE KOK_PRODUCT_ID IN (
                SELECT KOK_PRODUCT_ID 
                FROM FCT_KOK_PRODUCT_INFO 
                WHERE KOK_PRODUCT_ID NOT IN 
                    (SELECT DISTINCT KOK_PRODUCT_ID 
                    FROM FCT_KOK_DETAIL_INFO) 
                INTERSECT
                SELECT KOK_PRODUCT_ID 
                FROM FCT_KOK_PRODUCT_INFO 
                WHERE KOK_PRODUCT_ID NOT IN 
                    (SELECT DISTINCT KOK_PRODUCT_ID 
                    FROM FCT_KOK_IMAGE_INFO)
                    );
                      ''')
        cur_s.execute('''                    
            DELETE FROM FCT_KOK_PRICE_INFO
            WHERE KOK_PRODUCT_ID NOT IN (
                SELECT KOK_PRODUCT_ID FROM FCT_KOK_PRODUCT_INFO);
                      ''')
        print('⭕ [KOK] 무결성 확보')
    except Exception as e:
        print(f"❌ MariaDB 오류: {e}")
        sys.exit(1)
    finally:
        cur_o.close()
        conn_o.close()
        cur_s.close()
        conn_s.close()

def main():
    create_tables_kok_fct()
    preprocess_kok()

if __name__ == "__main__":
    main()