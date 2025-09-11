import pandas as pd
import ETL.utils.utils as utils
import time
def create_fct_homeshopping(): # 홈쇼핑 관련 데이터 FCT TABLE 생성 함수
    conn, cur_s = utils.con_to_maria_service()
    create_list = """
    CREATE TABLE IF NOT EXISTS FCT_HOMESHOPPING_LIST (
        LIVE_ID	INT PRIMARY KEY,
        HOMESHOPPING_ID SMALLINT,
        LIVE_DATE DATE,
        LIVE_START_TIME TIME,
        LIVE_END_TIME TIME,
        PROMOTION_TYPE ENUM('main', 'sub'),
        PRODUCT_ID BIGINT,
        PRODUCT_NAME TEXT,
        THUMB_IMG_URL TEXT,
        SCHEDULED_OR_CANCELLED TINYINT NOT NULL DEFAULT 1,
        FOREIGN KEY (HOMESHOPPING_ID) REFERENCES HOMESHOPPING_INFO (HOMESHOPPING_ID),
        UNIQUE KEY UNIQ_PROD_COL (HOMESHOPPING_ID, LIVE_DATE, LIVE_START_TIME, LIVE_END_TIME, PROMOTION_TYPE, PRODUCT_ID)
        );
    """
    create_prod = """
    CREATE TABLE IF NOT EXISTS FCT_HOMESHOPPING_PRODUCT_INFO (
        PRODUCT_ID BIGINT PRIMARY KEY, 
        STORE_NAME VARCHAR(1000),
        SALE_PRICE BIGINT,
        DC_RATE INT,
        DC_PRICE BIGINT
        );
    """
    create_detail = """
    CREATE TABLE IF NOT EXISTS FCT_HOMESHOPPING_DETAIL_INFO (
        DETAIL_ID INT PRIMARY KEY,
        PRODUCT_ID BIGINT,
        DETAIL_COL VARCHAR(1000),
        DETAIL_VAL TEXT,
        FOREIGN KEY (PRODUCT_ID) REFERENCES FCT_HOMESHOPPING_PRODUCT_INFO (PRODUCT_ID),
        UNIQUE KEY UNIQ_PROD_COL (PRODUCT_ID, DETAIL_COL)
        );
    """
    create_img = """
    CREATE TABLE IF NOT EXISTS FCT_HOMESHOPPING_IMG_URL (
        IMG_ID INT PRIMARY KEY,
        PRODUCT_ID BIGINT,
        SORT_ORDER SMALLINT,
        IMG_URL VARCHAR(4000),
        FOREIGN KEY (PRODUCT_ID) REFERENCES FCT_HOMESHOPPING_PRODUCT_INFO (PRODUCT_ID),
        UNIQUE KEY UNIQ_PROD_COL (PRODUCT_ID, IMG_URL)
        );
    """
    create_hs_info = """
    CREATE TABLE IF NOT EXISTS HOMESHOPPING_INFO (
        HOMESHOPPING_ID	SMALLINT PRIMARY KEY,
        HOMESHOPPING_NAME VARCHAR(20),
        HOMESHOPPING_CHANNEL SMALLINT,
        LIVE_URL VARCHAR(200)
        );
    """
    create_hs_cur_schedule = '''
    CREATE TABLE IF NOT EXISTS FCT_HOMESHOPPING_CURRENT_SCHEDULE (
        HOMESHOPPING_ID SMALLINT,
        LIVE_DATE DATE,
        LIVE_START_TIME TIME,
        LIVE_END_TIME TIME,
        PRODUCT_ID BIGINT,
        FOREIGN KEY (HOMESHOPPING_ID) REFERENCES HOMESHOPPING_INFO (HOMESHOPPING_ID)
        );
    '''
    cur_s.execute(create_hs_info)
    cur_s.execute(create_list)
    cur_s.execute(create_prod)
    cur_s.execute(create_detail)
    cur_s.execute(create_img)
    cur_s.execute(create_hs_cur_schedule)
    cur_s.close()
    conn.close()

def slt_to_cln(table_name): # FCT TABLE로 옮길 데이터를 ODS TABLE에서 SELECT. FCT_와 ODS_를 제외한 테이블명 일치 필요
    conn_o, cur_o = utils.con_to_maria_ods()
    conn_s, cur_s = utils.con_to_maria_service()
    cur_s.execute(f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'SERVICE_DB'
    AND TABLE_NAME = 'FCT_{table_name}'
                  """)
    col_list = [row[0] for row in cur_s.fetchall()]
    i = ','.join(map(str,col_list))

    cur_s.execute(f'''
        SELECT
            DISTINCT PRODUCT_ID
        FROM FCT_{table_name};
                  ''')
    id_list = [row[0] for row in cur_s.fetchall()]
    j = ','.join(map(str,id_list))
    add_query = f"WHERE PRODUCT_ID NOT IN ({j})" if j else ""

    cur_o.execute(f"""
    SELECT 
        {i}
    FROM ODS_{table_name}
    {add_query};
    """)
    rows = cur_o.fetchall()
    df = pd.DataFrame(rows, columns=col_list)

    cur_o.close()
    conn_o.close()
    cur_s.close()
    conn_s.close()
    return df

def prep_homeshop_list():
    conn_o, cur_o = utils.con_to_maria_ods()
    conn_s, cur_s = utils.con_to_maria_service()
    cur_s.execute('''
        SELECT
            LIVE_ID
        FROM FCT_HOMESHOPPING_LIST;
                  ''')
    col_list = [row[0] for row in cur_s.fetchall()]
    i = ','.join(map(str,col_list))
    add_query = f"WHERE LIVE_ID NOT IN ({i})" if i else ""
    cur_o.execute(f'''
        SELECT
            LIVE_ID,
            HOMESHOPPING_ID,
            LIVE_DATE,
            LIVE_TIME,
            PROMOTION_TYPE,
            PRODUCT_ID,
            PRODUCT_NAME,
            THUMB_IMG_URL
        FROM ODS_HOMESHOPPING_LIST {add_query};
                  ''')
    rows = cur_o.fetchall()
    if not rows:  # 빈 결과 가드
        cur_o.close(); conn_o.close()
        cur_s.close(); conn_s.close()
        return
    b_df = pd.DataFrame(rows, columns=[ 'LIVE_ID',
                                        'HOMESHOPPING_ID',
                                        'LIVE_DATE',
                                        'LIVE_TIME',
                                        'PROMOTION_TYPE',
                                        'PRODUCT_ID',
                                        'PRODUCT_NAME',
                                        'THUMB_IMG_URL'])
    live_time = (
        b_df['LIVE_TIME']
        .astype(str)
        .str.replace('\u00A0', ' ', regex=False)   # NBSP 제거
        .str.replace('\u200B', '', regex=False)    # zero-width space 제거
        .str.replace('[～〜∼]', '~', regex=True)    # 전각/유사 틸드 -> ~
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
    )

    # 2) HH:MM ~ HH:MM 패턴만 추출 (매치 실패는 NaN으로 남음)
    times = live_time.str.extract(
        r'(?P<LIVE_START_TIME>\d{1,2}:\d{2})\s*~\s*(?P<LIVE_END_TIME>\d{1,2}:\d{2})'
    )
    # 3) 타입 변환
    b_df['LIVE_START_TIME'] = pd.to_datetime(
        times['LIVE_START_TIME'], format='%H:%M', errors='coerce'
    ).dt.time
    b_df['LIVE_END_TIME'] = pd.to_datetime(
        times['LIVE_END_TIME'], format='%H:%M', errors='coerce'
    ).dt.time
    
    b_df['LIVE_DATE'] = pd.to_datetime(b_df['LIVE_DATE'], format='%Y%m%d')
    a_df = b_df.drop(columns=['LIVE_TIME'])
    utils.insert_df_into_db(conn_s, a_df, 'FCT_HOMESHOPPING_LIST', "IGNORE") 
    cur_o.close()
    conn_o.close()
    cur_s.close()
    conn_s.close()
# 편성표 변경 필터 테이블
def prep_cur_schedule():
    conn_o, cur_o = utils.con_to_maria_ods()
    conn_s, cur_s = utils.con_to_maria_service()
    cur_s.execute('''DELETE FROM FCT_HOMESHOPPING_CURRENT_SCHEDULE;''')
    cur_o.execute('''SELECT * FROM ODS_HOMESHOPPING_CURRENT_SCHEDULE;''')
    rows = cur_o.fetchall()
    if not rows:  # 빈 결과 가드
        cur_o.close(); conn_o.close()
        cur_s.close(); conn_s.close()
        return
    b_df = pd.DataFrame(rows, columns=['HOMESHOPPING_ID',
                                    'LIVE_DATE',
                                    'LIVE_TIME',
                                    'PRODUCT_ID'])
    live_time = (
        b_df['LIVE_TIME']
        .astype(str)
        .str.replace('\u00A0', ' ', regex=False)   # NBSP 제거
        .str.replace('\u200B', '', regex=False)    # zero-width space 제거
        .str.replace('[～〜∼]', '~', regex=True)    # 전각/유사 틸드 -> ~
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
    )

    # 2) HH:MM ~ HH:MM 패턴만 추출 (매치 실패는 NaN으로 남음)
    times = live_time.str.extract(
        r'(?P<LIVE_START_TIME>\d{1,2}:\d{2})\s*~\s*(?P<LIVE_END_TIME>\d{1,2}:\d{2})'
    )
    # 3) 타입 변환
    b_df['LIVE_START_TIME'] = pd.to_datetime(
        times['LIVE_START_TIME'], format='%H:%M', errors='coerce'
    ).dt.time
    b_df['LIVE_END_TIME'] = pd.to_datetime(
        times['LIVE_END_TIME'], format='%H:%M', errors='coerce'
    ).dt.time
    
    b_df['LIVE_DATE'] = pd.to_datetime(b_df['LIVE_DATE'], format='%Y%m%d')
    a_df = b_df.drop(columns=['LIVE_TIME'])
    utils.insert_df_into_db(conn_s, a_df, 'FCT_HOMESHOPPING_CURRENT_SCHEDULE') 
    cur_o.close()
    conn_o.close()
    cur_s.close()
    conn_s.close()

def prep_homeshop_prd():
    conn_o, cur_o = utils.con_to_maria_ods()
    conn_s, cur_s = utils.con_to_maria_service()
    b_df = slt_to_cln('HOMESHOPPING_PRODUCT_INFO')

    for i in ['SALE_PRICE','DC_RATE','DC_PRICE']:
        b_df[i] = (
            b_df[i]
            .astype(str)                    
            .str.replace(r'[^0-9]', '', regex=True)
        )
        b_df[i] = pd.to_numeric(b_df[i], errors='coerce')
        b_df[i] = b_df[i].fillna(0).astype('int64')

    utils.insert_df_into_db(conn_s, b_df, 'FCT_HOMESHOPPING_PRODUCT_INFO', "IGNORE")
    cur_o.close()
    conn_o.close()
    cur_s.close()
    conn_s.close()
def prep_homeshop_dtl():
    conn_o, cur_o = utils.con_to_maria_ods()
    conn_s, cur_s = utils.con_to_maria_service()
    b_df = slt_to_cln('HOMESHOPPING_DETAIL_INFO')
    a_df = b_df.astype({
            'PRODUCT_ID':'int64'
    })
    utils.insert_df_into_db(conn_s, a_df, 'FCT_HOMESHOPPING_DETAIL_INFO', "IGNORE")
    cur_o.close()
    conn_o.close()
    cur_s.close()
    conn_s.close()
def prep_homeshop_img():
    conn_o, cur_o = utils.con_to_maria_ods()
    conn_s, cur_s = utils.con_to_maria_service()

    b_df = slt_to_cln('HOMESHOPPING_IMG_URL')
    a_df = b_df.astype({
            'PRODUCT_ID':'int64'
    })
    utils.insert_df_into_db(conn_s, a_df, 'FCT_HOMESHOPPING_IMG_URL', "IGNORE")
    cur_o.close()
    conn_o.close()
    cur_s.close()
    conn_s.close()
def prep_homeshop_info():
    conn_o, cur_o = utils.con_to_maria_ods()
    conn_s, cur_s = utils.con_to_maria_service()

    cur_o.execute("""
        SELECT * FROM HOMESHOPPING_INFO;
                  """)
    bdf = cur_o.fetchall()
    df = pd.DataFrame(bdf, columns=['HOMESHOPPING_ID','HOMESHOPPING_NAME','HOMESHOPPING_CHANNEL','LIVE_URL'])
    utils.insert_df_into_db(conn_s, df, 'HOMESHOPPING_INFO', "IGNORE")
    cur_o.close()
    conn_o.close()
    cur_s.close()
    conn_s.close()
def main(): # 홈쇼핑 데이터 전처리 및 FCT TABLE 적재 통합 코드
    conn_s, cur_s = utils.con_to_maria_service()
    prep_homeshop_info()
    print('🟣 [HS] insert into info table')
    prep_homeshop_list()
    print('🟣 [HS] insert into list table')
    prep_homeshop_prd()
    print('🟣 [HS] insert into product table')
    prep_homeshop_dtl()
    print('🟣 [HS] insert into detail table')
    prep_homeshop_img()
    print('🟣 [HS] insert into image table')
    # 편성표 변경 대응 필터 테이블
    prep_cur_schedule()
    print('🟣 [HS] insert into cur schedule table')
    # 홈앤쇼핑 중복 이미지 url 삭제
    cur_s.execute('''
        DELETE FROM FCT_HOMESHOPPING_IMG_URL
        WHERE 
            (IMG_URL LIKE '%format/avif%' OR IMG_URL LIKE '%format/webp%') AND 
            PRODUCT_ID IN (SELECT PRODUCT_ID FROM FCT_HOMESHOPPING_LIST WHERE HOMESHOPPING_ID = 1) AND
            PRODUCT_ID NOT IN(
                SELECT P.PRODUCT_ID FROM (
                    SELECT DISTINCT
                        A.PRODUCT_ID,
                        B.NOT_FORMATTED_IMG,
                        C.FORMATTED_IMG,
                        C.FORMATTED_IMG / B.NOT_FORMATTED_IMG AS TIMES
                    FROM FCT_HOMESHOPPING_IMG_URL A
                    
                    LEFT JOIN (
                        SELECT 
                            PRODUCT_ID, 
                            COUNT(IMG_URL) AS NOT_FORMATTED_IMG
                        FROM FCT_HOMESHOPPING_IMG_URL 
                        WHERE 
                            IMG_URL NOT LIKE '%format/avif%' AND 
                            IMG_URL NOT LIKE '%format/webp%' AND 
                            PRODUCT_ID IN (
                                SELECT PRODUCT_ID 
                                FROM FCT_HOMESHOPPING_LIST 
                                WHERE HOMESHOPPING_ID = 1
                                ) 
                        GROUP BY PRODUCT_ID) B ON B.PRODUCT_ID = A.PRODUCT_ID
                    
                    LEFT JOIN (
                        SELECT 
                            PRODUCT_ID, 
                            COUNT(IMG_URL) AS FORMATTED_IMG
                        FROM FCT_HOMESHOPPING_IMG_URL 
                        WHERE 
                            (IMG_URL LIKE '%format/avif%' OR 
                            IMG_URL LIKE '%format/webp%') AND 
                            PRODUCT_ID IN (
                                SELECT PRODUCT_ID 
                                FROM FCT_HOMESHOPPING_LIST 
                                WHERE HOMESHOPPING_ID = 1
                                ) 
                        GROUP BY PRODUCT_ID) C ON C.PRODUCT_ID = A.PRODUCT_ID
                    
                    WHERE A.PRODUCT_ID IN (
                                SELECT PRODUCT_ID 
                                FROM FCT_HOMESHOPPING_LIST 
                                WHERE HOMESHOPPING_ID = 1
                                ) 
                    )P
                WHERE P.NOT_FORMATTED_IMG IS NULL);
    ''')
    print('🟣 [HS] 홈앤쇼핑 중복 이미지 url 삭제')
    # FCT_HOMESHOPPING 관련 테이블 간 무결성 확보 (DELETE)
    cur_s.execute('''
        DELETE FROM FCT_HOMESHOPPING_DETAIL_INFO
        WHERE PRODUCT_ID NOT IN (
            SELECT DISTINCT PRODUCT_ID FROM FCT_HOMESHOPPING_IMG_URL);
    ''')
    cur_s.execute('''
        DELETE FROM FCT_HOMESHOPPING_IMG_URL
        WHERE PRODUCT_ID NOT IN (
            SELECT DISTINCT PRODUCT_ID FROM FCT_HOMESHOPPING_DETAIL_INFO);
    ''')
    cur_s.execute('''
        DELETE FROM FCT_HOMESHOPPING_PRODUCT_INFO
        WHERE PRODUCT_ID IN (
            SELECT PRODUCT_ID 
            FROM FCT_HOMESHOPPING_PRODUCT_INFO 
            WHERE PRODUCT_ID NOT IN 
                (SELECT DISTINCT PRODUCT_ID 
                FROM FCT_HOMESHOPPING_DETAIL_INFO) 
            INTERSECT
            SELECT PRODUCT_ID 
            FROM FCT_HOMESHOPPING_PRODUCT_INFO 
            WHERE PRODUCT_ID NOT IN 
                (SELECT DISTINCT PRODUCT_ID 
                FROM FCT_HOMESHOPPING_IMG_URL)
                );
    ''')
    cur_s.execute('''
        DELETE FROM FCT_HOMESHOPPING_LIST
        WHERE PRODUCT_ID NOT IN (
            SELECT PRODUCT_ID FROM FCT_HOMESHOPPING_PRODUCT_INFO);
    ''')
    print('🟣 [HS] 무결성 확보')
    # FCT_HOMESHOPPING_CURRENT_SCHEDULE 테이블을 이용한 현재 편성표 상태 반영
    ## 방영예정, 방영취소 분류
    cur_s.execute('''
        UPDATE FCT_HOMESHOPPING_LIST
        SET SCHEDULED_OR_CANCELLED = 0
        WHERE 
            CONCAT(
                CAST(HOMESHOPPING_ID AS CHAR),
                CAST(LIVE_DATE AS CHAR),
                CAST(LIVE_START_TIME AS CHAR),
                CAST(LIVE_END_TIME AS CHAR),
                CAST(PRODUCT_ID AS CHAR)
            ) NOT IN (
                    SELECT 
                        CONCAT(
                            CAST(HOMESHOPPING_ID AS CHAR),
                            CAST(LIVE_DATE AS CHAR),
                            CAST(LIVE_START_TIME AS CHAR),
                            CAST(LIVE_END_TIME AS CHAR),
                            CAST(PRODUCT_ID AS CHAR)
                        ) AS MATCH_STR
                    FROM FCT_HOMESHOPPING_CURRENT_SCHEDULE
                  ) 
            AND 
            LIVE_DATE >= DATE_FORMAT(NOW(),'%Y-%m-%d');
        ''')
    print('🟣 [HS] 편성표 갱신')
    cur_s.close()
    conn_s.close()

if __name__ == "__main__":
    main()
