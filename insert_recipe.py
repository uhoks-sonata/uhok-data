import pandas as pd
import utils.utils as utils
import re
import embedding.embedding as emb

# 레시피 관련 테이블 생성 (ODS, SERVICE)
def create_rcp_table():
    create_ods_rcp = """
        CREATE TABLE IF NOT EXISTS ODS_RECIPE (
            RCP_SNO	varchar(20)				PRIMARY KEY,
            RCP_TTL	varchar(200)	NULL	DEFAULT	NULL,	
            CKG_NM	varchar(40)	NULL	DEFAULT	NULL,	
            RGTR_ID	varchar(32)	NULL	DEFAULT	NULL,	
            RGTR_NM	varchar(64)	NULL	DEFAULT	NULL,	
            INQ_CNT	varchar(20)	NULL	DEFAULT	NULL,	
            RCMM_CNT	varchar(20)	NULL	DEFAULT	NULL,	
            SRAP_CNT	varchar(20)	NULL	DEFAULT	NULL,	
            CKG_MTH_ACTO_NM	varchar(100)	NULL	DEFAULT	NULL,	
            CKG_STA_ACTO_NM	varchar(100)	NULL	DEFAULT	NULL,	
            CKG_MTRL_ACTO_NM	varchar(100)	NULL	DEFAULT	NULL,	
            CKG_KND_ACTO_NM	varchar(100)	NULL	DEFAULT	NULL,	
            CKG_IPDC	text	NULL	DEFAULT	NULL,	
            CKG_MTRL_CN	text	NULL	DEFAULT	NULL,	
            CKG_INBUN_NM	varchar(100)	NULL	DEFAULT	NULL,	
            CKG_DODF_NM	varchar(100)	NULL	DEFAULT	NULL,	
            CKG_TIME_NM	varchar(100)	NULL	DEFAULT	NULL,	
            FIRST_REG_DT	varchar(20)	NULL	DEFAULT	NULL,	
            RCP_IMG_URL	text	NULL	DEFAULT	NULL	
    );
    """
    create_fct_rcp = """
        CREATE TABLE IF NOT EXISTS FCT_RECIPE (
            RECIPE_ID INT PRIMARY KEY,
            RECIPE_TITLE VARCHAR(200),
            COOKING_NAME VARCHAR(100),
            SCRAP_COUNT INT,
            COOKING_CASE_NAME VARCHAR(200),
            COOKING_CATEGORY_NAME VARCHAR(200),
            COOKING_INTRODUCTION TEXT,
            NUMBER_OF_SERVING VARCHAR(200),
            THUMBNAIL_URL TEXT
        );
    """
    create_fct_mtrl = """
        CREATE TABLE IF NOT EXISTS FCT_MTRL (
            MATERIAL_ID INT AUTO_INCREMENT PRIMARY KEY,
            RECIPE_ID INT,
            MATERIAL_NAME VARCHAR(100),
            MEASURE_AMOUNT VARCHAR(100),
            MEASURE_UNIT VARCHAR(200),
            DETAILS VARCHAR(200),
            FOREIGN KEY (RECIPE_ID) REFERENCES FCT_RECIPE (RECIPE_ID)
        );
    """
    create_tst_mtrl = """
        CREATE TABLE TESTTEST (
            MATERIAL_NAME VARCHAR(100) NOT NULL
        );
    """
    conn_o, cur_o = utils.con_to_maria_ods()
    conn_s, cur_s = utils.con_to_maria_service()
    # ODS_DB
    cur_o.execute(create_ods_rcp)
    #SERVICE_DB
    cur_s.execute(create_fct_rcp)
    cur_s.execute(create_fct_mtrl)
    cur_s.execute(create_tst_mtrl)
    cur_o.close()
    conn_o.close()
    cur_s.close()
    conn_s.close()
    print('⭕ CREATE ODS / FCT TABLE')
# 만개의 레시피 raw data 적재 (ODS_RECIPE)
def insert_rawdata():
    # CSV 파일 로드
    df = pd.read_csv('./__data/TB_RECIPE_SEARCH_241226.csv', encoding='utf-8')
    # mariaDB 연결
    conn, cur = utils.con_to_maria_ods()
    # 데이터 삽입 쿼리 (19개 컬럼)
    insert_query = """
    INSERT IGNORE INTO ODS_RECIPE (
        RCP_SNO, RCP_TTL, CKG_NM, RGTR_ID, RGTR_NM, INQ_CNT,
        RCMM_CNT, SRAP_CNT, CKG_MTH_ACTO_NM, CKG_STA_ACTO_NM,
        CKG_MTRL_ACTO_NM, CKG_KND_ACTO_NM, CKG_IPDC, CKG_MTRL_CN,
        CKG_INBUN_NM, CKG_DODF_NM, CKG_TIME_NM, FIRST_REG_DT, RCP_IMG_URL
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # 컬럼 정제 (공백 제거 + 대문자 변환)
    df.columns = [col.strip().upper() for col in df.columns]
    df = df.where(pd.notnull(df), None)
        
    # INSERT 수행         
    for i, row in df.iterrows():
        try:
            cur.execute(insert_query, (
                int(row['RCP_SNO']) if pd.notna(row['RCP_SNO']) else None,
                row['RCP_TTL'],
                row['CKG_NM'],
                row['RGTR_ID'],
                row['RGTR_NM'],
                int(row['INQ_CNT']) if pd.notna(row['INQ_CNT']) else None,
                int(row['RCMM_CNT']) if pd.notna(row['RCMM_CNT']) else None,
                int(row['SRAP_CNT']) if pd.notna(row['SRAP_CNT']) else None,
                row['CKG_MTH_ACTO_NM'],
                row['CKG_STA_ACTO_NM'],
                row['CKG_MTRL_ACTO_NM'],
                row['CKG_KND_ACTO_NM'],
                row['CKG_IPDC'],
                row['CKG_MTRL_CN'],
                row['CKG_INBUN_NM'],
                row['CKG_DODF_NM'],
                row['CKG_TIME_NM'],
                row['FIRST_REG_DT'],
                row['RCP_IMG_URL']
            ))
        except Exception as e:
            print(f"Insert error on row {i}: {e}")

    print("⭕ INSERT TO ODS_RECIPE")

    cur.close()
    conn.close()
# 만개의 레시피 데이터 전처리 (FCT_RECIPE)
def preprocess_rcp():
    # mariaDB 연결
    conn_o, cur_o = utils.con_to_maria_ods()
    conn_s, cur_s = utils.con_to_maria_service()
    # 테이블 로드
    cur_o.execute('SELECT * FROM ODS_RECIPE')
    rows = cur_o.fetchall()
    columns = [desc[0] for desc in cur_o.description]  # 컬럼명 추출
    df = pd.DataFrame(rows, columns=columns)
    
    # 컬럼 정제 (공백 제거 + 대문자 변환)
    df.columns = [col.strip().upper() for col in df.columns]
    df = df.where(pd.notnull(df), None)
    
    # 전처리 - NaN 포함 row 제거
    df = df.dropna(subset=[
        'RCP_SNO', 'RCP_TTL', 'CKG_NM', 'SRAP_CNT',
        'CKG_STA_ACTO_NM', 'CKG_MTRL_ACTO_NM', 'CKG_KND_ACTO_NM',
        'CKG_IPDC', 'CKG_MTRL_CN', 'CKG_INBUN_NM', 'RCP_IMG_URL'
    ])

    # INSERT 쿼리 (컬럼명 일치)
    insert_query = """
        INSERT IGNORE INTO FCT_RECIPE (
            RECIPE_ID, RECIPE_TITLE, COOKING_NAME,
            SCRAP_COUNT, COOKING_CASE_NAME,
            COOKING_CATEGORY_NAME, COOKING_INTRODUCTION,
            NUMBER_OF_SERVING, THUMBNAIL_URL
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    # INSERT 수행
    for i, row in df.iterrows():
        try:
            cur_s.execute(insert_query, (
                int(row['RCP_SNO']),
                row['RCP_TTL'],
                row['CKG_NM'],
                int(row['SRAP_CNT']),
                row['CKG_STA_ACTO_NM'],
                row['CKG_KND_ACTO_NM'],
                row['CKG_IPDC'],
                row['CKG_INBUN_NM'],
                row['RCP_IMG_URL']
            ))
        except Exception as e:
            print(f"Insert error on row {i}: {e}")

    print("⭕ INSERT TO FCT_RECIPE")

    cur_o.close()
    conn_o.close()
    cur_s.close()
    conn_s.close()
# 만개의 레시피 재료 컬럼 전처리 (FCT_MTRL)
def preprocess_mtrl():
    # 재료 컬럼 전처리 함수
    def parse_mtrl_text(rcp_sno, mtrl_str):
        def clean_whitespace(text):
            if not isinstance(text, str):
                return text
            # 비가시성 공백 전부 제거 후 strip
            ### ZWSP, NBSP, Ideographic Space, Word Joiner, ZWNBSP
            return re.sub(r'[\u200B\u00A0\u3000\u2060\uFEFF]', '', text).strip()

        items = mtrl_str.split('|')
        results = []

        for item in items:
            parts = item.split('\x07')  # ASCII 7 (bell character)
            # 공백 정제 포함하여 처리
            parts = [clean_whitespace(p) for p in parts if clean_whitespace(p) != '']

            if len(parts) >= 3:
                mtrl_name = parts[0]
                mtrl_cnt = parts[1]
                mtrl_unit = parts[2]
                mtrl_detail = parts[3] if len(parts) > 3 else None
            elif len(parts) == 2:
                mtrl_name = parts[0]
                mtrl_cnt = parts[1]
                mtrl_unit = None
                mtrl_detail = None
            elif len(parts) == 1:
                mtrl_name = parts[0]
                mtrl_cnt = None
                mtrl_unit = None
                mtrl_detail = None
            else:
                continue

            results.append({
                'RCP_SNO': rcp_sno,
                'MTRL_NAME': mtrl_name,
                'MTRL_CNT': mtrl_cnt,
                'MTRL_UNIT': mtrl_unit,
                'MTRL_DETAIL': mtrl_detail
            })

        return pd.DataFrame(results)
    # mariaDB 연결
    conn_o, cur_o = utils.con_to_maria_ods()
    conn_s, cur_s = utils.con_to_maria_service()

    # ✅ 전체 재료 문자열 불러오기
    cur_s.execute('''SELECT RECIPE_ID FROM FCT_RECIPE''')
    rows = cur_s.fetchall()
    rcp_id_list = [r[0] for r in rows]
    list_to_text = ','.join(map(str,rcp_id_list))
    cur_o.execute(fr'''
                        SELECT 
                            b.RCP_SNO, 
                        CASE
                            WHEN LOCATE('|', b.cleaned) > 0 THEN
                            CONCAT(SUBSTRING(b.cleaned, 1, LOCATE('|', b.cleaned) - 1),
                                    SUBSTRING(b.cleaned, LOCATE('|', b.cleaned) + 1))
                            ELSE b.cleaned
                        END AS cleaned
                        FROM (
                            SELECT 
                                a.RCP_SNO, 
                                REGEXP_REPLACE(a.cleaned_text, '\\s*\\|\\s*', '|') AS cleaned 
                            FROM (
                                SELECT 
                                    RCP_SNO, 
                                    REGEXP_REPLACE(CKG_MTRL_CN, '\\[[^]]+\\]', '|') AS cleaned_text
                                FROM ODS_RECIPE 
                                WHERE RCP_SNO IN ({list_to_text})
                            ) a
                        ) b;
                    '''
                    )
    rows = cur_o.fetchall()

    all_dfs = []

    for rcp_sno, mtrl_str in rows:
        if mtrl_str:
            df = parse_mtrl_text(rcp_sno, mtrl_str)
            all_dfs.append(df)

    full_df = pd.concat(all_dfs, ignore_index=True)
    full_df = full_df.where(pd.notnull(full_df), None)  # NaN → None

    ## 전처리
    # 1. 앞뒤 공백 제거
    full_df['MTRL_NAME'] = full_df['MTRL_NAME'].astype(str).str.strip()
    # 2. 공백 제거 후 ''인 경우 제거
    full_df = full_df[full_df['MTRL_NAME'] != '']

    # ✅ 전체 삽입
    insert_query = """
        INSERT INTO FCT_MTRL (RECIPE_ID, MATERIAL_NAME, MEASURE_AMOUNT, MEASURE_UNIT, DETAILS)
        VALUES (?, ?, ?, ?, ?)
    """

    for _, row in full_df.iterrows():
        try:
            cur_s.execute(insert_query, (
                row['RCP_SNO'],
                row['MTRL_NAME'],
                row['MTRL_CNT'],
                row['MTRL_UNIT'],
                row['MTRL_DETAIL']
            ))
        except Exception as e:
            print(f"❌ Insert error on RCP_SNO {row['RCP_SNO']}: {e}")

    print("⭕ INSERT TO FCT_MTRL")

    cur_o.close()
    conn_o.close()
    cur_s.close()
    conn_s.close()
# 재료명 중복제거된 테이블 별도 생성 (TEST_MTRL)
def insert_tst_mtrl():
    conn_s, cur_s = utils.con_to_maria_service()
    cur_s.execute('''
        INSERT INTO TEST_MTRL (SELECT DISTINCT MATERIAL_NAME FROM FCT_MTRL)
                  ''')
    cur_s.close()
    conn_s.close()

def main():
    create_rcp_table()
    insert_rawdata()
    preprocess_rcp()
    preprocess_mtrl()
    insert_tst_mtrl()
    emb.rcp_embed()
    emb.mtrl_embed()

if __name__ == "__main__":
    main()