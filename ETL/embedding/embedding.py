import numpy as np
from sentence_transformers import SentenceTransformer
import pandas as pd
import ETL.utils.utils as utils

def create_vec_table():
    cr_rcp_vec_query = """
        CREATE TABLE IF NOT EXISTS "RECIPE_VECTOR_TABLE" (
                "VECTOR_ID" SERIAL PRIMARY KEY,
                "RECIPE_ID" BIGINT,
                "VECTOR_NAME" vector(384)
                );
                """
    cr_mtrl_vec_query = """
        CREATE TABLE IF NOT EXISTS "MATERIAL_VECTOR_TABLE" (
                "VECTOR_ID" SERIAL PRIMARY KEY,
                "MATERIAL_ID" BIGINT,
                "MATERIAL_NAME" VARCHAR(100),
                "VECTOR_NAME" vector(384)
                );
                """
    cr_kok_vec_query = """
        CREATE TABLE IF NOT EXISTS "KOK_VECTOR_TABLE" (
                "VECTOR_ID" SERIAL PRIMARY KEY,
                "KOK_PRODUCT_ID" BIGINT,
                "VECTOR_NAME" vector(384)
                );
                """
    cr_hs_vec_query = """
        CREATE TABLE IF NOT EXISTS "HOMESHOPPING_VECTOR_TABLE" (
                "VECTOR_ID" SERIAL PRIMARY KEY,
                "PRODUCT_ID" BIGINT,
                "VECTOR_NAME" vector(384)
                );
                """
    conn, cur = utils.con_to_psql("REC_DB")
    cur.execute('''CREATE EXTENSION IF NOT EXISTS vector;''')
    cur.execute(cr_rcp_vec_query)
    cur.execute(cr_mtrl_vec_query)
    cur.execute(cr_kok_vec_query)
    cur.execute(cr_hs_vec_query)
    cur.close
    conn.close

def embed_texts(model, texts):
    vectors = model.encode(texts, normalize_embeddings=True)
    return vectors.tolist()

def rcp_embed():
    # 임베딩 모델 로드 (MiniLM, 384차원)
    model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

    conn_s, cur_s = utils.con_to_maria_service()
    conn_r, cur_r = utils.con_to_psql('REC_DB')

    # COOKING_NAME 임베딩 → VECTOR_NAME 컬럼에 저장
    cur_s.execute("""
                SELECT RECIPE_ID, COOKING_NAME
                FROM FCT_RECIPE;
                """)
    rows = cur_s.fetchall()
    to_vec_df = pd.DataFrame(rows, columns=['RECIPE_ID', 'COOKING_NAME'])
    print('DataFrame 생성 완료')
    to_vec_df['VECTOR_NAME'] = embed_texts(model, to_vec_df['COOKING_NAME'].tolist())
    print('Vector value 생성 완료')
    vec_df = to_vec_df[['RECIPE_ID', 'VECTOR_NAME']]
    utils.insert_df_into_db(conn_r, vec_df, 'RECIPE_VECTOR_TABLE')
    print('Insert 완료')
    cur_s.close()
    conn_s.close()
    cur_r.close()
    conn_r.close()

def mtrl_embed():
    # 임베딩 모델 로드 (MiniLM, 384차원)
    model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

    conn_s, cur_s = utils.con_to_maria_service()
    conn_r, cur_r = utils.con_to_psql('REC_DB')

    # COOKING_NAME 임베딩 → VECTOR_NAME 컬럼에 저장
    cur_s.execute("""
                SELECT MIN(MATERIAL_ID) AS MATERIAL_ID, MATERIAL_NAME
                FROM FCT_MTRL
                GROUP BY MATERIAL_NAME;
                """)
    rows = cur_s.fetchall()
    to_vec_df = pd.DataFrame(rows, columns=['MATERIAL_ID', 'MATERIAL_NAME'])
    print('DataFrame 생성 완료')
    to_vec_df['VECTOR_NAME'] = embed_texts(model, to_vec_df['MATERIAL_NAME'].tolist())
    print('Vector value 생성 완료')
    vec_df = to_vec_df[['MATERIAL_ID', 'MATERIAL_NAME', 'VECTOR_NAME']]
    utils.insert_df_into_db(conn_r, vec_df, 'MATERIAL_VECTOR_TABLE')
    print('Insert 완료')
    cur_s.close()
    conn_s.close()
    cur_r.close()
    conn_r.close()

def kok_embed():
    # 임베딩 모델 로드 (MiniLM, 384차원)
    model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
    conn_s, cur_s = utils.con_to_maria_service()
    conn_r, cur_r = utils.con_to_psql('REC_DB')
    cur_r.execute(f'''
        SELECT
            DISTINCT "KOK_PRODUCT_ID"
        FROM "KOK_VECTOR_TABLE";
                  ''')
    id_list = [row[0] for row in cur_r.fetchall()]
    j = ','.join(map(str,id_list))
    add_query = f"WHERE KOK_PRODUCT_ID NOT IN ({j})" if j else ""
    # COOKING_NAME 임베딩 → VECTOR_NAME 컬럼에 저장
    cur_s.execute(f"""
                SELECT KOK_PRODUCT_ID, KOK_PRODUCT_NAME
                FROM FCT_KOK_PRODUCT_INFO
                {add_query};
                """)
    rows = cur_s.fetchall()
    if not rows :
        cur_r.close(); conn_r.close()
        cur_s.close(); conn_s.close()
        print('[KOK] 벡터 테이블 갱신 사항 없음')
        return
    to_vec_df = pd.DataFrame(rows, columns=['KOK_PRODUCT_ID', 'KOK_PRODUCT_NAME'])
    print('[KOK] DataFrame 생성 완료')
    to_vec_df['VECTOR_NAME'] = embed_texts(model, to_vec_df['KOK_PRODUCT_NAME'].tolist())
    print('[KOK] Vector value 생성 완료')
    vec_df = to_vec_df[['KOK_PRODUCT_ID', 'VECTOR_NAME']]
    utils.insert_df_into_db(conn_r, vec_df, 'KOK_VECTOR_TABLE')
    print('[KOK] Insert 완료')
    cur_r.close()
    conn_r.close()
    cur_s.close()
    conn_s.close()

def homeshop_embed():
    # 임베딩 모델 로드 (MiniLM, 384차원)
    model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
    conn_s, cur_s = utils.con_to_maria_service()
    conn_r, cur_r = utils.con_to_psql('REC_DB')
    cur_r.execute(f'''
        SELECT
            DISTINCT "PRODUCT_ID"
        FROM "HOMESHOPPING_VECTOR_TABLE";
                  ''')
    id_list = [row[0] for row in cur_r.fetchall()]
    j = ','.join(map(str,id_list))
    add_query = f"WHERE PRODUCT_ID NOT IN ({j})" if j else ""
    # COOKING_NAME 임베딩 → VECTOR_NAME 컬럼에 저장
    cur_s.execute(f"""
                SELECT DISTINCT PRODUCT_ID, PRODUCT_NAME
                FROM FCT_HOMESHOPPING_LIST
                {add_query};
                """)
    rows = cur_s.fetchall()
    if not rows :
        cur_r.close(); conn_r.close()
        cur_s.close(); conn_s.close()
        print('[HS] 벡터 테이블 갱신 사항 없음')
        return

    to_vec_df = pd.DataFrame(rows, columns=['PRODUCT_ID', 'PRODUCT_NAME'])
    print('[HS] DataFrame 생성 완료')
    to_vec_df['VECTOR_NAME'] = embed_texts(model, to_vec_df['PRODUCT_NAME'].tolist())
    print('[HS] Vector value 생성 완료')
    vec_df = to_vec_df[['PRODUCT_ID', 'VECTOR_NAME']]
    utils.insert_df_into_db(conn_r, vec_df, 'HOMESHOPPING_VECTOR_TABLE')
    print('[HS] Insert 완료')
    cur_r.close()
    conn_r.close()
    cur_s.close()
    conn_s.close()

def del_hs_vec():
    conn_s, cur_s = utils.con_to_maria_service()
    conn_r, cur_r = utils.con_to_psql('REC_DB')
    cur_s.execute("""
        SELECT PRODUCT_ID FROM FCT_HOMESHOPPING_LIST;
                  """)
    rows = cur_s.fetchall()
    id_list = [r[0] for r in rows]
    id_list_text = ','.join(map(str, id_list))
    cur_r.execute(f"""
        DELETE FROM "HOMESHOPPING_VECTOR_TABLE" WHERE "PRODUCT_ID" NOT IN ({id_list_text});
                  """)
    cur_s.close()
    conn_s.close()
    cur_r.close()
    conn_r.close()

def main():
    kok_embed()
    homeshop_embed()    

if __name__ == "__main__":
    main()