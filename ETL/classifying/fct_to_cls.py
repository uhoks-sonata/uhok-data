import ETL.utils.utils as utils

# classify 테이블 생성
def create_cls_tables():
    conn_s, cur_s = utils.con_to_maria_service()
    cr_cls_hs_query = """
        CREATE TABLE IF NOT EXISTS HOMESHOPPING_CLASSIFY (
        PRODUCT_ID BIGINT PRIMARY KEY, 
        PRODUCT_NAME TEXT, 
        CLS_FOOD TINYINT(1) NULL DEFAULT NULL, 
        CLS_ING TINYINT(1) NULL DEFAULT NULL,
        CREATED_AT TIMESTAMP NOT NULL DEFAULT current_timestamp()
        );
    """
    cr_cls_kok_query = """
        CREATE TABLE IF NOT EXISTS KOK_CLASSIFY (
        PRODUCT_ID BIGINT PRIMARY KEY,
        STORE_NAME TEXT,
        PRODUCT_NAME TEXT,
        CLS_ING TINYINT(1) NULL DEFAULT NULL,
        CREATED_AT TIMESTAMP NOT NULL DEFAULT current_timestamp()
        );
    """
    cur_s.execute(cr_cls_hs_query)
    cur_s.execute(cr_cls_kok_query)
    cur_s.close()
    conn_s.close()
# fct -> cls 데이터 적재
def fct_to_cls():
    conn_s, cur_s = utils.con_to_maria_service()
    cur_s.execute("""
        INSERT IGNORE INTO HOMESHOPPING_CLASSIFY (PRODUCT_ID, PRODUCT_NAME)
        SELECT 
            PRODUCT_ID, PRODUCT_NAME
        FROM FCT_HOMESHOPPING_LIST
        WHERE PRODUCT_ID NOT IN 
                  (SELECT PRODUCT_ID FROM HOMESHOPPING_CLASSIFY);
                  """)
    cur_s.execute("""
        INSERT IGNORE INTO KOK_CLASSIFY (PRODUCT_ID, STORE_NAME, PRODUCT_NAME)
        SELECT 
            KOK_PRODUCT_ID, KOK_STORE_NAME, KOK_PRODUCT_NAME
        FROM FCT_KOK_PRODUCT_INFO
        WHERE KOK_PRODUCT_ID NOT IN 
                (SELECT PRODUCT_ID FROM KOK_CLASSIFY);
                  """)
    cur_s.close()
    conn_s.close()

def main():
    create_cls_tables()
    fct_to_cls()   

if __name__ == "__main__":
    main()