import pandas as pd
import mariadb
import utils.utils as utils

conn, cur = utils.con_to_maria_service()

def insert_food_class():
    csv = pd.read_csv('./models/ODS_HOMESHOPPING_LIST_MODIFY.csv', encoding = 'utf-8')

    df = csv[['PRODUCT_ID', 'PRODUCT_NAME', 'IS_FOOD']]

    df.loc[:, "IS_FOOD"] = df["IS_FOOD"].map({"식품": 1, "비식품": 0})
    df2 = df.copy()
    col_list = list(df.columns)

    df2.drop_duplicates(subset=["PRODUCT_ID"], keep="first", inplace=True)

    print(df.count())
    print(df2.count())
    for _, row in df.iterrows():
        values = [row[col] for col in col_list]
        cur.execute("""
                INSERT IGNORE INTO HOMESHOPPING_CLASSIFY (PRODUCT_ID, PRODUCT_NAME, CLS_FOOD)
                    VALUES (%s, %s, %s);
                    """, values)
        
def insert_ingr_class():
    csv = pd.read_csv('./models/ODS_HOMESHOPPING_LIST_WITH_LABELS_thr06.csv', encoding = 'utf-8')
    df = csv[['PRODUCT_ID', 'IS_ING_PRED']]
    df.loc[:, "IS_ING_PRED"] = df["IS_ING_PRED"].map({"식재료": 1, "완제품": 0})
    df.drop_duplicates(subset=["PRODUCT_ID"], keep="first", inplace=True)
    for _, row in df.iterrows():
        cur.execute("""
            UPDATE HOMESHOPPING_CLASSIFY
            SET CLS_ING = %s
            WHERE PRODUCT_ID = %s;
        """, (row["IS_ING_PRED"], row["PRODUCT_ID"]))

def insert_kok_ingr_class():
    csv = pd.read_csv('./models/kok_modify_labeled_only_food.csv', encoding = 'utf-8')
    df = csv[['KOK_PRODUCT_ID','KOK_PRODUCT_NAME','KOK_STORE_NAME','IS_ING_PRED']]
    df.loc[:, "IS_ING_PRED"] = df["IS_ING_PRED"].map({"식재료": 1, "완제품": 0})
    df.drop_duplicates(subset=["KOK_PRODUCT_ID"], keep="first", inplace=True)
    df = df.where(pd.notnull(df), None)
    col_list = list(df.columns)
    
    for _, row in df.iterrows():
        values = [row[col] for col in col_list]
        cur.execute("""
            INSERT IGNORE INTO KOK_CLASSIFY (PRODUCT_ID, PRODUCT_NAME, STORE_NAME, CLS_ING)
                    VALUES (%s, %s, %s, %s);
        """, values)

if __name__ == "__main__":
    conn, cur = utils.con_to_maria_service()
    insert_kok_ingr_class()
    cur.close()
    conn.close()