from bs4 import BeautifulSoup
import base64
import pandas as pd
import os
import time
from playwright.sync_api import sync_playwright
import ETL.utils.utils as utils
import ETL.preprocessing.preprocessing_kok as prkok
import random

def create_tables_ods_kok():
    # ODS 테이블 생성 쿼리
    create_ods1 = '''
        CREATE TABLE IF NOT EXISTS ODS_KOK_PRODUCT_INFO (
            KOK_PRODUCT_ID VARCHAR(20) PRIMARY KEY,
            KOK_STORE_NAME TEXT,
            KOK_PRODUCT_NAME TEXT,
            KOK_THUMBNAIL TEXT,
            KOK_PRODUCT_PRICE VARCHAR(11),
            KOK_REVIEW_SCORE VARCHAR(11),
            KOK_REVIEW_CNT VARCHAR(11),
            KOK_5_RATIO VARCHAR(11),
            KOK_4_RATIO VARCHAR(11),
            KOK_3_RATIO VARCHAR(11),
            KOK_2_RATIO VARCHAR(11),
            KOK_1_RATIO VARCHAR(11),
            KOK_ASPECT_PRICE VARCHAR(20),
            KOK_ASPECT_PRICE_RATIO VARCHAR(11),
            KOK_ASPECT_DELIVERY VARCHAR(20),
            KOK_ASPECT_DELIVERY_RATIO VARCHAR(11),
            KOK_ASPECT_TASTE VARCHAR(20),
            KOK_ASPECT_TASTE_RATIO VARCHAR(11),
            KOK_SELLER TEXT,
            KOK_CO_CEO VARCHAR(30),
            KOK_CO_REG_NO VARCHAR(15),
            KOK_CO_EC_REG VARCHAR(50),
            KOK_TELL VARCHAR(20),
            KOK_VER_ITEM VARCHAR(50),
            KOK_VER_DATE VARCHAR(50),
            KOK_CO_ADDR	TEXT,
            KOK_RETURN_ADDR	TEXT,
            KOK_EXCHANGE_ADDR TEXT,
            CREATED_AT TIMESTAMP NOT NULL DEFAULT current_timestamp()
        );
    '''
    create_ods2 = '''
        CREATE TABLE IF NOT EXISTS ODS_KOK_IMAGE_INFO (
            KOK_IMG_ID INT AUTO_INCREMENT PRIMARY KEY,
            KOK_PRODUCT_ID VARCHAR(20),
            KOK_IMG_URL TEXT
        );
    '''
    create_ods3 = '''
        CREATE TABLE IF NOT EXISTS ODS_KOK_DETAIL_INFO (
            KOK_DETAIL_COL_ID INT AUTO_INCREMENT PRIMARY KEY,
            KOK_PRODUCT_ID VARCHAR(20),
            KOK_DETAIL_COL TEXT,
            KOK_DETAIL_VAL TEXT
        );
    '''
    create_ods4 = '''
        CREATE TABLE IF NOT EXISTS ODS_KOK_REVIEW_EXAMPLE (
            KOK_REVIEW_ID INT AUTO_INCREMENT PRIMARY KEY,
            KOK_PRODUCT_ID VARCHAR(20),
            KOK_NICKNAME VARCHAR(20),
            KOK_REVIEW_DATE	VARCHAR(20),
            KOK_REVIEW_SCORE INT,
            KOK_PRICE_EVAL VARCHAR(50),
            KOK_DELIVERY_EVAL VARCHAR(50),
            KOK_TASTE_EVAL VARCHAR(50),
            KOK_REVIEW_TEXT TEXT
        );
    '''
    create_ods5 = '''
        CREATE TABLE IF NOT EXISTS ODS_KOK_PRICE_INFO (
            KOK_PRICE_ID INT AUTO_INCREMENT PRIMARY KEY,
            KOK_PRODUCT_ID VARCHAR(15),
            KOK_DISCOUNT_RATE VARCHAR(11),
            KOK_DISCOUNTED_PRICE BIGINT,
            RENEW_AT TIMESTAMP NULL DEFAULT NULL,
            CREATED_AT TIMESTAMP NOT NULL DEFAULT current_timestamp(),
            UNIQUE KEY uq_key_name (KOK_PRODUCT_ID, KOK_DISCOUNT_RATE, KOK_DISCOUNTED_PRICE)
        );
    '''
    create_view_sql = """
    CREATE OR REPLACE
    SQL SECURITY INVOKER
    VIEW V_KOK_RAWPRICE_DELTA_FAST AS
    WITH s AS (
      SELECT
        KOK_PRODUCT_ID,
        ROW_NUMBER() OVER (PARTITION BY KOK_PRODUCT_ID ORDER BY KOK_PRICE_ID DESC) AS rn,
        IFNULL(STR_TO_NUM(KOK_DISCOUNT_RATE),0) AS rate,
        ROUND(
          KOK_DISCOUNTED_PRICE * 100 /
          NULLIF(100 - IFNULL(STR_TO_NUM(KOK_DISCOUNT_RATE),0),0), 0
        ) AS est_raw
      FROM ODS_KOK_PRICE_INFO
    ),
    g AS (
      SELECT
        KOK_PRODUCT_ID,
        MAX(CASE WHEN rn=1 THEN est_raw END) AS est1,
        MAX(CASE WHEN rn=2 THEN est_raw END) AS est2,
        MAX(CASE WHEN rn=1 THEN rate   END)  AS rate1,
        MAX(CASE WHEN rn=2 THEN rate   END)  AS rate2
      FROM s
      WHERE rn <= 2
      GROUP BY KOK_PRODUCT_ID
      HAVING COUNT(*) >= 2
    )
    SELECT
      KOK_PRODUCT_ID,
      rate1 AS last_rate,
      est1  AS top_est_raw_price,
      rate2 AS before_rate,
      est2  AS second_est_raw_price,
      (est1 - est2) AS diff,
      ABS((est1 - est2) / NULLIF(est1,0)) AS diff_ratio,
      GREATEST(rate1, rate2) AS shown_rate_max,
      -- 노이즈 허용치(정수 할인율+100원 반올림 상한 + 0.1%p 여유)
      (1 / NULLIF(100 - GREATEST(rate1, rate2),0))
      + (100 / NULLIF(est1,0)) + 0.001 AS allow_max,
      CASE
        WHEN ABS((est1 - est2) / NULLIF(est1,0))
             <= ( (1 / NULLIF(100 - GREATEST(rate1, rate2),0))
                  + (100 / NULLIF(est1,0)) + 0.001 )
        THEN 'same_raw_price'
        ELSE 'changed_raw_price'
      END AS raw_price_change_flag
      FROM g;
    """

    # DESC 지정은 굳이 필요 없음(역순 스캔 가능). 인덱스 존재 시 스킵 처리
    check_index_sql = """
    SELECT 1
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'ODS_KOK_PRICE_INFO'
      AND index_name = 'idx_priceinfo_prod_price'
    LIMIT 1;
    """
    create_index_sql = """
    CREATE INDEX idx_priceinfo_prod_price
      ON ODS_KOK_PRICE_INFO (KOK_PRODUCT_ID, KOK_PRICE_ID);
    """
    # mariaDB 연결
    conn, cur = utils.con_to_maria_ods()
    # 테이블 생성
    cur.execute(create_ods1)
    cur.execute(create_ods2)
    cur.execute(create_ods3)
    cur.execute(create_ods4)
    cur.execute(create_ods5)
    cur.execute(create_view_sql)
    cur.execute(check_index_sql)
    exists = cur.fetchone()
    if not exists:
        cur.execute(create_index_sql)
    cur.close()
    conn.close()

# PRODUCT_ID 추출 > list
def crawl_product_id(soup):
    product_ids = []
    for tag in soup.find_all(attrs={"data-product-id": True}):
        product_id = tag.get("data-product-id")
        if product_id and product_id.isdigit():  # 숫자인 경우만
            product_ids.append(product_id)
    return product_ids

# KOK_PRICE_TABLE 테이블 삽입 데이터 추출 > dataframe
def crawl_sale_price_info(soup, product_ids):
    cards = soup.select("div.card.product-section-ul-li")
    data = []
    for count, card in enumerate(cards):
        if count >= len(product_ids):
            break
        product_id = product_ids[count]
        discount_tag = card.select_one(".card-info_rate")
        discount = discount_tag.get_text(strip=True) if discount_tag else None
        price_tag = card.select_one(".card-info_price strong")
        price = price_tag.get_text(strip=True).replace(",", "") if price_tag else None
        data.append({
            "KOK_PRODUCT_ID": product_id,
            "KOK_DISCOUNT_RATE": discount,
            "KOK_DISCOUNTED_PRICE": int(price) if price and price.isdigit() else price
        })
    if not data:
        return pd.DataFrame(columns=[
            "KOK_PRODUCT_ID", "KOK_DISCOUNT_RATE", "KOK_DISCOUNTED_PRICE"
        ])
    return pd.DataFrame(data)

# KOK_PRODUCT_INFO 테이블 삽입 데이터 추출 > dataframe
def crawl_basic_info(soup, product_id):
    brand = soup.select_one("span.product__title > strong")
    product_name = soup.select_one("span.product__title")
    original_price = soup.select_one("div.card-info-del_wrap del")
    rating = soup.select_one("button#review_show span")
    review_count = soup.select_one("button#review_show strong")
    thumb_tag = soup.select_one("div.card_main-image img")
    thumb_url = thumb_tag.get("src") if thumb_tag else None
    if not thumb_url or thumb_url == "" or "about:blank" in thumb_url:
        og_image_tag = soup.select_one("meta[property='og:image']")
        thumb_url = og_image_tag.get("content") if og_image_tag and og_image_tag.get("content") else None
    table = soup.find("table", class_="row-table mb--40")
    td_list = table.find_all("td") if table else []
    values = [td.get_text(strip=True) for td in td_list] + [None] * (10 - len(td_list))

    score_percents = [div.get_text(strip=True) for div in soup.select("div.review_info_list ul li.per > div")[:5]] + [None] * 5
    aspect_values = [div.get_text(strip=True) for div in soup.select("div.review_info_list ul li.info > div")] + [None] * 3
    aspect_percents = [div.get_text(strip=True) for div in soup.select("div.review_info_list ul li.per > div")[5:8]] + [None] * 3

    basic_info = [{
        "KOK_PRODUCT_ID": product_id,
        "KOK_STORE_NAME": brand.get_text(strip=True) if brand else None,
        "KOK_PRODUCT_NAME": product_name.get_text(strip=True) if product_name else None,
        "KOK_THUMBNAIL": thumb_url if thumb_url else None,
        "KOK_PRODUCT_PRICE": original_price.get_text(strip=True) if original_price else None,
        "KOK_REVIEW_SCORE": rating.get_text(strip=True) if rating else None,
        "KOK_REVIEW_CNT": review_count.get_text(strip=True) if review_count else None,
        "KOK_5_RATIO": score_percents[0] if score_percents[0] else None, 
        "KOK_4_RATIO": score_percents[1] if score_percents[1] else None, 
        "KOK_3_RATIO": score_percents[2] if score_percents[2] else None, 
        "KOK_2_RATIO": score_percents[3] if score_percents[3] else None, 
        "KOK_1_RATIO": score_percents[4] if score_percents[4] else None,
        "KOK_ASPECT_PRICE": aspect_values[0] if aspect_values[0] else None, 
        "KOK_ASPECT_PRICE_RATIO": aspect_percents[0] if aspect_percents[0] else None,
        "KOK_ASPECT_DELIVERY": aspect_values[1] if aspect_values[1] else None, 
        "KOK_ASPECT_DELIVERY_RATIO": aspect_percents[1] if aspect_percents[1] else None,
        "KOK_ASPECT_TASTE": aspect_values[2] if aspect_values[2] else None, 
        "KOK_ASPECT_TASTE_RATIO": aspect_percents[2] if aspect_percents[2] else None,
        "KOK_SELLER": values[0] if values[0] else None, 
        "KOK_CO_CEO": values[1] if values[1] else None, 
        "KOK_CO_REG_NO": values[2] if values[2] else None, 
        "KOK_CO_EC_REG": values[3] if values[3] else None,
        "KOK_TELL": values[4] if values[4] else None, 
        "KOK_VER_ITEM": values[5] if values[5] else None, 
        "KOK_VER_DATE": values[6] if values[6] else None,
        "KOK_CO_ADDR": values[7] if values[7] else None, 
        "KOK_RETURN_ADDR": values[8] if values[8] else None, 
        "KOK_EXCHANGE_ADDR": values[9] if values[9] else None
    }]
    if not basic_info:
        return pd.DataFrame(columns=[
        "KOK_PRODUCT_ID",
        "KOK_STORE_NAME",
        "KOK_PRODUCT_NAME",
        "KOK_THUMBNAIL",
        "KOK_PRODUCT_PRICE",
        "KOK_REVIEW_SCORE",
        "KOK_REVIEW_CNT",
        "KOK_5_RATIO",
        "KOK_4_RATIO",
        "KOK_3_RATIO",
        "KOK_2_RATIO",
        "KOK_1_RATIO",
        "KOK_ASPECT_PRICE",
        "KOK_ASPECT_PRICE_RATIO",
        "KOK_ASPECT_DELIVERY",
        "KOK_ASPECT_DELIVERY_RATIO",
        "KOK_ASPECT_TASTE",
        "KOK_ASPECT_TASTE_RATIO",
        "KOK_SELLER",
        "KOK_CO_CEO",
        "KOK_CO_REG_NO",
        "KOK_CO_EC_REG",
        "KOK_TELL",
        "KOK_VER_ITEM",
        "KOK_VER_DATE",
        "KOK_CO_ADDR",
        "KOK_RETURN_ADDR", 
        "KOK_EXCHANGE_ADDR",
        ])

    df = pd.DataFrame(basic_info, dtype=object)
    df = df.where(pd.notna(df), None)
    return df

# KOK_DETAIL_INFO 테이블 삽입 데이터 추출 > dataframe
def crawl_detail_info(soup: BeautifulSoup, product_code: str) -> pd.DataFrame:
    result = []
    tables = soup.find_all("table", class_="row-table mb--40")
    if len(tables) < 2:
        return pd.DataFrame(columns=["code", "col", "val"])
    rows = tables[1].find_all("tr")
    for row in rows:
        th = row.find("th")
        td = row.find("td")
        if th and td:
            result.append({"KOK_PRODUCT_ID": product_code, 
                           "KOK_DETAIL_COL": th.get_text(strip=True), 
                           "KOK_DETAIL_VAL": td.get_text(strip=True)})
    if not result:
        return pd.DataFrame(columns=[
            "KOK_PRODUCT_ID", "KOK_DETAIL_COL", "KOK_DETAIL_VAL"
        ])
    return pd.DataFrame(result)

# KOK_REVIEW_EXAMPLE 테이블 삽입 데이터 추출 > dataframe
def crawl_personal_review(soup, product_id):
    review_data = []
    review_items = soup.select("li.review_info_info_item")[:10]
    for item in review_items:
        nickname = item.select_one("div.phone")
        date = item.select_one("div.date")
        stars = len(item.select("div.stars img[src*='s_star_fill']"))
        rating_detail = {
            li.select_one("p").get_text(strip=True): li.select_one("span").get_text(strip=True)
            for li in item.select("ul.list > li") if li.select_one("p") and li.select_one("span")
        }
        text = item.select_one("div.text")
        review_data.append({
            "KOK_PRODUCT_ID": product_id,
            "KOK_NICKNAME": nickname.get_text(strip=True) if nickname else None,
            "KOK_REVIEW_DATE": date.get_text(strip=True) if date else None,
            "KOK_REVIEW_SCORE": stars,
            "KOK_PRICE_EVAL": rating_detail.get("가격"),
            "KOK_DELIVERY_EVAL": rating_detail.get("배송"),
            "KOK_TASTE_EVAL": rating_detail.get("맛"),
            "KOK_REVIEW_TEXT": text.get_text(strip=True) if text else None
        })
    if not review_data:
        return pd.DataFrame(columns=[
            "KOK_PRODUCT_ID", "KOK_NICKNAME", "KOK_REVIEW_DATE", "KOK_REVIEW_SCORE",
            "KOK_PRICE_EVAL", "KOK_DELIVERY_EVAL", "KOK_TASTE_EVAL", "KOK_REVIEW_TEXT"
        ])
    return pd.DataFrame(review_data)

# KOK_IMAGE_INFO 테이블 삽입 데이터 추출 > dataframe
def crawl_img_src(soup, product_code):
    product_info = soup.select_one("div.product_info_area")
    image_urls = []
    if product_info:
        imgs = product_info.find_all("img")
        for img in imgs:
            src = img.get("src", "")
            if src and not src.lower().endswith(".gif"):
                image_urls.append({"KOK_PRODUCT_ID": product_code, 
                                   "KOK_IMG_URL": src})
    if not image_urls:
        return pd.DataFrame(columns=[
            "KOK_PRODUCT_ID", "KOK_IMG_URL"
        ])
    return pd.DataFrame(image_urls)

# 크롤링 데이터 txt로 저장
def save_product_output(product_id, meta_dict, table_df, review_df, imglist_df):
    output_dir = f"outputs/{product_id}"
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "meta.txt"), "w", encoding="utf-8") as f:
        for key, val in meta_dict.items():
            f.write(f"{key}: {val}\n")
    table_df.to_csv(os.path.join(output_dir, "table.txt"), sep="\t", index=False, encoding="utf-8")
    review_df.to_csv(os.path.join(output_dir, "review.txt"), sep="\t", index=False, encoding="utf-8")
    imglist_df.to_csv(os.path.join(output_dir, "imglist.txt"), sep="\t", index=False, encoding="utf-8")

# 각 데이터 타입 확인
def check_type(a):
    if isinstance(a, dict):
        print("[KOK] keys and types:")
        for key, val in a.items():
            print(f" ✔ {key}: {type(val)}")
    elif isinstance(a, pd.DataFrame):
        print('----------------------')
        print(f'{a.dtypes}')
        print('----------------------')
    elif isinstance(a, list) and a:
        print(f" ✔ list : {type(a[0])}")

# 가격정보 크롤링 (상품 리스트)
def crawl_kok_price():
    conn, cur = utils.con_to_maria_ods()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

        for i in range(11, 25):
            if i == 12:
                continue
            page = 1
            while True:
                code = f'16{i}'
                version = 202507311600
                l_url = f'https://kok.uplus.co.kr/m/event/category/uplus/page?code={code}&page={page}&size=20&sort=recommend_asc&minPrice=0&maxPrice=9999999999&version={version}&'

                print(f"[KOK] 카테고리 {i} - 페이지 {page}")
                page_l = context.new_page()
                page_l.goto(l_url)
                time.sleep(random.uniform(1, 1.5))
                html = page_l.content()
                l_soup = BeautifulSoup(html, "html.parser")
                page_l.close()

                product_ids = crawl_product_id(l_soup)
                if len(product_ids) == 0:
                    break
                price_info = crawl_sale_price_info(l_soup, product_ids)
                utils.insert_df_into_db(conn, price_info, "ODS_KOK_PRICE_INFO")
                if len(product_ids) < 20:
                    break
                page += 1
    cur.close()
    conn.close()
# 상품 상세 정보 크롤링
def crawling_kok_detail():
    conn, cur = utils.con_to_maria_ods()
    cur.execute('''
                SELECT DISTINCT KOK_PRODUCT_ID FROM ODS_KOK_PRICE_INFO
                WHERE KOK_PRODUCT_ID NOT IN (SELECT DISTINCT KOK_PRODUCT_ID FROM ODS_KOK_PRODUCT_INFO);
                ''')
    rows = cur.fetchall()
    product_ids = [row[0] for row in rows]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
        basic_info_dump = pd.DataFrame()
        detail_info_dump = pd.DataFrame()
        personal_review_dump = pd.DataFrame()
        img_urls_dump = pd.DataFrame()
        reset_count = 0

        pr_count = 1
        error = 0 

        for pid in product_ids:
            # 10개마다 데이터프레임 초기화 및 적재
            if reset_count == 10:
                utils.insert_df_into_db(conn, basic_info_dump, "ODS_KOK_PRODUCT_INFO")
                # basic_info에 들어간 ID만 기준으로 나머지 정리
                inserted_ids = set(basic_info_dump["KOK_PRODUCT_ID"].unique())

                detail_info_dump = detail_info_dump[detail_info_dump["KOK_PRODUCT_ID"].isin(inserted_ids)]
                personal_review_dump = personal_review_dump[personal_review_dump["KOK_PRODUCT_ID"].isin(inserted_ids)]
                img_urls_dump = img_urls_dump[img_urls_dump["KOK_PRODUCT_ID"].isin(inserted_ids)]

                # ✅ 긴 텍스트 잘라서 오류 방지
                detail_info_dump["KOK_DETAIL_VAL"] = detail_info_dump["KOK_DETAIL_VAL"].astype(str).str[:4000]
                detail_info_dump["KOK_DETAIL_COL"] = detail_info_dump["KOK_DETAIL_COL"].astype(str).str[:1000]

                utils.insert_df_into_db(conn, detail_info_dump, "ODS_KOK_DETAIL_INFO")
                utils.insert_df_into_db(conn, personal_review_dump, "ODS_KOK_REVIEW_EXAMPLE")
                utils.insert_df_into_db(conn, img_urls_dump, "ODS_KOK_IMAGE_INFO")

                # 데이터프레임 초기화
                basic_info_dump = pd.DataFrame()
                detail_info_dump = pd.DataFrame()
                personal_review_dump = pd.DataFrame()
                img_urls_dump = pd.DataFrame()
                reset_count = 0

                print('[KOK] 중간 적재')
            else : 
                pass

            try:
                print(f"[KOK] {pr_count}. 상품 ID: {pid}")
                proCode = base64.b64encode(pid.encode("utf-8")).decode("utf-8")
                p_url = f'https://kok.uplus.co.kr/m/product?proCode={proCode}'

                page_d = context.new_page()
                page_d.goto(p_url, timeout=30000)
                for sel in [
                    "span.product__title",
                    "div.card-info.mt--4",
                    "div.card_main-image img",
                    "table.row-table.mb--40",  # 판매자정보 테이블
                    "div.product_info_area img",  # 이미지 리스트 영역
                    "div.tab_info.product_more",  # 상품정보제공 고시 outer div
                    "div.heading_4Sb.mb--16",  # '상품정보제공 고시' 제목
                    "div.pro_detail_buy_table_liner + table.row-table"  # 고시 항목 테이블
                ]:
                    page_d.wait_for_selector(sel, state="attached", timeout=30000)

                time.sleep(random.uniform(1, 1.5))
                html = page_d.content()
                p_soup = BeautifulSoup(html, "html.parser")
                page_d.close()

                basic_info = crawl_basic_info(p_soup, pid)
                detail_info = crawl_detail_info(p_soup, pid)
                personal_review = crawl_personal_review(p_soup, pid)
                img_urls = crawl_img_src(p_soup, pid)

                basic_info_dump = pd.concat([basic_info_dump, basic_info], ignore_index=True)
                detail_info_dump = pd.concat([detail_info_dump, detail_info], ignore_index=True)
                personal_review_dump = pd.concat([personal_review_dump, personal_review], ignore_index=True)
                img_urls_dump = pd.concat([img_urls_dump, img_urls], ignore_index=True)

                pr_count += 1
                reset_count += 1

            except Exception as e:
                print(f"[KOK] [ERROR] {pid} 오류 발생: {e}")
                if 'page_d' in locals():
                    page_d.close()

                error += 1
                continue

        browser.close()
    print(f'[KOK] {pr_count}개 적재 / {error}개 오류')
    cur.close()
    conn.close()

def deal_with_changed_raw_price():
    conn_o, cur_o = utils.con_to_maria_ods()
    conn_s, cur_s = utils.con_to_maria_service()
    print('▶ [KOK] 상품 정보 변동 대응 수행')
    cur_o.execute('''
                SELECT 
                    KOK_PRODUCT_ID 
                FROM V_KOK_RAWPRICE_DELTA_FAST 
                WHERE 
                    raw_price_change_flag = 'changed_raw_price' 
                    AND 
                    KOK_PRODUCT_ID NOT IN (
                        SELECT KOK_PRODUCT_ID FROM ODS_KOK_PRICE_INFO
                        WHERE RENEW_AT IS NOT NULL);
                ''')
    rows = cur_o.fetchall()
    try:
        conn_o.autocommit = False
        conn_s.autocommit = False
        if rows:
            ch_list = ','.join(r[0] for r in rows)
            cur_o.execute(f'''
                DELETE FROM ODS_KOK_PRODUCT_INFO
                WHERE KOK_PRODUCT_ID IN ({ch_list});
            ''')
            cur_o.execute(f'''
                DELETE FROM ODS_KOK_IMAGE_INFO
                WHERE KOK_PRODUCT_ID IN ({ch_list});
            ''')
            cur_o.execute(f'''
                DELETE FROM ODS_KOK_DETAIL_INFO
                WHERE KOK_PRODUCT_ID IN ({ch_list});
            ''')
            cur_o.execute(f'''
                DELETE FROM ODS_KOK_REVIEW_EXAMPLE
                WHERE KOK_PRODUCT_ID IN ({ch_list});
            ''')
            print('[KOK] ODS 데이터 삭제')
            cur_o.execute(f'''
                UPDATE ODS_KOK_PRICE_INFO
                SET RENEW_AT = CURRENT_TIMESTAMP()
                WHERE KOK_PRICE_ID IN (
                    SELECT a.KOK_PRICE_ID FROM(
                        SELECT 
                            MAX(KOK_PRICE_ID) AS KOK_PRICE_ID, 
                            KOK_PRODUCT_ID 
                        FROM ODS_KOK_PRICE_INFO 
                        WHERE KOK_PRODUCT_ID IN ({ch_list})
                        GROUP BY KOK_PRODUCT_ID) a);
            ''')
            print('[KOK] RENEW_AT 업데이트')
            conn_o.commit()
            cur_s.execute(f'''
                DELETE FROM FCT_KOK_PRODUCT_INFO
                WHERE KOK_PRODUCT_ID IN ({ch_list});
            ''')
            print('[KOK] FCT 데이터 삭제')
            conn_s.commit()
            print('✅ [KOK] 상품 정보 변동 대응 완료')
            return [ch_list]
        else:
            return False
        
    except Exception as e:
        conn_o.rollback()
        print('❌ [KOK] 상품 정보 변동 대응 실패')
        raise

    finally:
        cur_o.close()
        cur_s.close()
        conn_s.close()
        conn_o.close()

def main():
    create_tables_ods_kok()
    crawl_kok_price()
    deal_with_changed_raw_price()
    crawling_kok_detail()

if __name__=="__main__" :
    main()