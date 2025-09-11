import requests
import time
import math
import json
import re
import numpy as np
import pandas as pd
import ETL.utils.utils as utils
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime, timezone
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import traceback
import random


# 홈쇼핑 관련 ODS 테이블 생성
def create_ods_hs():
    # 테이블 생성 쿼리
    create_homeshopping_info = '''
        CREATE TABLE IF NOT EXISTS HOMESHOPPING_INFO (
            HOMESHOPPING_ID SMALLINT PRIMARY KEY,
            HOMESHOPPING_NAME VARCHAR(20),
            HOMESHOPPING_CHANNEL SMALLINT,
            LIVE_URL VARCHAR(200)
            );
        '''
    create_ods_homeshopping_list = '''
        CREATE TABLE IF NOT EXISTS `ODS_HOMESHOPPING_LIST` (
            `LIVE_ID` INT(11) NOT NULL AUTO_INCREMENT,
            `HOMESHOPPING_ID` SMALLINT(6) NULL DEFAULT NULL,
            `LIVE_DATE` VARCHAR(20) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `LIVE_TIME` VARCHAR(20) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `PROMOTION_TYPE` ENUM('main','sub') NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `LIVE_TITLE` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `PRODUCT_ID` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `PRODUCT_NAME` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `SALE_PRICE` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `DC_PRICE` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `DC_RATE` VARCHAR(20) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `THUMB_IMG_URL` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `CREATED_AT` TIMESTAMP NOT NULL DEFAULT current_timestamp(),
            PRIMARY KEY (`LIVE_ID`) USING BTREE,
            UNIQUE INDEX `UNIQ_PROD_COL` (`HOMESHOPPING_ID`, `LIVE_DATE`, `LIVE_TIME`, `PROMOTION_TYPE`, `PRODUCT_ID`) USING BTREE
        )
        COLLATE='utf8mb4_general_ci'
        ENGINE=InnoDB
        ;
        '''
    create_ods_homeshopping_product_info = '''
        CREATE TABLE IF NOT EXISTS `ODS_HOMESHOPPING_PRODUCT_INFO` (
            `PRODUCT_ID` VARCHAR(50) NOT NULL COLLATE 'utf8mb4_general_ci',
            `HOMESHOPPING_ID` SMALLINT(6) NULL DEFAULT NULL,
            `STORE_NAME` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `PRODUCT_NAME` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `SALE_PRICE` VARCHAR(50) NOT NULL COLLATE 'utf8mb4_general_ci',
            `DC_RATE` VARCHAR(20) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `DC_PRICE` VARCHAR(50) NOT NULL COLLATE 'utf8mb4_general_ci',
            `DELIVERY_FEE` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `DELIVERY_CO` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `RETURN_EXCHANGE` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `TERM` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `CREATED_AT` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`PRODUCT_ID`) USING BTREE,
            INDEX `HOMESHOPPING_ID` (`HOMESHOPPING_ID`) USING BTREE,
            CONSTRAINT `ODS_HOMESHOPPING_PRODUCT_INFO_ibfk_1` FOREIGN KEY (`HOMESHOPPING_ID`) REFERENCES `HOMESHOPPING_INFO` (`HOMESHOPPING_ID`) ON UPDATE RESTRICT ON DELETE RESTRICT
        )
        COLLATE='utf8mb4_general_ci'
        ENGINE=InnoDB
        ;
        '''
    create_ods_homeshopping_img_url = '''
        CREATE TABLE IF NOT EXISTS `ODS_HOMESHOPPING_IMG_URL` (
            `IMG_ID` INT(11) NOT NULL AUTO_INCREMENT,
            `PRODUCT_ID` VARCHAR(20) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `SORT_ORDER` SMALLINT(6) NULL DEFAULT NULL,
            `IMG_URL` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            PRIMARY KEY (`IMG_ID`) USING BTREE,
            UNIQUE INDEX `UK_COL` (`PRODUCT_ID`, `IMG_URL`) USING HASH
        )
        COLLATE='utf8mb4_general_ci'
        ENGINE=InnoDB
        ;
        '''
    create_ods_homeshopping_detail_info = '''
        CREATE TABLE IF NOT EXISTS `ODS_HOMESHOPPING_DETAIL_INFO` (
            `DETAIL_ID` INT(11) NOT NULL AUTO_INCREMENT,
            `PRODUCT_ID` VARCHAR(20) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `DETAIL_COL` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            `DETAIL_VAL` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
            PRIMARY KEY (`DETAIL_ID`) USING BTREE,
            UNIQUE INDEX `UK_COL` (`PRODUCT_ID`, `DETAIL_COL`) USING HASH
        )
        COLLATE='utf8mb4_general_ci'
        ENGINE=InnoDB
        ;
        '''
    create_homeshopping_current_schedule = '''
        CREATE TABLE IF NOT EXISTS ODS_HOMESHOPPING_CURRENT_SCHEDULE (
            HOMESHOPPING_ID SMALLINT,
            LIVE_DATE VARCHAR(15),
            LIVE_TIME VARCHAR(15),
            PRODUCT_ID VARCHAR(20),
            FOREIGN KEY (HOMESHOPPING_ID) REFERENCES HOMESHOPPING_INFO (HOMESHOPPING_ID)
        );
    '''
    check_info_count = '''SELECT COUNT(*) FROM HOMESHOPPING_INFO'''
    # mariaDB 연결
    conn, cur = utils.con_to_maria_ods()
    # 인포테이블 생성 (홈쇼핑 채널 정보 테이블)
    cur.execute(create_homeshopping_info)
    cur.execute(check_info_count)
    count = cur.fetchall()[0][0]
    # 채널 정보 insert
    if count == 0:
        cur.execute('''
            INSERT IGNORE INTO HOMESHOPPING_INFO 
                    (HOMESHOPPING_ID, HOMESHOPPING_NAME, HOMESHOPPING_CHANNEL, LIVE_URL)
            VALUES 
             (1, '홈앤쇼핑', 4, 'https://s32.qtcdn.co.kr/media/liveM3U8/idx/669128309/enc/129701903/playlist.m3u8'),
             (2, '현대홈쇼핑', 12, 'https://cdnlive.hmall.com/live/hmall.stream/chunklist.m3u8'),
             (3, '현대홈쇼핑+샵', 34, 'https://dtvstreaming.hyundaihmall.com/newcjp3/cjpstream_1M/chunklist.m3u8'),
             (4, 'NS홈쇼핑', 13, 'https://livestream.nsmall.com/IPHONE/nsmallMobile.m3u8'),
             (5, 'NS샵+', 41, 'https://shoppstream.nsmall.com/IPHONE/mobile.m3u8'),
             (6, '공영쇼핑', 20, NULL),
             (7, 'GS샵 LIVE', 6, NULL),
             (8, 'GS마이샵', 30, NULL),
             (9, 'CJ온스타일', 8, NULL),
             (10, 'CJ온스타일플러스', 32, NULL),
             (11, '롯데홈쇼핑', 10, NULL),
             (12, '롯데 OneTV', 36, NULL),
             (13, 'KT 알파쇼핑', 2, NULL),
             (14, 'SK 스토어', 17, NULL),
             (15, '신세계쇼핑', 21, NULL),
             (16, 'W쇼핑', 28, NULL),
             (17, '쇼핑엔터', 37, NULL);
            ''')
    else:
        pass
    # 홈쇼핑 ODS TABLE 생성
    cur.execute(create_ods_homeshopping_list)
    cur.execute(create_ods_homeshopping_product_info)
    cur.execute(create_ods_homeshopping_img_url)
    cur.execute(create_ods_homeshopping_detail_info)
    cur.execute(create_homeshopping_current_schedule)
    cur.execute('''
        DELETE FROM ODS_HOMESHOPPING_CURRENT_SCHEDULE;
    ''')

    cur.close()
    conn.close()

'''
홈앤쇼핑 크롤링 함수 
crawl_hns
crawl_hns_detail
'''
# 홈앤쇼핑 편성표 크롤링, 데이터 INSERT TO ODS
# HOMESHOPPING_ID = 1
def crawl_hns():
    def crawl_schedule_page(html):
        soup = BeautifulSoup(html, "html.parser")
        items = soup.select("li.item")
        results = []
        live_date = soup.select_one('div#scheduleWrap')['date']

        for item in items:
            dc_rate = None
            time_range = item.select_one(".live-time span").text.strip()
            if time_range =='지금방송중':
                KST = timezone(timedelta(hours=9))
                st_time = datetime.now(KST).strftime("%H:%M")
                end_time = datetime.now(KST).strftime("%H:%M")

                on_air_div = soup.select_one("#onAirTime")
                if on_air_div and on_air_div.has_attr("bdEtimeSecond"):
                    end_time_full = on_air_div["bdEtimeSecond"]
                    end_time = end_time_full.strip().split(" ")[1][:5]
                time_range = st_time + ' ~ ' + end_time
            show_title = item.select_one(".live-time p.time").text.strip().replace(time_range, "").strip()

            product_id = item.select_one("a.goods-info")["onclick"].split("'")[1]
            title = item.select_one(".tit").text.strip()
            price_block = item.select_one(".price")
            price_tag = price_block.select_one("strong") if price_block else None
            rate_block = item.select_one(".rate")
            rate_tag = rate_block.select_one("span") if rate_block else None
            if price_tag:
                price = price_tag.text.strip()
            else:
                consult_tag = price_block.select_one("p.counselPrd") if price_block else None
                price = consult_tag.text.strip() if consult_tag else None  # e.g., "상담 예약 상품"

            dc_rate = rate_tag.text.strip() if rate_tag else None
            if dc_rate is not None and (dc_rate == "" or dc_rate.lower() == "nan" or (isinstance(dc_rate, float) and math.isnan(dc_rate))):
                dc_rate = None
            img = item.select_one(".goods-thumb img")["src"]
            img_url = "https:" + img if img.startswith("//") else img

            results.append({
                "HOMESHOPPING_ID" : 1,
                "LIVE_DATE" : live_date,
                "LIVE_TIME": time_range,
                "PROMOTION_TYPE": "main",
                "LIVE_TITLE": show_title,
                "PRODUCT_ID": product_id,
                "PRODUCT_NAME": title,
                "DC_PRICE": price,
                "DC_RATE": dc_rate if dc_rate else None,
                "THUMB_IMG_URL": img_url
            })

            sub_items = item.select(".sub-prd ul li")
            for sub in sub_items:
                sub_dc_rate = None
                sub_product_id = sub.select_one("a")["onclick"].split("'")[1]
                sub_title = sub.select_one(".tit").text.strip()
                sub_price_block = sub.select_one(".price")
                sub_price_tag = sub_price_block.select_one("strong") if sub_price_block else None
                sub_rate_block = sub.select_one(".rate")
                sub_rate_tag = sub_rate_block.select_one("span") if sub_rate_block else None
                if sub_price_tag:
                    sub_price = sub_price_tag.text.strip()
                else:
                    sub_consult_tag = sub_price_block.select_one("p.counselPrd") if sub_price_block else None
                    sub_price = sub_consult_tag.text.strip() if sub_consult_tag else None
                sub_dc_rate = sub_rate_tag.text.strip() if sub_rate_tag else None
                if sub_dc_rate is not None and (sub_dc_rate == "" or sub_dc_rate.lower() == "nan" or (isinstance(sub_dc_rate, float) and math.isnan(sub_dc_rate))):
                    sub_dc_rate = None
                sub_img = sub.select_one("img")["src"]
                sub_img_url = "https:" + sub_img if sub_img.startswith("//") else sub_img

                results.append({
                    "HOMESHOPPING_ID" : 1,
                    "LIVE_DATE" : live_date,
                    "LIVE_TIME": time_range,
                    "PROMOTION_TYPE": "sub",
                    "LIVE_TITLE": show_title,
                    "PRODUCT_ID": sub_product_id,
                    "PRODUCT_NAME": sub_title,
                    "DC_PRICE": sub_price,
                    "DC_RATE": sub_dc_rate if sub_dc_rate else None,
                    "THUMB_IMG_URL": sub_img_url,
                })
            time_check_point = time_range[time_range.find('~')+2:].strip() if time_range.find('~') else '지금방송중'
        return pd.DataFrame(results)
    conn, cur = utils.con_to_maria_ods()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

        for i in range(11):
            today = date.today()
            current = today + timedelta(days=i)
            formatted = current.strftime('%Y%m%d')
            print('[HNS]', formatted, "시작")
            url = f'https://www.hnsmall.com/display/tvschedule-list?categoryCode=60000059&areaCode=8005369&broadDay={formatted}'

            page = context.new_page()
            page.goto(url, timeout=60000)
            page.wait_for_timeout(5000)
            html = page.content()
            page.close()

            df = crawl_schedule_page(html)
            utils.insert_df_into_db(conn, df, 'ODS_HOMESHOPPING_LIST', 'IGNORE')
            current_schedule = df[['HOMESHOPPING_ID','LIVE_DATE','LIVE_TIME','PRODUCT_ID']]
            utils.insert_df_into_db(conn, current_schedule, 'ODS_HOMESHOPPING_CURRENT_SCHEDULE')
            print('[HNS]', formatted, '종료')
        browser.close()
    cur.close()
    conn.close()

# 수정 홈앤쇼핑 디테일 데이터 크롤링 코드
def crawl_hns_detail(
    homeshopping_id: int,
    limit: int | None = None,
    batch_size: int = 10,
    headless: bool = True,
    through_value: str | None = None,
    debug_dump_dir: str | None = None,
):
    """
    NS(=hnsmall) 상품 상세/이미지/가격을 수집하여 ODS 테이블 3종에 적재.
    - homeshopping_id: ODS_HOMESHOPPING_LIST 기준 필터
    - limit: 수집할 PRODUCT_ID 개수 제한 (None이면 제한 없음)
    - batch_size: N개마다 DB 적재
    - headless: 브라우저 헤드리스 옵션
    - through_value: 디테일컷 <picture[through="..."]> 필터. None이면 모든 through 허용
    - debug_dump_dir: HTML 덤프 경로 (None이면 덤프 안함)
    """

    # ------------------
    # 내부 유틸
    # ------------------
    def dump_html(name: str, html: str):
        if not debug_dump_dir:
            return
        try:
            os.makedirs(debug_dump_dir, exist_ok=True)
            path = os.path.join(debug_dump_dir, name)
            with open(path, "w", encoding="utf-8") as f:
                f.write(html)
        except Exception as e:
            print(f"[HNS-DETAIL] [WARN] dump_html fail {name}: {e}")
 
    def _parse_item_tables_from_soup(soup: BeautifulSoup, product_id: str) -> list[dict]:
        rows = []
        # 클래스 변형 대응: v2 우선, 없으면 대체 테이블들도 허용
        for tbl in soup.select("table.itemTableRow.v2, table.itemTableRow, table.itemInfoRow"):
            for tr in tbl.select("tbody tr"):
                th = tr.find("th")
                td = tr.find("td")
                # <th> 없이 <td>만 있는 행 대비(첫 td를 col로 보고, 두번째 td를 값으로)
                if not th and td:
                    tds = tr.find_all("td")
                    if len(tds) >= 2:
                        th = tds[0]
                        td = tds[1]
                if not th or not td:
                    continue
                col = utils._clean_text(th.get_text(" ", strip=True))
                val = utils._clean_text(td.get_text(" ", strip=True))
                if col or val:
                    rows.append({"PRODUCT_ID": product_id, "DETAIL_COL": col, "DETAIL_VAL": val})
        return rows

    def _with_page(func):
        """브라우저/페이지 열고 닫는 보일러플레이트"""
        def wrapper(url: str):
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=headless)
                page = browser.new_page()
                page.goto(url, timeout=60_000, wait_until="domcontentloaded")
                try:
                    result = func(page)
                finally:
                    browser.close()
            return result
        return wrapper

    @_with_page
    def crawl_product_detail_page(page) -> pd.DataFrame:
        detail_rows: list[dict] = []

        # 탭 바가 보일 때까지 대기
        try:
            page.wait_for_selector("ul.tabList", timeout=15_000)
        except:
            pass

        # 탭1 열기
        try:
            page.locator('ul.tabList a[href="#tab1Cont"]').first.click(timeout=5_000)
        except:
            try:
                page.locator("ul.tabList li >> nth=0 button, ul.tabList li >> nth=0 a").click(timeout=5_000)
            except:
                pass

        # iframe 우선
        try:
            iframe_el = page.wait_for_selector('iframe#goodsDescribeView', state="attached", timeout=10_000)
            frame = iframe_el.content_frame()
            for _ in range(10):
                if frame is not None:
                    break
                time.sleep(random.uniform(1, 1.5))
                frame = iframe_el.content_frame()
            if frame:
                frame.wait_for_load_state("domcontentloaded", timeout=10_000)
                frame.wait_for_selector("body", timeout=10_000)
                frame_html = frame.content()
                dump_html(f"{product_id}_tab1_iframe.html", frame_html)
                soup_iframe = BeautifulSoup(frame_html, "html.parser")
                detail_rows += _parse_item_tables_from_soup(soup_iframe, product_id)
        except:
            pass

        # iframe에서 못 찾았거나 빈 경우 → 탭1 본문 파싱
        if not detail_rows:
            try:
                page.wait_for_selector("#tab1Cont", timeout=10_000)
                html_tab1 = page.locator("#tab1Cont").inner_html()
                dump_html(f"{product_id}_tab1.html", html_tab1)
                soup_tab1 = BeautifulSoup(html_tab1, "html.parser")
                detail_rows += _parse_item_tables_from_soup(soup_tab1, product_id)
            except:
                pass

        # 탭2(구매정보)
        try:
            page.locator('ul.tabList a[href="#tab2Cont"]').first.click(timeout=5_000)
        except:
            try:
                page.locator("ul.tabList li >> nth=1 button, ul.tabList li >> nth=1 a").click(timeout=5_000)
            except:
                pass

        try:
            page.wait_for_selector("#tab2Cont", timeout=10_000)
            html_tab2 = page.locator("#tab2Cont").inner_html()
            dump_html(f"{product_id}_tab2.html", html_tab2)
            soup_tab2 = BeautifulSoup(html_tab2, "html.parser")
            detail_rows += _parse_item_tables_from_soup(soup_tab2, product_id)
        except:
            pass

        df = pd.DataFrame(detail_rows, columns=["PRODUCT_ID", "DETAIL_COL", "DETAIL_VAL"])
        return df

    def _normalize_and_filter_urls(urls: list[str]) -> list[str]:
        out = []
        seen = set()
        for u in urls:
            if not u:
                continue
            u = u.strip()
            # protocol-relative -> https:
            if u.startswith("//"):
                u = "https:" + u
            # data:나 svg 같은 건 제외(원하면 주석 해제)
            if u.startswith("data:"):
                continue
            # 확장자 필터(너무 빡세면 주석 처리)
            if not re.search(r"\.(jpe?g|png|webp)(\?|#|$)", u, re.I):
                # background-image가 webp가 아닐 수도 있어 확장자 필터를 완화하려면 위 라인 제거
                pass
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out

    def crawl_hns_detail_images(product_id: str, headless: bool = False, max_scroll_steps: int = 8) -> pd.DataFrame:
        """
        - 탭1(상품설명) iframe 내부에서 이미지 URL을 가능한 모든 방식으로 수집:
        <img src|data-src|data-original|...>, <source srcset>, style="background-image:url(...)"
        - 스크롤 몇 번 내려서 lazy 이미지까지 긁음(속도/부하 고려해 steps 조절)
        """
        url = f"https://www.hnsmall.com/display/goods.do?goods_code={product_id}"

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            page = browser.new_page()
            if not isinstance(url, str):
                raise TypeError(f"url must be str, got {type(url)}")  # 디버깅용
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)

            # 탭1 열기(상품설명)
            try:
                page.locator('ul.tabList a[href="#tab1Cont"]').first.click(timeout=5_000)
            except:
                try:
                    page.locator("ul.tabList li >> nth=0 button, ul.tabList li >> nth=0 a").click(timeout=5_000)
                except:
                    pass

            # iframe 취득(여기서 종종 None이 뜨니 재시도)
            frame = None
            try:
                iframe_el = page.wait_for_selector('iframe#goodsDescribeView', state="attached", timeout=15_000)
                for _ in range(10):
                    frame = iframe_el.content_frame()
                    if frame:
                        break
                    time.sleep(0.3)
            except:
                frame = None

            # 폴백: 탭 본문에서 직접
            def _collect_from_html(html: str, base_href: str) -> list[str]:
                soup = BeautifulSoup(html, "html.parser")
                urls = []

                # <img>의 여러 lazy 속성 커버
                for img in soup.select("img"):
                    cand = [
                        img.get("src"),
                        img.get("data-src"),
                        img.get("data-original"),
                        img.get("data-lazy"),
                        img.get("data-echo"),
                        img.get("data-image"),
                        img.get("data-img"),
                    ]
                    for c in cand:
                        if c:
                            urls.append(c)

                # <source srcset> / <img srcset>
                for el in soup.select("source[srcset], img[srcset]"):
                    srcset = el.get("srcset") or ""
                    # "url1 320w, url2 640w ..." -> 가장 오른쪽(보통 가장 큰) pick
                    parts = [s.strip() for s in srcset.split(",") if s.strip()]
                    if parts:
                        last = parts[-1].split()[0]
                        urls.append(last)

                # CSS background-image
                for div in soup.select("[style*='background']"):
                    style = div.get("style") or ""
                    matches = re.findall(r"url\((.*?)\)", style, flags=re.I)
                    for m in matches:
                        m = m.strip(" '\"")
                        if m:
                            urls.append(m)

                # 절대화: BeautifulSoup만으로는 base 처리 어려우니 python에서 URL join
                abs_urls = []
                from urllib.parse import urljoin
                for u in urls:
                    if not u:
                        continue
                    if u.startswith("http://") or u.startswith("https://") or u.startswith("data:") or u.startswith("//"):
                        abs_urls.append(u)
                    else:
                        abs_urls.append(urljoin(base_href, u))
                return _normalize_and_filter_urls(abs_urls)

            collected = []

            if frame:
                # lazy 이미지가 스크롤 시 주입될 수 있어 몇 번 스크롤
                try:
                    frame.wait_for_selector("body", timeout=10_000)
                    for _ in range(max_scroll_steps):
                        frame.evaluate("window.scrollBy(0, document.body.scrollHeight/4)")
                        time.sleep(0.5)
                except:
                    pass

                # 프레임 안에서 JS로 직접 수집(성능/정확도↑)
                try:
                    js_urls = frame.evaluate(r"""
                    () => {
                    const urls = new Set();

                    const push = (u) => { if (u && typeof u === 'string') urls.add(u.trim()); };

                    // imgs
                    document.querySelectorAll('img').forEach(img => {
                        ['src','data-src','data-original','data-lazy','data-echo','data-image','data-img']
                        .forEach(k => push(img.getAttribute(k)));
                        const srcset = img.getAttribute('srcset');
                        if (srcset) {
                        const parts = srcset.split(',').map(s=>s.trim()).filter(Boolean);
                        if (parts.length) push(parts[parts.length-1].split(/\s+/)[0]);
                        }
                    });

                    // <source srcset>
                    document.querySelectorAll('source[srcset]').forEach(el => {
                        const srcset = el.getAttribute('srcset');
                        if (srcset) {
                        const parts = srcset.split(',').map(s=>s.trim()).filter(Boolean);
                        if (parts.length) push(parts[parts.length-1].split(/\s+/)[0]);
                        }
                    });

                    // background-image
                    document.querySelectorAll('[style*="background"]').forEach(el => {
                        const st = el.getAttribute('style') || '';
                        const m = st.match(/url\\((.*?)\\)/gi);
                        if (m) {
                        m.forEach(one => {
                            const url = one.replace(/^url\\((.*)\\)$/i, '$1').replace(/^["']|["']$/g, '').trim();
                            if (url) push(url);
                        });
                        }
                    });

                    return Array.from(urls);
                    }
                    """)
                    # 절대화 & 정리
                    base_href = frame.url
                    collected.extend(_collect_from_html(
                        "".join([f'<img src="{u}">' for u in js_urls]),  # 간단히 URL만 절대화/정리 재사용
                        base_href
                    ))
                except:
                    pass

                # 프레임 전체 HTML에서도 보조 수집(혹시 JS 수집에서 빠진 케이스)
                try:
                    frame_html = frame.content()
                    collected.extend(_collect_from_html(frame_html, frame.url))
                except:
                    pass
            else:
                # 폴백: #tab1Cont 본문에서 수집
                try:
                    page.wait_for_selector("#tab1Cont", timeout=10_000)
                    html_tab1 = page.locator("#tab1Cont").inner_html()
                    collected.extend(_collect_from_html(html_tab1, page.url))
                except:
                    pass

            browser.close()

        urls = _normalize_and_filter_urls(collected)
        if not urls:
            return pd.DataFrame(columns=["PRODUCT_ID", "SORT_ORDER", "IMG_URL"])

        df = pd.DataFrame({"PRODUCT_ID": product_id, "IMG_URL": urls})
        df = df.drop_duplicates(subset=["IMG_URL"]).reset_index(drop=True)
        df.insert(1, "SORT_ORDER", df.index + 1)
        return df[["PRODUCT_ID", "SORT_ORDER", "IMG_URL"]]


    @_with_page
    def crawl_prices_page(page) -> pd.DataFrame:
        # 가격 루트 대기(여러 패턴 허용)
        page.wait_for_selector('.resultPrice .priceTotal, .priceTotal', timeout=20_000)
        data = page.evaluate("""
        () => {
          const root = document.querySelector('.resultPrice .priceTotal') 
                    || document.querySelector('.priceTotal');
          if (!root) return {sell:null, origin:null, rate:null};
          const digits = s => (s||'').replace(/[^0-9]/g, '');
          const text = sel => {
              const el = root.querySelector(sel);
              return el ? el.textContent.trim() : null;
          };
          const sell = digits(text('.sellPrice strong')) || digits(text('.sell strong')) || null;
          let origin = null;
          for (const em of root.querySelectorAll('.base em, em')) {
              if (em.closest('.sellPrice') || em.closest('.discount')) continue;
              const n = digits(em.textContent);
              if (n) { origin = n; break; }
          }
          let rate = null;
          const d = root.querySelector('.discount');
          if (d) {
              const n = digits(d.textContent);
              if (n) rate = n;
          }
          return {sell, origin, rate};
        }
        """)

        def _to_int(s: str | None) -> int | None:
            if not s:
                return None
            s = re.sub(r"[^0-9]", "", s)
            return int(s) if s else None

        sale_price  = _to_int(data.get('sell'))
        list_price  = _to_int(data.get('origin'))
        dc_rate     = _to_int(data.get('rate'))

        if dc_rate is None and list_price and sale_price and list_price > 0 and sale_price <= list_price:
            dc_rate = round((list_price - sale_price) * 100 / list_price)
        if list_price is None and sale_price is not None:
            list_price = sale_price
            if dc_rate is None:
                dc_rate = 0

        row = {
            "PRODUCT_ID": product_id,
            "HOMESHOPPING_ID": homeshopping_id,
            "SALE_PRICE": list_price,
            "DC_PRICE": sale_price,
            "DC_RATE": dc_rate
        }
        return pd.DataFrame([row], columns=["PRODUCT_ID", "HOMESHOPPING_ID", "SALE_PRICE", "DC_PRICE", "DC_RATE"])

    # ------------------
    # DB에서 PRODUCT_ID 목록 불러오기
    # ------------------
    conn, cur = utils.con_to_maria_ods()

    where_exclude = "AND PRODUCT_ID NOT IN (SELECT PRODUCT_ID FROM ODS_HOMESHOPPING_PRODUCT_INFO)"
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    sql = f"""
        SELECT DISTINCT PRODUCT_ID
        FROM ODS_HOMESHOPPING_LIST
        WHERE HOMESHOPPING_ID = {int(homeshopping_id)}
        {where_exclude}
        {limit_clause};
    """
    cur.execute(sql)
    id_list = [str(r[0]) for r in cur.fetchall()]

    if not id_list:
        print("[HNS-DETAIL] [INFO] 대상 PRODUCT_ID 없음")
        return

    # ------------------
    # 수집 루프
    # ------------------
    df_product, df_img, df_detail = utils.def_dataframe()
    total_cnt = 0
    end_cnt = len(id_list)
    flush_count = 0

    for product_id in id_list:
        print('[HNS-DETAIL]',product_id, "수집")
        url = f"https://www.hnsmall.com/display/goods.do?goods_code={product_id}"

        try:
            # 상세(탭/iframe)
            df_detail_info = crawl_product_detail_page(url)
        except Exception as e:
            print(f"[HNS-DETAIL] [ERROR][detail] {product_id}: {e!r}")
            df_detail_info = pd.DataFrame(columns=["PRODUCT_ID", "DETAIL_COL", "DETAIL_VAL"])

        try:
            # 이미지
            df_img_url = crawl_hns_detail_images(str(product_id), headless=True, max_scroll_steps=8)
        except Exception as e:
            print(f"[HNS-DETAIL] [ERROR][img] {product_id}: {e!r}")
            df_img_url = pd.DataFrame(columns=["PRODUCT_ID", "SORT_ORDER", "IMG_URL"])

        try:
            # 가격
            df_product_info = crawl_prices_page(url)
        except Exception as e:
            print(f"[HNS-DETAIL] [ERROR][price] {product_id}: {e!r}")
            df_product_info = pd.DataFrame(columns=["PRODUCT_ID", "HOMESHOPPING_ID", "SALE_PRICE", "DC_PRICE", "DC_RATE"])

        # 합치기
        df_product = pd.concat([df_product, df_product_info], ignore_index=True)
        df_img     = pd.concat([df_img, df_img_url], ignore_index=True)
        df_detail  = pd.concat([df_detail, df_detail_info], ignore_index=True)

        flush_count += 1
        total_cnt   += 1

        # 배치 적재
        if flush_count >= batch_size or total_cnt == end_cnt:
            df_product = utils.sanitize_for_db(df_product)
            df_detail  = utils.sanitize_for_db(df_detail)
            df_img     = utils.sanitize_for_db(df_img)

            utils.insert_df_into_db(conn, df_product, "ODS_HOMESHOPPING_PRODUCT_INFO", 'IGNORE')
            utils.insert_df_into_db(conn, df_detail,  "ODS_HOMESHOPPING_DETAIL_INFO",  'IGNORE')
            utils.insert_df_into_db(conn, df_img,     "ODS_HOMESHOPPING_IMG_URL",      'IGNORE')

            print('[HNS] Dump to DB...', f'total : {total_cnt} / {end_cnt}')

            # 초기화
            df_product, df_img, df_detail = utils.def_dataframe()
            flush_count = 0
    cur.close()
    conn.close()
    print('[HNS] Complete', f'total : {total_cnt} / {end_cnt}')

'''
현대홈쇼핑 크롤링 함수
crawl_hyundai
crawl_hyundai_detail
'''
# 현대홈쇼핑 편성표 크롤링, 데이터 INSERT TO ODS
# HOMESHOPPING_ID = 2, 3
def crawl_hyundai():
    conn, cur = utils.con_to_maria_ods()
    brodType = ['etv','dtv']

    for k in brodType:
        if k == 'etv':
            h_index = 2
        else:
            h_index = 3
        today = date.today()
        for days in range(11):
            page = 200
            current = today + timedelta(days=days)
            cr_date = current.strftime('%Y%m%d')
            print('[HYUNDAI]', cr_date, "시작")
            url = f'https://wwwca.hmall.com/api/hf/dp/v1/main-tv-new/tv-list?brodDt={cr_date}&brodPrrgPage={page}&brodType={k}&deviceInfo=pc'
            response = requests.get(url)
            data = response.json()

            result = []
            # 메인상품
            for i in data['respData']['broadItemList']:
                if i.get('newBrodDt') :
                    live_date = i.get('newBrodDt', '')
                else:
                    live_date = i.get('brodDt', '')
                live_time = i.get('brodStrtDtm', '') + '~' + i.get('brodEndDtm', '')
                product_id = str(i.get('slitmCd', '0'))
                product_name = i.get('slitmNm', '')
                sale_price = str(i.get('sellPrc', ''))
                dc_price = str(i.get('bbprc', ''))
                dc_rate = str(i.get('copnRate', ''))
                p = list(product_id)
                img_name = i.get('brodImgNm', '')
                img_type = img_name[img_name.find('.')+1:]
                img_url = f"https://image.hmall.com/static/{p[7]}/{p[6]}/{p[4]+p[5]}/{p[2]+p[3]}/onair{product_id}.{img_type}?AR=0&SF=webp"
                live_title = i.get('prmoTxtCntn', '')
                result.append({
                        "HOMESHOPPING_ID" : h_index,
                        "LIVE_DATE" : live_date,
                        "LIVE_TIME": live_time,
                        "PROMOTION_TYPE": "main",
                        "LIVE_TITLE": live_title,
                        "PRODUCT_ID": product_id,
                        "PRODUCT_NAME": product_name,
                        "SALE_PRICE": sale_price,
                        "DC_PRICE" : dc_price,
                        "DC_RATE": dc_rate,
                        "THUMB_IMG_URL": img_url
                    })
                # 서브상품
                if 'withItemList' in i:
                    for j in i['withItemList']:
                        live_date = j.get('brodDt', '')
                        live_time = j.get('brodStrtDtm', '') + ' ~ ' + j.get('brodEndDtm', '')
                        product_id = str(j.get('slitmCd', '0'))
                        product_name = j.get('slitmNm', '')
                        sale_price = str(j.get('sellPrc', ''))
                        dc_price = str(j.get('bbprc', ''))
                        dc_rate = str(j.get('copnRate', ''))
                        p = list(product_id)
                        img_name = j.get('thumImgNm', '')
                        img_type = img_name[img_name.find('.')+1:]
                        img_url = f"https://image.hmall.com/static/{p[7]}/{p[6]}/{p[4]+p[5]}/{p[2]+p[3]}/{product_id}_0.{img_type}?RS=125x125&AR=0&SF=webp"
                        live_title = j.get('prmoTxtCntn', '')
                        result.append({
                                "HOMESHOPPING_ID" : h_index,
                                "LIVE_DATE" : live_date,
                                "LIVE_TIME": live_time,
                                "PROMOTION_TYPE": "sub",
                                "LIVE_TITLE": live_title,
                                "PRODUCT_ID": product_id,
                                "PRODUCT_NAME": product_name,
                                "SALE_PRICE": sale_price,
                                "DC_PRICE" : dc_price,
                                "DC_RATE": dc_rate,
                                "THUMB_IMG_URL": img_url
                                })
                        result_df = pd.DataFrame(result)
            time.sleep(random.uniform(1, 1.5))
            utils.insert_df_into_db(conn, result_df, 'ODS_HOMESHOPPING_LIST', 'IGNORE')
            current_schedule = result_df[['HOMESHOPPING_ID','LIVE_DATE','LIVE_TIME','PRODUCT_ID']]
            utils.insert_df_into_db(conn, current_schedule, 'ODS_HOMESHOPPING_CURRENT_SCHEDULE')
            print('[HYUNDAI]', cr_date, "종료")
    cur.close()
    conn.close()
# 상세 정보 크롤링
def crawl_hyundai_detail(homeshopping_id):
    def _get_soup_by_playwright(url: str, wait_ms: int = 5000) -> BeautifulSoup:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36")
            )
            page = ctx.new_page()
            page.goto(url, timeout=60_000, wait_until="domcontentloaded")
            page.wait_for_timeout(wait_ms)
            try:
                page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
            except Exception as e:
                print('[HYUNDAI-DETAIL]', e)
            page.wait_for_timeout(5000)
            html = page.content()
            ctx.close()
            browser.close()
        return BeautifulSoup(html, "html.parser")

    def _get_product_id_from_url(url: str) -> str | None:
        # fallback: slitmCd=2238... 쿼리에서 뽑기
        qs = parse_qs(urlparse(url).query)
        arr = qs.get('slitmCd')
        return arr[0] if arr else None

    # html => dataframe
    def parse_hmall_product(url: str, homeshopping_id) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        soup = _get_soup_by_playwright(url)

        # 1) __NEXT_DATA__ 기반 JSON 파싱 시도
        product_id = None
        seller_name = None
        product_name = None
        origin_price = None
        discount_rate = None
        sale_price = None
        img_rows = []
        detail_rows = []

        next_script = soup.select_one('script#__NEXT_DATA__')
        if next_script and next_script.string:
            try:
                data = json.loads(next_script.string)
                pp = (data.get("props") or {}).get("pageProps") or {}
                resp = (pp.get("respData") or {})
                item = (resp.get("itemPtc") or {})

                product_id = item.get("slitmCd") or _get_product_id_from_url(url)
                product_name = item.get("slitmNm")
                seller_name = item.get("brndNm")
                origin_price = item.get("bbprc")   # 정상가
                sale_price = item.get("sellPrc")   # 할인가
                if origin_price and sale_price and origin_price > 0:
                    discount_rate = int(round((sale_price - origin_price) * 100.0 / origin_price))

                # 상세 정보가 JSON에도 있을 수 있으나, 여기서는 DOM fallback로 처리
            except Exception as e:
                print(f"[HYUNDAI-DETAIL] [WARN] JSON parsing error: {e}")

        # 2) DOM 기반 파싱 (JSON 값이 None이거나 비어있을 때만 덮어씀)
        # 상품코드
        if not product_id:
            for tr in soup.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th and td and utils._clean_text(th.get_text()) == "상품코드":
                    em = td.find("em")
                    product_id = utils._clean_text(em.get_text() if em else td.get_text())
                    break
            if not product_id:
                product_id = _get_product_id_from_url(url)

        # 판매자명
        if not seller_name:
            a_brand = soup.select_one(".brandshop-link a.link")
            if a_brand:
                seller_name = utils._clean_text(a_brand.get("ga-custom-creative")) or utils._clean_text(a_brand.get_text())

        # 상품명
        if not product_name:
            og = soup.select_one('meta[property="og:title"]')
            product_name = utils._clean_text(og.get("content")) if og else None
            if not product_name:
                pdname = soup.select_one("div.pdname")
                product_name = utils._clean_text(pdname.get_text()) if pdname else None

        # 가격
        if not sale_price or not origin_price:
            em_before = soup.select_one(".pdpricebox .sale-before em")
            em_rate   = soup.select_one(".pdpricebox .sale-rate em")
            em_sale   = soup.select_one(".pdpricebox .sale-price em")
            if em_sale:
                sale_price = sale_price or utils._num_only(em_sale.get_text())
            if em_before:
                origin_price = origin_price or utils._num_only(em_before.get_text())
            else:
                origin_price = origin_price or sale_price
            if em_rate:
                discount_rate = discount_rate or utils._num_only(em_rate.get_text())

        # 배송/반품 등 기본 정보
        basic_map = {
            "상품코드": None,
            "가격": None,
            "배송비": None,
            "택배사": None,
            "반품/교환": None,
            "소비기한": None,
        }
        for tr in soup.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = utils._clean_text(th.get_text())
            if key not in basic_map:
                continue
            if key == "반품/교환":
                spans = [ utils._clean_text(s.get_text()) for s in td.select("span") if utils._clean_text(s.get_text()) ]
                val = ", ".join(spans) if spans else utils._clean_text(td.get_text())
            elif key == "가격":
                em = td.find("em")
                val = utils._clean_text(em.get_text()) if em else utils._clean_text(td.get_text())
                val = re.sub(r'\s*원$', '', val) if val else val
            else:
                em = td.find("em")
                val = utils._clean_text(em.get_text()) if em else utils._clean_text(td.get_text())
            basic_map[key] = val

        # 이미지 (DOM 기반 추가 이미지)
        order = 1
        for img in soup.select(".speedycat-container img"):
            ds = img.get("data-src")
            if not ds:
                continue
            u = utils._https(ds.strip())
            if u:
                img_rows.append({"PRODUCT_ID": product_id, "SORT_ORDER": order, "IMG_URL": u})
                order += 1

        # 상세 정보 (DOM)
        panel = soup.select_one(".accordion-panel.product-essential-info")
        if panel:
            for h4 in panel.select("h4.subheadings"):
                col = utils._clean_text(h4.get_text())
                p = h4.find_next_sibling("p", class_="abstract2")
                if not p:
                    continue
                val = p.get_text("\n").strip()
                detail_rows.append({"PRODUCT_ID": product_id, "DETAIL_COL": col, "DETAIL_VAL": val})

        # 최종 DF 생성
        df_product_info = pd.DataFrame([{
            "PRODUCT_ID": product_id,
            "HOMESHOPPING_ID": homeshopping_id,
            "STORE_NAME": seller_name,
            "PRODUCT_NAME": product_name,
            "SALE_PRICE": sale_price,
            "DC_RATE": discount_rate,
            "DC_PRICE": origin_price,
            "DELIVERY_FEE": basic_map["배송비"],
            "DELIVERY_CO": basic_map["택배사"],
            "RETURN_EXCHANGE": basic_map["반품/교환"],
            "TERM": basic_map["소비기한"]
        }])

        df_img_url = pd.DataFrame(img_rows)
        df_detail_info = pd.DataFrame(detail_rows)

        return df_product_info, df_img_url, df_detail_info

    # DB접속
    conn, cur = utils.con_to_maria_ods()
    # PRODUCT_ID 리스트 DB에서 호출
    cur.execute(f"""
            SELECT 
                DISTINCT PRODUCT_ID 
            FROM ODS_HOMESHOPPING_LIST 
            WHERE HOMESHOPPING_ID = {homeshopping_id} 
                AND PRODUCT_ID NOT IN (SELECT PRODUCT_ID FROM ODS_HOMESHOPPING_PRODUCT_INFO);
                """)
    id_list = [row[0] for row in cur.fetchall()]
    # 초기 empty_dataframe 생성
    df_product, df_img, df_detail = utils.def_dataframe()
    # dump => db insert 기준
    count = 0
    total_cnt = 0
    for i in id_list:
        print('[HYUNDAI-DETAIL]', i, '수집')
        url = f"https://www.hmall.com/md/pda/itemPtc?slitmCd={i}"
        # 크롤링 / 빈 데이터 프레임에 concat
        df_product_info, df_img_url, df_detail_info = parse_hmall_product(url, homeshopping_id)
        df_product = pd.concat([df_product, df_product_info], ignore_index=True)
        df_img = pd.concat([df_img, df_img_url], ignore_index=True)
        df_detail = pd.concat([df_detail, df_detail_info], ignore_index=True)
        count += 1
        total_cnt += 1
        if count == 10 or total_cnt == len(id_list):
            # 10개 product 정보 데이터 insert 실행
            utils.insert_df_into_db(conn, df_product, "ODS_HOMESHOPPING_PRODUCT_INFO", 'IGNORE')
            utils.insert_df_into_db(conn, df_detail, "ODS_HOMESHOPPING_DETAIL_INFO", 'IGNORE')
            utils.insert_df_into_db(conn, df_img, "ODS_HOMESHOPPING_IMG_URL", 'IGNORE')
            print('[HYUNDAI-DETAIL] Dump to DB...', f'total : {total_cnt}')
            # dump count 초기화, dataframe 초기화
            df_product, df_img, df_detail = utils.def_dataframe()
            count = 0
    cur.close()
    conn.close()
    print('[HYUNDAI-DETAIL] complete', f'total : {total_cnt}')

'''
NS홈쇼핑 크롤링 함수
crawl_ns
crawl_ns_detail
'''
# NS홈쇼핑 편성표 크롤링, 데이터 INSERT TO ODS
# HOMESHOPPING_ID = 4, 5
def crawl_ns():
    conn, cur = utils.con_to_maria_ods()
    brodType = {'tv':'TV','shopplus':'CTCOM'}

    for k in brodType:
        if k == 'tv':
            h_index = 4
        else:
            h_index = 5
        today = date.today()
        for i in range(11):
            current = today + timedelta(days=i)
            cr_date = current.strftime('%Y%m%d')
            print("[NS]", cr_date, "시작")
            url = f'https://mapi.nsmall.com/md/api/v1/display/media/schedule/{k}/{cr_date}?formDate={cr_date}&cnnlCd={brodType[k]}'
            price_url = f'https://mapi.nsmall.com/md/api/v1/display/media/schedule/brdctPriceInfo?formDate={cr_date}&CnnlCd={k}'
            response1 = requests.get(url)
            time.sleep(random.uniform(1, 1.5))
            response2 = requests.get(price_url)
            data = response1.json()['data']['resultData']['totalOrgan']
            price_data = response2.json()['data']['resultData']
            price_df = pd.DataFrame(price_data)
            # print(price_df[['goodsCd','salePrice','dcPrice','dcRate']])
            result = []
            # 메인상품
            for t in data:
                goods = t.get('goods', '')
                brodcst = goods.get('brdctInfo', '')
                raw = brodcst.get('formStartDttm', '')
                live_date = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d") if raw else ""
                live_time = utils.datetime_to_time(brodcst.get('formStartDttm', '')) + ' ~ ' + utils.datetime_to_time(brodcst.get('formEndDttm', ''))
                product_id = t.get('goodsCd', '')
                product_name = t.get('goods','').get('productNm','')

                sp = price_df.loc[price_df['goodsCd'] == product_id, 'salePrice']
                sale_price = int(sp.values[0]) if not sp.empty else None

                dp = price_df.loc[price_df['goodsCd'] == product_id, 'dcPrice']
                dc_price = int(dp.values[0]) if not dp.empty else None

                dr = price_df.loc[price_df['goodsCd'] == product_id, 'dcRate']
                dc_rate = int(dr.values[0]) if not dr.empty else None
                if dc_rate == 'nan':
                    dc_rate = None
                img_url = f"https://product-image.prod-nsmall.com/new/{product_id}_X6.jpg"
                live_title = t.get('pgmNm')
                result.append({
                        "HOMESHOPPING_ID" : h_index,
                        "LIVE_DATE" : live_date,
                        "LIVE_TIME": live_time,
                        "PROMOTION_TYPE": "main",
                        "LIVE_TITLE": live_title,
                        "PRODUCT_ID": product_id,
                        "PRODUCT_NAME": product_name,
                        "SALE_PRICE": sale_price,
                        "DC_PRICE": dc_price,
                        "DC_RATE": dc_rate,
                        "THUMB_IMG_URL": img_url
                    })
                # 서브상품
                if len(t['relTotalOrgan']) > 0:
                    for j in t['relTotalOrgan']:
                        goods = j.get('goods', '')
                        brodcst = goods.get('brdctInfo', '')
                        raw = brodcst.get('formStartDttm', '')
                        live_date = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d") if raw else ""
                        live_time = utils.datetime_to_time(brodcst.get('formStartDttm', '')) + ' ~ ' + utils.datetime_to_time(brodcst.get('formEndDttm', ''))
                        product_id = j.get('goodsCd', '')
                        product_name = j.get('goods','').get('productNm','')

                        sp = price_df.loc[price_df['goodsCd'] == product_id, 'salePrice']
                        sale_price = int(sp.values[0]) if not sp.empty else None

                        dp = price_df.loc[price_df['goodsCd'] == product_id, 'dcPrice']
                        dc_price = int(dp.values[0]) if not dp.empty else None

                        dr = price_df.loc[price_df['goodsCd'] == product_id, 'dcRate']
                        dc_rate = int(dr.values[0]) if not dr.empty else None
                        if dc_rate == 'nan':
                            dc_rate = None
                        img_url = j.get('imageUrl', '')
                        live_title = j.get('pgmNm')
                        result.append({
                                "HOMESHOPPING_ID" : h_index,
                                "LIVE_DATE" : live_date,
                                "LIVE_TIME": live_time,
                                "PROMOTION_TYPE": "sub",
                                "LIVE_TITLE": live_title,
                                "PRODUCT_ID": product_id,
                                "PRODUCT_NAME": product_name,
                                "SALE_PRICE": sale_price,
                                "DC_PRICE": dc_price,
                                "DC_RATE": dc_rate,
                                "THUMB_IMG_URL": img_url
                                })
                        result_df = pd.DataFrame(result)
            utils.insert_df_into_db(conn, result_df, 'ODS_HOMESHOPPING_LIST', 'IGNORE')
            current_schedule = result_df[['HOMESHOPPING_ID','LIVE_DATE','LIVE_TIME','PRODUCT_ID']]
            utils.insert_df_into_db(conn, current_schedule, 'ODS_HOMESHOPPING_CURRENT_SCHEDULE')
            print('[NS]', cr_date, "종료")
            time.sleep(random.uniform(1, 1.5))
    cur.close()
    conn.close()
# 상세 정보 크롤링
def crawl_ns_detail(homeshopping_id):
    def get_ns_html_to_soup(url, goto_timeout=45_000, click_timeout=10_000):
        """
        url: 접속할 웹페이지 주소
        button_selectors: 버튼의 CSS 선택자 리스트 (예: ['#btn1', '#btn2'])
        """
        button_selectors = [
            ".goods-tab-list li:nth-child(1) button",  # 첫 번째 버튼 CSS 선택자
            ".goods-tab-list li:nth-child(2) button"  # 두 번째 버튼 CSS 선택자
        ]
        html_results = {}

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # 1. 접속 시 HTML 가져오기
            for attempt in range(2):
                try:
                    page.goto(url, timeout=goto_timeout, wait_until="domcontentloaded")
                    break
                except PlaywrightTimeoutError as e:
                    if attempt == 0:
                        print(f"[NS-DETAIL] [WARN] goto timeout, retry once... url={url}")
                        continue
                    print(f"[NS-DETAIL] [SKIP] goto timeout after retry: {url} ({e})")
                    browser.close()
                    return None, None, None
            time.sleep(random.uniform(1, 1.5))
            html_results["initial"] = page.content()

            # 2. 각 버튼 클릭 후 HTML 가져오기
            for idx, selector in enumerate(button_selectors, start=1):
                try:
                    page.click(selector, timeout=click_timeout)

                    if idx == 1:
                        # [버튼1=이미지] 이미지 컨테이너/이미지 로딩까지 대기
                        try:
                            page.wait_for_selector(".goods-detail-img-wrap", timeout=click_timeout)
                        except PlaywrightTimeoutError:
                            pass
                        # lazy-load 유도
                        try:
                            page.locator(".goods-detail-img-wrap").first.scroll_into_view_if_needed()
                        except Exception:
                            pass
                        for _ in range(3):
                            page.mouse.wheel(0, 1000)
                            page.wait_for_timeout(200)
                        # 실제 첫 이미지가 로드(naturalWidth>0)될 때까지 대기
                        try:
                            page.wait_for_function(
                                """() => {
                                    const img = document.querySelector('.goods-detail-img-wrap img');
                                    if (!img) return false;
                                    const src = img.getAttribute('src') || img.getAttribute('data-src')
                                            || img.getAttribute('data-original') || img.getAttribute('data-lazy');
                                    return !!src && img.complete && (img.naturalWidth || 0) > 0;
                                }""",
                                timeout=click_timeout
                            )
                        except PlaywrightTimeoutError:
                            pass

                    elif idx == 2:
                        # [버튼2=상세정보고시] 헤더 및 테이블 등장까지 대기
                        try:
                            page.wait_for_selector('h3.title:has-text("상품 필수 정보")', timeout=click_timeout)
                        except PlaywrightTimeoutError:
                            pass
                        # 헤더 다음 영역 어딘가에 table 렌더링될 때까지 대기
                        try:
                            page.wait_for_function(
                                """() => {
                                    const headers = Array.from(document.querySelectorAll('h3.title'));
                                    const h = headers.find(el => (el.textContent || '').includes('상품 필수 정보'));
                                    if (!h) return false;
                                    let node = h;
                                    for (let i=0; i<10 && node; i++) {
                                        if (node.tagName && node.tagName.toLowerCase() === 'table') return true;
                                        if (node.querySelector && node.querySelector('table')) return true;
                                        node = node.nextElementSibling;
                                    }
                                    return false;
                                }""",
                                timeout=click_timeout
                            )
                        except PlaywrightTimeoutError:
                            pass

                    time.sleep(random.uniform(1, 1.5))
                    html_results[f"button_{idx}"] = page.content()
                except Exception as e:
                    print(f"[NS-DETAIL] [ERROR] 버튼 {idx} 클릭 실패: {e}")
                    html_results[f"button_{idx}"] = None

            browser.close()

        # BeautifulSoup로 파싱해서 반환
        parsed_results = {k: BeautifulSoup(v, "html.parser") if v else None for k, v in html_results.items()}
        return parsed_results['initial'], parsed_results['button_1'], parsed_results['button_2']

    def ns_detail_crawl(soup, product_id):
        detail_rows = []
        info_header = soup.find("h3", class_="title", string=lambda s: s and "상품 필수 정보" in s)
        if info_header:
            table = info_header.find_next("table")
            if table:
                for tr in table.find_all("tr"):
                    th = tr.find("th")
                    td = tr.find("td")
                    if not th or not td:
                        continue
                    col = th.get_text(strip=True)
                    val = td.get_text(strip=True)
                    detail_rows.append({
                        "PRODUCT_ID": product_id,
                        "DETAIL_COL": col,
                        "DETAIL_VAL": val
                    })

        return pd.DataFrame(detail_rows)

    def ns_img_crawl(soup, product_id):
        # 상품설명 영역의 첫 번째 이미지 src 추출
        img_tag = soup.select_one(".goods-detail-img-wrap img")
        img_url = img_tag.get("src") if img_tag else None
        row = {
            "PRODUCT_ID" : product_id,
            "SORT_ORDER" : 1,
            "IMG_URL" : img_url
        }

        return pd.DataFrame([row], columns=["PRODUCT_ID", "SORT_ORDER", "IMG_URL"])

    def ns_info_crawl(soup, homeshopping_id) -> pd.DataFrame:
        """
        ns_html.txt 같은 HTML 문자열에서
        [PRODUCT_ID, STORE_NAME, PRODUCT_NAME, SALE_PRICE, DC_RATE, DC_PRICE] 1행 DataFrame 반환
        """
        def _meta_recobell(soup, prop):
            tag = soup.find("meta", {"name": "recobell", "property": prop})
            return tag.get("content") if tag and tag.get("content") is not None else None
        
        # 1) meta recobell 우선
        product_id   = _meta_recobell(soup, "eg:itemId")
        product_name = _meta_recobell(soup, "eg:itemName")
        store_name   = _meta_recobell(soup, "eg:brandName")
        sale_price   = utils._num_only(_meta_recobell(soup, "eg:originalPrice"))  # 정상가
        dc_price     = utils._num_only(_meta_recobell(soup, "eg:salePrice"))      # 할인가
        dc_rate      = None

        # 2) 가격 DOM 파싱 (미할인/할인 케이스 모두 처리)
        price_box = soup.select_one(".price-wrap")
        if price_box:
            # 할인 케이스
            dc_price_dom = price_box.select_one(".dc-price")
            origin_dom   = price_box.select_one(".origin-price")
            rate_dom     = price_box.select_one(".dc-rate")

            # 미할인 케이스
            current_dom  = price_box.select_one(".current-price")

            if dc_price_dom:  # 할인 있음
                dc_price = utils._num_only(dc_price_dom.get_text()) or dc_price
                sale_price = utils._num_only(origin_dom.get_text() if origin_dom else None) or sale_price
                if rate_dom:
                    dc_rate = utils._num_only(rate_dom.get_text())
            elif current_dom:  # 할인 없음
                p = utils._num_only(current_dom.get_text())
                if p is not None:
                    sale_price = sale_price or p
                    dc_price = dc_price or p
                    dc_rate = 0

        # 3) 할인율 계산 보정 (meta/DOM 어느 쪽이든 값이 있을 때)
        if dc_rate is None and sale_price and dc_price is not None:
            try:
                if sale_price > 0 and dc_price <= sale_price:
                    dc_rate = int(round((sale_price - dc_price) * 100.0 / sale_price))
            except Exception:
                dc_rate = None

        # 4) 이름/브랜드 DOM fallback
        if not product_name:
            # h1이나 대표 타이틀 계열
            for sel in ["h1", ".goods-title", ".goods-name", ".prd-name", ".product-title", ".tit", ".title"]:
                el = soup.select_one(sel)
                if el and utils._clean_text(el.get_text()):
                    product_name = utils._clean_text(el.get_text())
                    break
            # 마지막으로 og:title 시도
            if not product_name:
                og = soup.find("meta", property="og:title")
                if og and og.get("content"):
                    product_name = utils._clean_text(og.get("content"))

        # brand/store fallback은 사이트 구조마다 다르니 meta 없으면 빈 값으로 둠
        # product_id도 meta가 최선. 필요시 다른 경로에서 파싱 추가 가능.

        row = {
            "PRODUCT_ID": product_id,
            "HOMESHOPPING_ID": homeshopping_id,
            "STORE_NAME": store_name,
            "PRODUCT_NAME": product_name,
            "SALE_PRICE": sale_price,
            "DC_RATE": dc_rate,
            "DC_PRICE": dc_price,
        }

        return pd.DataFrame([row], columns=list(row.keys()))
    conn, cur = utils.con_to_maria_ods()
    # PRODUCT_ID 리스트 DB에서 호출
    cur.execute(f"""
            SELECT 
                DISTINCT PRODUCT_ID 
            FROM ODS_HOMESHOPPING_LIST 
            WHERE HOMESHOPPING_ID = {homeshopping_id} 
                AND PRODUCT_ID NOT IN (SELECT PRODUCT_ID FROM ODS_HOMESHOPPING_PRODUCT_INFO);
                """)
    id_list = [row[0] for row in cur.fetchall()]
    # 초기 empty_dataframe 생성
    df_product, df_img, df_detail = utils.def_dataframe()
    count = 0
    total_cnt = 0
    for i in id_list:
        print('[NS-DETAIL] ',i, '수집')
        url = f'https://m.nsmall.com/goods/{i}'
        soup_init, soup_img, soup_detail = get_ns_html_to_soup(url)

        if soup_init is None:                 # 진입 자체 실패 → 스킵
            print(f"[NS-DETAIL] [SKIP] load failed for {i}")
            continue

        df_product_info = ns_info_crawl(soup_init, homeshopping_id)
        if soup_img:
            df_img_url = ns_img_crawl(soup_img, i)
            df_img = pd.concat([df_img, df_img_url], ignore_index=True)
        if soup_detail:
            df_detail_info = ns_detail_crawl(soup_detail, i)
            df_detail = pd.concat([df_detail, df_detail_info], ignore_index=True)

        df_product = pd.concat([df_product, df_product_info], ignore_index=True)
        df_img = pd.concat([df_img, df_img_url], ignore_index=True)
        df_detail = pd.concat([df_detail, df_detail_info], ignore_index=True)
        count += 1
        total_cnt += 1
        if count == 10 or total_cnt == len(id_list):
            df_product = utils.sanitize_for_db(df_product)
            df_detail  = utils.sanitize_for_db(df_detail)
            df_img     = utils.sanitize_for_db(df_img)
            # 10개 product 정보 데이터 insert 실행
            utils.insert_df_into_db(conn, df_product, "ODS_HOMESHOPPING_PRODUCT_INFO", 'IGNORE')
            utils.insert_df_into_db(conn, df_detail, "ODS_HOMESHOPPING_DETAIL_INFO", 'IGNORE')
            utils.insert_df_into_db(conn, df_img, "ODS_HOMESHOPPING_IMG_URL", 'IGNORE')
            print('[NS-DETAIL] Dump to DB...', f'total : {total_cnt} / {len(id_list)}')
            # dump count 초기화, dataframe 초기화
            df_product, df_img, df_detail = utils.def_dataframe()
            count = 0
    cur.close()
    conn.close()
    print('[NS-DETAIL] complete', f'total : {total_cnt}')

def run_group1():
    # 1) 홈앤쇼핑
    try:
        crawl_hns()               # 편성표
    except Exception:
        print("[GROUP1] crawl_hns FAILED:\n", traceback.format_exc())
    try:
        crawl_hns_detail(1)       # 상세/이미지/가격
    except Exception:
        print("[GROUP1] crawl_hns_detail FAILED:\n", traceback.format_exc())
    return "group1_done"

def run_group2():
    # 2) 현대홈쇼핑 etv/dtv
    try:
        crawl_hyundai()           # 편성표
    except Exception:
        print("[GROUP2] crawl_hyundai FAILED:\n", traceback.format_exc())
    for hid in (2, 3):
        try:
            crawl_hyundai_detail(hid)  # 상세/이미지/가격
        except Exception:
            print(f"[GROUP2] crawl_hyundai_detail({hid}) FAILED:\n", traceback.format_exc())
    return "group2_done"

def run_group3():
    # 3) NS홈쇼핑 TV/샵+
    try:
        crawl_ns()                # 편성표
    except Exception:
        print("[GROUP3] crawl_ns FAILED:\n", traceback.format_exc())
    for hid in (4, 5):
        try:
            crawl_ns_detail(hid)       # 상세/이미지/가격
        except Exception:
            print(f"[GROUP3] crawl_ns_detail({hid}) FAILED:\n", traceback.format_exc())
    return "group3_done"

def main():
    # Playwright와의 호환을 위해 프로세스 시작 방식을 spawn으로 고정
    try:
        multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError:
        pass

    # 최초 1회만 테이블 생성
    try:
        create_ods_hs()
    except Exception:
        print("[INIT] create_ods_hs FAILED:\n", traceback.format_exc())

    tasks = [run_group1, run_group2, run_group3]

    # 동시 프로세스 실행
    with ProcessPoolExecutor(max_workers=3) as ex:
        futs = [ex.submit(fn) for fn in tasks]
        for f in as_completed(futs):
            try:
                print("[DONE]", f.result())
            except Exception:
                print("[WORKER FAILED]\n", traceback.format_exc())

if __name__ == "__main__":
    main()