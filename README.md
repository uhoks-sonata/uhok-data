# UHOK Data
- 레포지토리 관리 담당자 : 장윤수
### ✏️ 개요
- U+콕&홈쇼핑 기반 사용자 맞춤 식재료 및 레시피 추천 서비스입니다.
- Airflow 기반의 홈쇼핑, 쇼핑몰의 실시간 데이터 ETL 파이프라인을 구축합니다.

## 📑 사용 데이터

### 🏪 홈쇼핑
- **상품 정보**: 홈쇼핑 상품 정보 및 썸네일 이미지 정보
- **편성표 정보**: 채널별 편성표 정보
- **상품 상세**: 상품정보제공에 관한 고시에 따른 제품 정보, 상세 이미지
- **라이브 스트리밍**: 실시간 방송 플레이어 URL

### 🛍️ U+콕
- **가격 정보**: 상품별 가격 정보
- **상품 상세**: 상품정보제공에 관한 고시에 따른 제품 정보, 리뷰 정보, 판매자 정보

### 🍳 레시피
- **레시피 정보**: 만개의 레시피 무료 데이터

## 🏗️ 아키텍처

### 🛠 기술 스택

| 구분 | 기술 |
|---|---|
| 개발언어 | ![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=Python&logoColor=white) |
| 데이터베이스 | ![MariaDB](https://img.shields.io/badge/MariaDB-8db5dc?style=flat&logo=MariaDB&logoColor=black) ![PostgerSQL](https://img.shields.io/badge/PostgerSQL-a9abe4?style=flat&logo=PostgreSQL&logoColor=black) |
| 컨테이너 | ![Docker](https://img.shields.io/badge/Docker-001b1e?style=flat&logo=Docker&logoColor=blue) |
| 오케스트레이션 | ![Airflow](https://img.shields.io/badge/Airflow-59d4d8?style=flat&logo=apacheAirflow&logoColor=black) |

## 🗜 ETL 파이프라인

<img src="documents/images/ETL_pipeline.png">

### 📋 데이터 수집
#### 공통사항
- `ODS_DB` 내 적재
- DAG의 크롤링 단계 수행시간 단축을 위해 이미 수집된 제품 상세 정보 데이터 제외 후 수집
- 원활한 서비스 운영을 위해 제품 상세 정보 미수집 제품 관련 정보 전체 삭제 및 재수집 반복
#### 레시피
- **만개의 레시피** (최초 1회)
  + csv파일 전체 적재
#### 쇼핑몰
- **U+콕**
  + Playwright와 CSS 선택자를 사용해 데이터 수집 진행
  + 식품 카테고리 내 제품 리스트 수집
    - 제품코드
    - 할인가
    - 할인율
    - 상기 3개 항목 UNIQUE KEY 설정 및 중복 데이터 미적재
  + 가격 변동 여부 판별
    - 상품 리스트 조회 단계에서의 원가 정보 부재
    - WINDOW 함수를 사용해 직전 가격 정보의 추정원가와 최신 가격 정보의 추청원가를 비교
    - 오차 범위 설정
    - 원가 변동 판별 시 기존 제품 상세 정보 삭제 및 재수집
  + 제품 상세 정보 수집 (제품코드 조회 후 수집)
    - 제품 기본정보
    - 제품 상세 이미지 URL
    - 상품정보제공에 관한 고시에 따른 제품 정보
    - 리뷰 정보 및 상세 리뷰 정보 5개
#### 홈쇼핑
- **공통사항**
  + 편성정보 수집
    - 제품 코드, 제품명
    - 방영일시
    - 가격정보 (원가, 할인율, 할인가)
    - 썸네일 이미지
  + 편성표에 편성된 제품에 한해 제품 상세 정보 수집 (제품코드 조회 후 수집)
    - 제품 상세 이미지 URL
    - 상품정보제공에 관한 고시에 따른 제품 정보
  + 홈쇼핑 편성표 변경에 대응하기 위한 당일 편성 정보를 적재할 별도 테이블 추가
    - 모델 학습을 위해 기존 수집 데이터 보존
- **홈앤쇼핑**
  + Playwright와 CSS 선택자를 사용해 데이터 수집 진행
  + 현재 방영중인 프로그램에 한해 방영 시간이 `지금방송중`으로 출력, `00:00 ~ 00:00` 형태로 크롤링 수행 시각 입력
- **현대홈쇼핑 (2개 채널)**
  + Playwright와 CSS 선택자, Requests와 JSON 데이터를 사용해 데이터 수집 진행
- **NS홈쇼핑 (2개 채널)**
  + Playwright와 CSS 선택자, Requests와 JSON 데이터를 사용해 데이터 수집 진행
  + 제품 상세 정보 로딩 시 데이터 부재로 해당 제품 관련 상세 정보 일부 파싱 불가 
  + Playwright를 사용해 두 개 tab 버튼 클릭 후 로딩 대기 (불가피한 크롤링 수행 시간 증가)
### 📚 데이터 전처리
- **공통 사항**
  + `SERVICE_DB` 내 적재
  + 이미지 정보, 상품정보고시 데이터 미수집 시 해당 제품 코드에 해당되는 데이터 전부 삭제
- **레시피** (최초 1회)
  + 미사용 컬럼 제거
  + 상세 식재료 컬럼 내 특수문자(`|`,`ASCHII CODE`) 를 기준으로 분리 및 별도 재료 테이블 내 적재
- **쇼핑몰**
  + 각종 수치 데이터 내 특수문자 제거 및 데이터 타입 변경
  + 이상치 (ex. 2,147,483,648 %) 0으로 변환
- **홈쇼핑**
  + 미사용 컬럼 제거 (각 홈쇼핑별로 단계별 수집 가능 데이터 상이)
  + 데이터 타입 변경
  + 크롤링 수행 당시의 편성정보와 비교해 기존 편성 프로그램 취소 / 방영 여부 수정
  + 중복 수집된 이미지 URL 삭제 (서로 다른 확장자)
### 📈 데이터 임베딩
- 전처리 이후 하기 데이터에 대한 임베딩 값 생성 및 적재 (384차원)
  + `SERVICE_DB` > `FCT_RECIPE` > `COOKING_NAME`
  + `SERVICE_DB` > `FCT_MTRL` > `MATERIAL_NAME` (DISTINCT 적용)
  + `SERVICE_DB` > `FCT_KOK_PRODUCT_INFO` > `KOK_PRODUCT_NAME`
  + `SERVICE_DB` > `FCT_HOMESHOPPING_LIST` > `PRODUCT_NAME` (DISTINCT 적용)
### 🔖 제품군 분류
- 전처리 이후 `FCT_TABLE`로 부터 제품명 데이터 추출
- 홈쇼핑 데이터 : 식품/비식품 분류 이후 식재료/완제품 분류
- 쇼핑몰 데이터 : 식재료/완제품 분류 (식품 카테고리의 제품 데이터만을 수집)
- 서비스 모니터링을 통한 데이터 드리프트 관리\
    a. 추천, 라이브 편성 모니터링\
    b. 식품/비식품 분류 오류, 식재료/완제품 분류 오류 발견\
    c. 쿼리 실행을 통한 데이터 변경\
    d. 사전 내 필터 단어 추가\
    e. 재학습

### 📆 DAG
  ```
    Crawl    [U+콕, 홈앤쇼핑, 현대홈쇼핑, NS홈쇼핑] 
      ↓
  Preprocess [U+콕, 홈쇼핑]
      ↓
    Embed    [U+콕, 홈쇼핑]
      ↓
   Classify  [U+콕, 홈쇼핑]
  ```
### 📂 폴더 구조
  ```
  uhok-data/
  ├── ETL/                                   # ETL 파이프라인 Python 패키지                   
  │   ├── __init__.py 
  │   │                              
  │   ├── data/                              # 데이터 디렉토리
  │   │   └── TB_RECIPE_SEARCH_241226.csv    # 만개의 레시피 무료 데이터
  │   │
  │   ├── ingestion/                         # 데이터 추출 디렉토리
  │   │   ├── __init__.py
  │   │   ├── crawl_homeshop.py              # 홈쇼핑 데이터 크롤링 로직
  │   │   └── crawl_kok.py                   # 콕 데이터 크롤링 로직
  │   │
  │   ├── preprocessing/                     # 전처리 디렉토리
  │   │   ├── __init__.py
  │   │   ├── preprocessing_hs.py            # 홈쇼핑 데이터 전처리 로직
  │   │   └── preprocessing_kok.py           # 콕 데이터 전처리 로직
  │   │
  │   ├── embedding/                         # 임베딩 디렉토리
  │   │   ├── __init__.py
  │   │   └── embedding.py                   # 임베딩 생성 로직
  │   │
  │   ├── classifying/                       # 분류 디렉토리
  │   │   ├── __init__.py
  │   │   │
  │   │   ├── artifacts/                     # 분류 모델 저장 디렉토리
  │   │   │   ├── finished_vs_ingredient_linear_svc.joblib
  │   │   │   ├── ing_labeled
  │   │   │   ├── keyword_meta.json
  │   │   │   ├── kok_finished_vs_ingredient.joblib
  │   │   │   ├── linear_svm_calibrated.pkl
  │   │   │   ├── tfidf_char.pkl
  │   │   │   └── tfidf_word.pkl
  │   │   │
  │   │   ├── fct_to_cls.py                  # FCT_TABLE -> CLS_TABLE
  │   │   ├── predict_hs.py                  # 홈쇼핑 상품 분류 로직
  │   │   ├── predict_kok.py                 # 콕 상품 분류 로직
  │   │   ├── predict_main.py                # 분류 통합 로직
  │   │   ├── train_cls_hs_food_model.py     # 홈쇼핑 상품 식품 판별 로직
  │   │   ├── train_cls_hs_ingr_model.py     # 홈쇼핑 상품 식재료 판별 로직
  │   │   ├── train_cls_kok_model.py         # 콕 상품 식재료 판별 로직
  │   │   └── train_main.py                  # 모델 훈련 통합 로직
  │   │ 
  │   ├── utils/                             # 유틸 디렉토리
  │   │   ├── __init__.py
  │   │   └── utils.py                       # 공통 유틸 함수
  │   │ 
  │   ├── insert_recipe.py                   # 레시피 데이터 ETL 파이프라인
  │   └── main.py                            # 외부 데이터 ETL 함수
  │
  ├── dags/
  │   └── etl_dag.py                         # DAG 정의
  │
  ├── docker-compose.yaml            
  ├── Dockerfile                     
  └── requirements.txt
  ```
## 📖 데이터 정의

### 🪧 테이블 정의서

  [AUTH_DB](documents/Table_def_AUTH_DB.pdf) \
  [ODS_DB](documents/Table_def_ODS_DB.pdf)\
  [SERVICE_DB](documents/Table_def_SERVICE_DB.pdf)\
  [REC_DB](documents/Table_def_REC_DB.pdf)\
  [LOG_DB](documents/Table_def_LOG_DB.pdf)

  ### 🖇️ ERD
  - **AUTH_DB** - `MariaDB`

  <img src="documents/images/auth.PNG" width="250">

  - **ODS_DB** - `MariaDB`

  <img src="documents/images/ods.PNG">

  - **SERVICE_DB** - `MariaDB`

  <img src="documents/images/service.PNG">

  - **REC_DB** - `PostgreSQL`

  <img src="documents/images/rec.PNG" width="125">

  - **LOG_DB** - `PostgreSQL`

  <img src="documents/images/log.PNG" width="250">

---

## 🚀 빠른 시작

### ✅ 사전 요구사항
- Python 
  + 로컬 : Python 3.13.5
  + Airflow : Python 3.12.12 (자동 설치)
- Docker Desktop
- MariaDB
- PostgreSQL

### ⚙️ 환경 설정

  **1. 저장소 클론**
  ```bash
  git clone <repository-url>
  ```
  **2. 로컬 DB 구축**
  - Ubuntu 환경 DB 구축은 [DB_Dump_Manual](./DB_Dump_Manual.md) 참고
  - MariaDB
  ```sql
  CREATE DATABASE ODS_DB;
  CREATE DATABASE SERVICE_DB;
  ```

  - PostgreSQL
  ```sql
  -- PostgreSQL은 대문자를 쌍따옴표 안에 넣어야 인식

  CREATE DATABASE "REC_DB";
  ```
  **3. 유저 생성 및 권한 부여**
  - `MariaDB`
  ```sql
  -- 생성예시
  CREATE USER 'user'@'%' IDENTIFIED BY 'password';

  -- 권한부여
  GRANT ALL PRIVILEGES ON *.* TO 'user'@'%';
  ```
  - `PostgreSQL`
  ```sql
  -- 생성예시
  CREATE USER user WITH PASSWORD 'password';

  -- 권한부여
  ALTER USER user WITH SUPERUSER;
  ```
  **4. 환경 변수 설정**
  ```bash
  ### .env 파일 생성
  cp .env.example .env

  ### ----------- MariaDB -------------- 
  MARIADB_ODS_URL="mysql+pymysql://user:password@localhost:3306/AUTH_DB"

  ### 서비스용 DB (service_db)
  MARIADB_SERVICE_URL="mysql+asyncmy://user:password@localhost:3306/SERVICE_DB"

  ### ----------- PostgreSQL -----------
  POSTGRES_URL="postgresql://user:password@localhost:5432/"

  ### ----------- Airflow --------------
  AIRFLOW_UID=50000
  ```
  **5. 로컬 가상환경 설정** (로컬테스트 진행 시)
  ```bash
  $ uv venv --python 3.13.5
  $ source .venv/Scripts/activate

  ### requirements.txt > torch cpu버전 정의 주석처리 이후 pip install
  $ uv pip install -r requirements.txt

  ### uv python 라이브러리 설치 이후 url, torch 부분 주석 해제
  ```
  **6. 레시피 데이터 ETL 로직 실행**
  ```bash
  ### 레시피 데이터 ETL 최초 1회 실행
  $ python -m ETL.insert_recipe
  ```
  **7. 도커 빌드 & 업**
  ```bash
  $ docker compose build
  $ docker compose up -d
  ```
  **8. Airflow DAG Trigger 실행**
  ```
  기본 PORT : 8080
  기본 ID/PW : airflow / airflow
  DAG > uhok_pipeline > Trigger
  ```

---

**UHOK Data** - 레시피 추천을 위한 실시간 마켓 데이터 ETL
