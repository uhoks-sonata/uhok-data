# UHOK Data

U+콕&홈쇼핑 기반 사용자 맞춤 식재료 및 레시피 추천 서비스입니다.\
Airflow 기반의 홈쇼핑, 쇼핑몰의 실시간 데이터 ETL 파이프라인을 구축합니다.

## 📑 사용 데이터

### 🏪 홈쇼핑 (HomeShopping Data)
- **상품 정보**: 홈쇼핑 상품 정보 및 썸네일 이미지 정보
- **편성표 정보**: 채널별 편성표 정보
- **상품 상세**: 상품정보제공 고시, 상세 이미지
- **라이브 스트리밍**: 실시간 방송 플레이어 URL

### 🛍️ U+ 콕 (KOK Data)
- **가격 정보**: 상품별 가격 정보
- **상품 상세**: 상품정보제공 고시, 리뷰 정보, 판매자 정보

### 🍳 레시피 (Recipe Data)
- **레시피 정보**: 만개의 레시피 무료 데이터

## 🏗️ 아키텍처

### 🛠 기술 스택
- **개발 언어**\
  <img src="https://img.shields.io/badge/Python-3776AB?style=flat&logo=Python&logoColor=white"/>
- **크롤링**\
  <img src="https://img.shields.io/badge/Requests-bada55?style=flat&logo=Python&logoColor=white"/>
  <img src="https://img.shields.io/badge/BeautifulSoup4-d97453?style=flat&logo=Python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Playwright-92dc8d?style=flat&logo=Python&logoColor=white"/>
- **데이터베이스**: MariaDB, PostgerSQL (Ubuntu)
- **DB 연결**: Pymysql, psycopg-binary
- **컨테이너**: Docker Compose (Airflow)
- **임베딩**: SentenceTransformer > paraphrase-multilingual-MiniLM-L12-v2
- **분류**: Linear SVM

## 🗜 ETL 파이프라인

<img src="documents/images/ETL_pipeline.png">

### 데이터 탐색


### 데이터 수집


### 데이터 전처리 및 임베딩


### DAG


### 폴더 구조
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
## 📚 데이터 정의

### 테이블 정의서

[AUTH_DB](documents/Table_def_AUTH_DB.pdf) \
[ODS_DB](documents/Table_def_ODS_DB.pdf)\
[SERVICE_DB](documents/Table_def_SERVICE_DB.pdf)\
[REC_DB](documents/Table_def_REC_DB.pdf)\
[LOG_DB](documents/Table_def_LOG_DB.pdf)

---
### ERD
#### MariaDB
- **AUTH_DB** - `Back-End`

<img src="documents/images/auth.PNG">

- **ODS_DB** - `Data-Engineer`

<img src="documents/images/ods.PNG">

- **SERVICE_DB** - `Data-Engineer`, `Back-End`

<img src="documents/images/service.PNG">

---

#### PostgreSQL

- **REC_DB** - `Data-Engineer`, `ML-Engineer`

<img src="documents/images/rec.PNG">

- **LOG_DB** - `Back-End`

<img src="documents/images/log.PNG">

---

## 🚀 빠른 시작

### 사전 요구사항
- Python 
  + 로컬 : Python 3.13.5
  + Airflow : Python 3.12.12 (자동 설치)
- Docker Desktop
- MariaDB
- PostgreSQL

### 환경 설정

**1. 저장소 클론**
```bash
git clone <repository-url>
cd uhok-data
```

**2. 로컬 DB 구축**
  + MariaDB
  ```sql
  CREATE DATABASE ODS_DB
  CREATE DATABASE SERVICE_DB
  ```
   \
  + PostgreSQL
  ```sql
  -- PostgreSQL은 대문자를 쌍따옴표 안에 넣어야 인식

  CREATE DATABASE "REC_DB"
  ```

**3. 유저 생성 및 권한 부여**
```sql
-- 생성예시
CREATE USER 'user'@'%' IDENTIFIED BY 'password';

-- 권한부여
GRANT ALL PRIVILEGES ON *.* TO 'user'@'%';
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

**5. 로컬 가상환경 설정**
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
  + 기본 PORT : 8080
  + 기본 ID/PW : `airflow` / `airflow`
  + DAG > uhok_pipeline > Trigger



## 🤝 기여하기

### 개발 워크플로우



## 문서


---

**UHOK Data** - 레시피 추천을 위한 실시간 마켓 데이터 ETL
