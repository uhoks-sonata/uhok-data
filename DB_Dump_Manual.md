# DB Dump 사용 매뉴얼
- DB구축은 Ubuntu 환경에서 진행할 것을 권장
## 📒 쿼리파일 다운로드
- Ubuntu 내 Firefox로 하기 링크 접속 및 쿼리파일 다운로드
- https://drive.google.com/drive/folders/1eryySlfNfgVuItZon25T3YqrM5_GGtAn

## ⚙️ Ubuntu 환경 설정 및 DB 구축

### 공통 선행 사항
```bash
$ sudo apt-get update
$ sudo atp install net-tools

# enp0s3 > inet ip 확인
$ ifconfig
```

### MariaDB
```bash
# MariaDB 설치
$ sudo apt install mariadb-server mariadb-client

# 방화벽 허용
$ sudo ufw allow 3306/tcp

# cnf 파일 수정 (bind-address = 0.0.0.0 으로 수정)
$ sudo vi /etc/mysql/mariadb.conf.d/50-server.cnf

# MariaDB 재시작
$ sudo systemctl restart mariadb

# active(running) 확인
$ sudo systemctl status mariadb

# 접속 커맨드
$ sudo mariadb
```

### PostgreSQL
```bash
# PostgreSQL 설치
$ sudo apt install postgresql postgresql-contrib

# 방화벽 허용
$ sudo ufw allow 5432/tcp

# conf 파일 수정

### listen_addresses = '*' 주석 제거 및 수정
$ sudo vi /etc/postgresql/16/main/postgresql.conf

### 맨 마지막 줄에 
### host    all      all   0.0.0.0/0       scram-sha-256 추가
$ sudo vi /etc/postgresql/16/main/pg_hba.conf

# PostgreSQL 재시작
$ sudo systemctl restart postgresql

# active(running) 확인
$ sudo systemctl status postgresql

# pgvector extension install
$ sudo apt-get install postgresql-16-pgvector

# 접속 커맨드
$ sudo -u postgres psql
```


## 🦭 MariaDB 복구 절차
- 유저 이름, 비밀번호, 호스트IP, 포트번호 모두 예시.

### USER 생성 및 권한 부여
- 코드 예시 (MySQL 콘솔 내부 실행)
```sql
-- USER 생성
CREATE USER 'waksae'@'%' IDENTIFIED BY '1234';

-- 권한 부여
GRANT ALL PRIVILEGES ON *.* TO 'waksae'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
```

### Database 생성
- 코드 (MySQL 콘솔 내부 실행)
```sql
CREATE DATABASE AUTH_DB CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

CREATE DATABASE ODS_DB CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

CREATE DATABASE SERVICE_db CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
```
- `ODS_DB` > `STR_TO_NUM` 함수 생성
```sql
-- dump 복원 이전 반드시 생성.
DELIMITER $$

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
END$$

DELIMITER;
```

### Dump 복원 
```bash
$ mysql -h 192.168.219.124 -u waksae -p1234 --default-character-set=utf8mb4 AUTH_DB < ./Desktop/auth_db_dump.sql

$ mysql -h 192.168.219.124 -u waksae -p1234 --default-character-set=utf8mb4 ODS_DB < ./Desktop/ods_db_dump.sql

$ mysql -h 192.168.219.124 -u waksae -p1234 --default-character-set=utf8mb4 SERVICE_DB < ./Desktop/service_db_dump.sql
```

## 🐘 PostgreSQL 복구 절차

### USER 생성 및 권한 부여
```sql
CREATE USER waksae WITH PASSWORD '1234';

ALTER USER waksae WITH SUPERUSER;
```

### Database 생성
- 코드
```bash
# locale 리스트 확인
locale -a
```

```sql
CREATE DATABASE "LOG_DB"
ENCODING = 'UTF8'
LC_COLLATE = 'ko_KR.utf8' -- locale -a 로 확인한 리스트 중 기입
LC_CTYPE = 'ko_KR.utf8'
TEMPLATE = template0;

CREATE DATABASE "REC_DB";
ENCODING = 'UTF8'
LC_COLLATE = 'ko_KR.utf8'
LC_CTYPE = 'ko_KR.utf8'
TEMPLATE = template0;

EXIT
```

- Extension 생성 (pgvector)
```bash
$ sudo -u postgres psql -d "REC_DB"
```
```sql
CREATE EXTENSION vector;
```

### Dump 복원
```bash
$ psql -h 192.168.219.124 -U waksae -d "LOG_DB" -f ./Desktop/log_db_dump.sql

$ psql -h 192.168.219.124 -U waksae -d "REC_DB" -f ./Desktop/rec_db_dump.sql
```