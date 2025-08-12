## 데이터 다운로드 받기

이 레포지토리의 최상위 경로에서 아래의 스크립트를 실행합니다.

### 1. 의존성 설치

```bash
pip install -r 'data/도서관정보나루/장서 대출목록/python/requirements.txt'
```

### 2. 데이터 목록 조회하기 ([`datasource.csv`](../datasource.csv) 생성하기)

직접 크롤링 하려면 아래의 코드를 실행합니다.

```bash
python 'data/도서관정보나루/장서 대출목록/python/datasource_crawler.py'
```

크롤링 할 시간이 없다면 [이 파일](../datasource.csv)을 다운로드 받아서 `data/도서관정보나루/장서 대출목록/datasource.csv` 경로에 배치합니다.

### 3. 데이터 목록에 명시된 데이터 다운로드 받기

`data/도서관정보나루/장서 대출목록/datasource.csv` 파일이 없다면,
이 과정을 실행하기 전에 2번 과정을 통해 생성해야합니다.

```bash
python 'data/도서관정보나루/장서 대출목록/python/datasource_downloader.py'
```
