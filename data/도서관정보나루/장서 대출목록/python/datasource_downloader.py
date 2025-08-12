#!python

"""
도서관 정보나루 - 장서 대출목록 크롤러(for 다운로드)

datasource_crawler.py 로 생성한 datasource.csv 를 바탕으로 데이터를 다운로드 받아 저장하는 작업을 수행합니다.
"""

import pathlib

import pandas as pd
import requests

from config import DIR
from config import DATASOURCE_FILE
from util import get_logger
from util import time_string
from util import add_time_record
from util import get_average_time
from util import is_valid_csv


logger = get_logger('crawler:downloader')


def row_filter(LibraryName: str, Year: int, Month: int, Url: str, ValidUrl: bool, SaveAt: str) -> bool:
    """이 조건에 해당하는 데이터만 다운로드 받는다."""
    file = DIR / SaveAt
    if not ValidUrl:
        return False
    if is_valid_csv(file):
        return False
    return True


def main():
    df = pd.read_csv(DATASOURCE_FILE)

    def generate_row_idx():
        yielded = 0
        for i in range(len(df)):
            if i == 0 or (i+1) % 100 == 0:
                logger.info(f'{i+1}/{len(df)} 인덱스를 생성하는 중... ({yielded}개 생성됨)')
            if row_filter(*df.iloc[i]):
                yield i
                yielded += 1

    logger.info(f'다운 받을 파일의 수를 세는 중...')
    target_row_idx = list(generate_row_idx())
    total_count = len(target_row_idx)

    logger.info(f'총 {total_count}개의 파일을 다운로드 시작')
    for count, i in enumerate(target_row_idx, start=1):
        url = df.iloc[i]['Url']
        pathname = df.iloc[i]['SaveAt']
        file = DIR / pathname

        download_data(file, url)

        if not is_valid_csv(file):
            # update validity
            df['ValidUrl'].iat[i] = False
            df.to_csv(DATASOURCE_FILE, index=False)

        # 현재 진행률을 표시하면서 다운로드 진행
        add_time_record()
        log_message = f'[예상 소요시간] {time_string(get_average_time()*(total_count-count))}\t[진행률]: {count/total_count*100:.1f}% ({count}/{total_count})'
        print()
        print(log_message)


def download_data(file: pathlib.Path, url: str) -> bool:
    try:
        logger.info(f'다운로드를 준비하는 중: "{url}"')
        response = requests.get(url)
        response.raise_for_status()
        content = response.content.decode('euc_kr', errors='ignore')
        assert content
    except Exception as e:
        logger.error(e)
        logger.error(f'다운로드에 실패했습니다.')
        logger.info(f'파일을 생성하지 않습니다: "{file}"')
        return False
    else:
        logger.info(f'파일을 작성하는 중: "{file}"')
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(content)
        return True


main()
