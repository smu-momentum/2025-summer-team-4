#!python

"""
도서관 정보나루 - 장서 대출목록 크롤러(for 다운로드)

datasource_crawler.py 로 생성한 datasource.csv 를 바탕으로 데이터를 다운로드 받아 저장하는 작업을 수행합니다.
"""

import collections
import time
import pathlib

import pandas as pd
import requests

from config import DIR
from config import DATASOURCE_FILE
from config import get_logger


logger = get_logger('crawler:downloader')
time_recorder = collections.deque()


def row_filter(LibraryName: str, Year: int, Month: int, Url: str, ValidUrl: bool, SaveAt: str) -> bool:
    """이 조건에 해당하는 데이터만 다운로드 받는다."""
    return ValidUrl


def main():
    df = pd.read_csv(DATASOURCE_FILE)

    def generate_row_idx():
        for i in range(len(df)):
            if row_filter(*df.iloc[i]):
                yield i

    target_row_idx = list(generate_row_idx())
    total_count = len(target_row_idx)
    for count, i in enumerate(target_row_idx, start=1):
        url = df.iloc[i]['Url']
        is_valid_url = df.iloc[i]['ValidUrl']
        pathname = df.iloc[i]['SaveAt']
        file = DIR / pathname

        if not download_data(file, url):
            # update validity
            df['ValidUrl'].iat[i] = False
            df.to_csv(DATASOURCE_FILE, index=False)

        # 현재 진행률을 표시하면서 다운로드 진행
        add_time_record()
        log_message = f'[예상 소요시간] {time_string(get_average_time()*(total_count-count))}\t[진행률]: {count/total_count*100:.1f}% ({count}/{total_count})'
        print()
        print(log_message)


def download_data(file: pathlib.Path, url: str, force: bool = False) -> bool:
    if file.exists():
        logger.warning(f'파일이 존재합니다: "{file}"')
        if file.stat().st_size > 0 and not force:
            logger.info(f'다운로드를 건너뜁니다.')
            return True
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


def time_string(seconds: float) -> str:
    """초 단위의 시간을 'x시간 x분 x초' 형식으로 변환"""
    if seconds < 60:
        return f'{seconds:.0f}초'
    elif seconds < 3600:
        minutes = int(seconds // 60)
        seconds = int(seconds % 60)
        return f'{minutes}분 {seconds}초'
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = int(seconds % 60)
        return f'{hours}시간 {minutes}분 {seconds}초'


def add_time_record():
    time_recorder.append(time.time())
    while len(time_recorder) > 8:
        time_recorder.popleft()


def get_average_time() -> float:
    if not time_recorder:
        return 10
    return (time_recorder[-1]-time_recorder[0]) / len(time_recorder)


main()
