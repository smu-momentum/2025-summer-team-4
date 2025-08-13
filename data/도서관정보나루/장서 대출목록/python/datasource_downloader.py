#!python

"""
도서관 정보나루 - 장서 대출목록 크롤러(for 다운로드)

datasource_crawler.py 로 생성한 datasource.csv 를 바탕으로 데이터를 다운로드 받아 저장하는 작업을 수행합니다.
"""

import requests

from config import DIR
from datasource_manager import DataSourceBatcher
from util import get_logger


class DataSourceDownloadBatcher(DataSourceBatcher):
    def __filter__(self, index, LibraryName, Year, Month, Url, ValidUrl, SaveAt) -> bool:
        file = DIR / SaveAt
        try:
            assert ValidUrl
            assert not file.exists()
        except AssertionError:
            return False
        else:
            return True

    def __batch__(self, index, LibraryName, Year, Month, Url, ValidUrl, SaveAt):
        file = DIR / SaveAt
        try:
            self.logger.info(f'다운로드를 준비하는 중: "{Url}"')
            response = requests.get(Url)
            response.raise_for_status()
            content = response.content.decode('euc_kr', errors='ignore')
            assert content
        except Exception as e:
            self.logger.error(e)
            self.logger.error(f'다운로드에 실패했습니다.')
            self.logger.info(f'파일을 생성하지 않습니다: "{file}"')
        else:
            self.logger.info(f'파일을 작성하는 중: "{file}"')
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text(content)


if __name__ == '__main__':
    logger = get_logger('crawler:downloader')
    batcher = DataSourceDownloadBatcher(logger, max_timestamps=20)
    batcher.batch()
