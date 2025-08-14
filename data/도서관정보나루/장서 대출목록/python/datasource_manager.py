import collections
import functools
import logging
import pathlib
import sys
import time

import pandas as pd


DIR = pathlib.Path(__file__).parent.parent.resolve()

DATASOURCE_FILE = DIR / 'datasource.csv'

DEFAULT_LOG_FILE = DIR / 'python' / '.log'
DEFAULT_LOG_FORMAT = '[%(levelname)s] [%(name)s] [%(asctime)s] %(message)s'
DEFAULT_LOG_FORMATTER = logging.Formatter(DEFAULT_LOG_FORMAT)
DEFAULT_LOG_FILE_HANDLER = logging.StreamHandler(DEFAULT_LOG_FILE.open('a'))
DEFAULT_LOG_FILE_HANDLER.setLevel(logging.DEBUG)
DEFAULT_LOG_FILE_HANDLER.setFormatter(DEFAULT_LOG_FORMATTER)
DEFAULT_LOG_STREAM_HANDLER = logging.StreamHandler(sys.stdout)
DEFAULT_LOG_STREAM_HANDLER.setLevel(logging.DEBUG)
DEFAULT_LOG_STREAM_HANDLER.setFormatter(DEFAULT_LOG_FORMATTER)


class DataSourceBatcher:
    def __init__(self, logger_or_name: logging.Logger | str = 'datasource-batcher', max_timestamps: int = 10):
        if isinstance(logger_or_name, str):
            self.logger = logging.getLogger(logger_or_name)
            self.logger.setLevel(logging.DEBUG)
            self.logger.addHandler(DEFAULT_LOG_FILE_HANDLER)
            self.logger.addHandler(DEFAULT_LOG_STREAM_HANDLER)
        else:
            self.logger = logger_or_name
        self._df = pd.read_csv(DATASOURCE_FILE)
        self._timestamps = collections.deque()
        self._max_timestamps = max_timestamps
        self._record_batch_time()

    def __del__(self):
        del self._df

    @functools.cache
    def __len__(self) -> int:
        self.logger.info(f'배치 크기를 측정하는 중...')
        self.logger.info(f'총 탐색 대상 {len(self._df)}개.')
        step = len(self._df)//100
        size = 0
        for i in range(len(self._df)):
            if (i+1) % step == 0:
                self.logger.info(f'배치 크기 탐색 {(i+1)//step}% 완료 ({i+1}/{len(self._df)})')
            LibraryName, Year, Month, Url, ValidUrl, SaveAt = self._df.iloc[i]
            file = DIR / SaveAt
            if self.__filter__(i, LibraryName, Year, Month, Url, ValidUrl, SaveAt):
                size += 1
        return size

    def __batch__(self, index: int, LibraryName: str, Year: int, Month: int, Url: str, ValidUrl: bool, SaveAt: pathlib.Path):
        pass

    def __filter__(self, index: int, LibraryName: str, Year: int, Month: int, Url: str, ValidUrl: bool, SaveAt: pathlib.Path) -> bool:
        return True

    def batch(self):
        self.logger.info(f'배치 작업을 시작합니다.')
        self.logger.info(f'배치 크기: {len(self)}')
        count = 0
        for i in range(len(self._df)):
            LibraryName, Year, Month, Url, ValidUrl, SaveAt = self._df.iloc[i]
            file = DIR / SaveAt
            if self.__filter__(i, LibraryName, Year, Month, Url, ValidUrl, file):
                self._record_batch_time()
                count += 1
                self.logger.info(f'배치 작업 진행 중: {count/len(self)*100:.1f}% ({count}/{len(self)})')
                self.logger.info(f'예상 소요시간: {time_string(self._estimate_seconds_per_batch()*(len(self)-count))}')
                self.__batch__(i, LibraryName, Year, Month, Url, ValidUrl, file)

    def _record_batch_time(self):
        self._timestamps.append(time.time())
        while len(self._timestamps) > self._max_timestamps:
            self._timestamps.popleft()

    def _estimate_seconds_per_batch(self) -> float:
        return (self._timestamps[-1]-self._timestamps[0]) / len(self._timestamps)


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
