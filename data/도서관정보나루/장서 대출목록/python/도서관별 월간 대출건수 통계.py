#!python

import sys

import pandas as pd

from config import DIR
from config import DATASOURCE_FILE
from util import get_logger
from util import time_string
from util import memory_string
from util import add_time_record
from util import get_average_time
from util import escape_csv_double_quotes
from util import is_valid_csv


DATASOURCE = pd.read_csv(DATASOURCE_FILE)

logger = get_logger('data-analysis:도서관별 월간 대출건수')


def main():
    start = 0  # 1000으로 나누어 떨어지는 숫자로 지정을 권장
    data_dict = {
        '도서권수': [],
        '대출건수': [],
        '도서관명': [],
        '년': [],
        '월': [],
    }
    series = DATASOURCE[['LibraryName', 'SaveAt', 'Month', 'Year']]
    for i in range(start-1, len(series)):
        LibraryName, SaveAt, Month, Year = series.iloc[i]
        file = DIR / str(SaveAt)

        if not file.exists():
            logger.warning(f'파일이 존재하지 않습니다: "{file}"')
            continue

        add_time_record(maxsize=50)
        logger.info(f'[{i+1}/{len(series)}] "{file}"')
        logger.info(f'[time left = {time_string(get_average_time()*(len(series)-i))}]\t[memory usage = {memory_string(check_dict_memory(data_dict))}]')

        try:
            df = pd.read_csv(file, low_memory=False)
            assert '대출건수' in df
            assert '도서권수' in df
        except AssertionError as e:
            logger.error(f'파일을 파싱하는것에 실패했습니다: {file}')
            logger.error(f'대출건수 혹은 도서권수가 존재하지 않습니다.')
            logger.error(', '.join(df.columns))
            logger.error(e)
        except Exception as e:
            logger.error(f'파일을 파싱하는것에 실패했습니다: {file}')
            logger.error(e)
        else:
            data_dict['도서관명'].append(LibraryName)
            data_dict['대출건수'].append(df['대출건수'].sum())
            data_dict['도서권수'].append(df['도서권수'].sum())
            data_dict['년'].append(Year)
            data_dict['월'].append(Month)
            del df

        if (i+1) % 1000 == 0:
            df = pd.DataFrame(data_dict)
            file = DIR / f'도서관별_월간_대출건수 ({start+1}~{i+1} checkpoint).csv'
            logger.info(f'Saving checkpoint... "{file}"')
            df.to_csv(file, index=False)
            del df

    df = pd.DataFrame(data_dict)
    df.to_csv(DIR / f'도서관별_월간_대출건수 ({len(df)} rows).csv', index=False)
    logger.info(f'DONE parsing {len(df)} rows.')
    del df

def check_dict_memory(data_dict: dict) -> int:
    return sys.getsizeof(data_dict)+sum(map(sys.getsizeof, data_dict.values()))


main()
