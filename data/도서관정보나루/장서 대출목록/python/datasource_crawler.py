#!python

"""도서관 정보나루 - 장서 대출목록 크롤러

장서 대출목록 웹 페이지 주소는 아래와 같이 이루어져 있다.

[1] "https://www.data4library.kr/openDataL": 도서관 목록
[2] "https://www.data4library.kr/openDataV": 도서관 별 데이터 목록

[1]번에서 form을 submit하여 POST요청을 선행해야 [2]번 주소를 GET했을 때, 앞서 제출한 form에 해당하는 도서관 정보가 화면에 출력되는 괴상한 구조를 갖고 있다.
아마도 정적 크롤링 방지를 위한 조치 같아보이는데, Selenium을 이용한 동적 크롤링으로 우회한다.

데이터 크롤링에는 약 2시간 반 정도가 소요되는 것으로 보인다.
크롤링한 데이터는 "datasource.csv"로 저장되며,
수집하는 데이터는 [도서관명] [년] [월] [데이터를 다운로드 받을 수 있는 url]이다.
"""

import re
import typing

from pandas import DataFrame
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.wait import WebDriverWait

from config import DATASOURCE_FILE
from util import get_logger


logger = get_logger('crawler')
logger_opendata_l = get_logger('crawler:openDataL (도서관 목록)')
logger_opendata_v = get_logger('crawler:openDataV (도서관 데이터)')


def crawl_openDataL(driver: WebDriver) -> typing.Dict[str, str]:
    logger = logger_opendata_l

    def count_pages() -> int:
        elem = driver.find_element(by=By.XPATH, value='//*[@id="pagef"]/div[4]/a[4]')
        match = re.search(r"goPage\('(?P<count>\d+)'\);", elem.get_attribute(name='onclick'))
        return int(match['count'])

    def go_to_page(page_no: int):
        logger.info(f'{page_no}/{count_pages()} 페이지로 이동')
        driver.execute_script(f"goPage('{page_no}')")
        table = driver.find_element(by=By.XPATH, value='//*[@id="pagef"]/div[3]/table')
        wait = WebDriverWait(driver, timeout=5)
        wait.until(lambda _: table.is_displayed())

    def get_row_count() -> int:
        rows = driver.find_elements(by=By.XPATH, value='//*[@id="pagef"]/div[3]/table/tbody/tr/td[1]/a')
        logger.info(f'{len(rows)}개의 행 발견됨.')
        return len(rows)

    def go_to_openDataV(row_no: int):
        row = driver.find_element(by=By.XPATH, value=f'//*[@id="pagef"]/div[3]/table/tbody/tr[{row_no}]/td[1]/a')
        logger.info(f'도서관 데이터 페이지로 이동 ({row.text.strip()})')
        row.click()
        wait = WebDriverWait(driver, timeout=5)
        wait.until(lambda _: driver.current_url == 'https://www.data4library.kr/openDataV')

    csv_urls = {}
    driver.get(f"https://www.data4library.kr/openDataL")
    for page_no in range(1, count_pages()+1):
        go_to_page(page_no)
        if (row_count := get_row_count()) == 0:
            logger.warning(f'이 페이지에 도서관 정보가 존재하지 않음.')
            continue
        for i in range(row_count):
            try:
                go_to_openDataV(i+1)
                for name, url in crawl_openDataV(driver).items():
                    csv_urls[name] = url
            except Exception as e:
                logger.error(e)
                logger.error(f'{page_no} 페이지 {i+1}행 파싱에서 오류 발생.')
                pass
            driver.get(f"https://www.data4library.kr/openDataL")
            go_to_page(page_no)
    return csv_urls


def crawl_openDataV(driver: WebDriver) -> typing.Dict[str, str]:
    logger = logger_opendata_v

    def count_pages() -> int:
        elem = driver.find_element(by=By.XPATH, value='//*[@id="sb-site"]/section/div[2]/div[4]/a[4]')
        match = re.search(r"goPage\('(?P<count>\d+)'\);", elem.get_attribute(name='onclick'))
        return int(match['count'])

    def go_to_page(page_no: int):
        logger.info(f'{page_no}/{count_pages()} 페이지로 이동')
        driver.execute_script(f"goPage('{page_no}')")
        table = driver.find_element(by=By.XPATH, value='//*[@id="sb-site"]/section/div[2]/div[3]/table')
        wait = WebDriverWait(driver, timeout=5)
        wait.until(lambda _: table.is_displayed())

    def get_row_count() -> int:
        rows = driver.find_elements(by=By.XPATH, value='//*[@id="sb-site"]/section/div[2]/div[3]/table/tbody/tr/td[4]/a')
        logger.info(f'{len(rows)}개의 행 발견됨.')
        return len(rows)

    assert driver.current_url == 'https://www.data4library.kr/openDataV', driver.current_url
    csv_urls = {}
    for page_no in range(1, count_pages()+1):
        go_to_page(page_no)
        if (row_count := get_row_count()) == 0:
            logger.warning(f'이 페이지에 데이터가 존재하지 않음.')
            continue
        for i in range(row_count):
            row = driver.find_element(by=By.XPATH, value=f'//*[@id="sb-site"]/section/div[2]/div[3]/table/tbody/tr[{i+1}]')
            name = row.find_element(by=By.XPATH, value=f'.//td[2]').text
            url = row.find_element(by=By.XPATH, value=f'.//td[4]/a').get_attribute('data-url')
            csv_urls[name] = url
    return csv_urls


def main():
    # 데이터 크롤링
    try:
        options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(options=options)
        data = crawl_openDataL(driver)
    except Exception as e:
        logger.error(e)
        data = {}
    finally:
        driver.quit()

    logger.info(f'{len(data)}개의 데이터 발견됨.')

    # 정보 파싱
    data_dict = {
        'LibraryName': [],
        'Year': [],
        'Month': [],
        'Url': [],
        'ValidUrl': [],
        'SaveAt': [],
    }
    name_pattern = re.compile(r'(?P<도서관명>.+) ((장서 대출목록)|(장서/대출목록)|(장서목록)) \((?P<년>\d+)년 (?P<월>\d+)월\)')
    for name, url in data.items():
        try:
            match = name_pattern.search(name).groupdict()
            assert match['도서관명'] is not None
            assert match['년'] is not None
            assert match['월'] is not None
        except Exception:
            print(f'다음의 데이터 명을 파싱하는 과정에서 예외 발생:')
            print(f'-> "{name}"')
            print(f"{match['도서관명']}")
            break
        else:
            도서관명 = match['도서관명']
            년 = int(match['년'])
            월 = int(match['월'])
            filename = f'./도서관별/{도서관명}/{년}-{월:02d}.csv'

            data_dict['LibraryName'].append(도서관명)
            data_dict['Year'].append(년)
            data_dict['Month'].append(월)
            data_dict['Url'].append('https://www.data4library.kr'+url)
            data_dict['ValidUrl'].append(True)
            data_dict['SaveAt'].append(filename)

    # 데이터 프레임으로 변환 및 저장
    df = DataFrame(data_dict)
    df = df.sort_values(by='SaveAt')
    df.to_csv(DATASOURCE_FILE, index=False)


main()
