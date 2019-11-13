# -*- encoding:utf-8 -*-

from datetime import datetime
from pandas.io.json import json_normalize
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By

import bs4
import calendar
import inspect
import json
import os
import pandas
import platform
import requests
import sys
import time

USER_AGENT={
    "Darwin": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36",
    "Windows": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36"
}

MAX_TIMEOUT = 30
MAX_RETRIES = 6
URL_FMT_GET_TRADE_DATE_CODE = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx?bld=MKD%2F01%2F0110%2F01100305%2Fmkd01100305_01&name=form&_={}'
URL_GET_TRADE_DATE = 'http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx'
URL_FMT_GET_ISU_INFO_CODE = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx?bld=MKD%2F13%2F1304%2F13040106%2Fmkd13040106&name=form&_={}'
URL_GET_ISU_INFO = 'http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx'
PARAMS_GET_TRADE_DATE_GRIDTP = 'KRX'
PARAMS_GET_TRADE_DATE_PAGEPATH = '/contents/MKD/01/0110/01100305/MKD01100305.jsp'
PARAMS_GET_ISU_INFO_PAGEPATH = '/contents/MKD/13/1304/13040106/MKD13040106.jsp'
CMD_CHROMEDRIVER = '/Applications/chromedriver'
URL_GET_ETP_ISU_LIST = 'http://marketdata.krx.co.kr/mdi#document=13040106'


def requests_retry_session(
        retries=3,
        backoff_factor=3,   # backoff_factor * { 2 ^ (retry_count - 1)} second(s).
        status_forcelist=(500, 502, 504),
        session=None,
):
    session = session or requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor,
                  status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def get_pre_trade_date():
    headers = { 'User-Agent': USER_AGENT[platform.system()] }
    url = URL_FMT_GET_TRADE_DATE_CODE.format(int(time.time() * 1000))
    try:
        html_trade_date_code = requests_retry_session().get(url, headers=headers, timeout=MAX_TIMEOUT)
        if not len(html_trade_date_code.text):
            print("[TRADE_DATE_CODE] : Can't get code")
            return -1
    except Exception as ex:
        print("[TRADE_DATE_CODE] ", ex)
        return -1
    #print('It worked : ', html_trade_date_code.status_code)
    #print('[TRADE_DATE_CODE] : ', len(html_trade_date_code.text), ' (',html_trade_date_code.text, ')')

    try:
        date_yyyy = datetime.now().strftime('%Y')
        params_get_trade_date = {
            'search_bas_yy': date_yyyy,  # 검색년도(YYYY)
            'gridTp': PARAMS_GET_TRADE_DATE_GRIDTP,  # 거래소구분
            'pagePath': PARAMS_GET_TRADE_DATE_PAGEPATH,  # 페이지경로
            'code': html_trade_date_code.text,
            'pageFirstCall': 'Y'
        }
        html_trade_date = requests_retry_session().post(URL_GET_TRADE_DATE, data=params_get_trade_date, headers=headers, timeout=MAX_TIMEOUT)
    except Exception as ex:
        print("[TRADE_DATE] ", ex)
        return -1
    #print('It worked : ', html_trade_date.status_code)
    #print('[TRADE_DATE] : (', html_trade_date.text, ')')

    # TRADE_DATE가 들어있는 HTML 페이지를 JSON 포맷으로 변환후, 지정태그가 있는것만 추출한다.
    try:
        df_hdays = json_normalize(json.loads(html_trade_date.text), 'block1')
        df_hdays = df_hdays['calnd_dd_dy'].str.extract('(\d{4}-\d{2}-\d{2})', expand=False)
        df_hdays = pandas.to_datetime(df_hdays)
    except Exception as ex:
        print(html_trade_date.text)
        print("[JSON] ", ex)
        return -1
    u_start_date = datetime.now().strftime('%Y-01-01')  # 1년의 날짜 생성 시작일 YYYY-01-01
    u_end_date = datetime.now().strftime(
        '%Y-12-' + str(calendar.monthrange(int(date_yyyy), 12)[1]))  # 1년의 날짜 생성 마지막일 YYYY-12-DD
    # 1년의 날짜를 생성하되 freq 를 'B'로 지정(business day)하여 주말(토,일)은 제외한다
    df_mdays = pandas.date_range(u_start_date, u_end_date, freq='B')
    # 개장일 중 휴장일 일자는 drop
    df_mdays = df_mdays.drop(df_hdays)

    # 오늘일자와 비교하여 오늘보다 작은 전일자를 가져온다
    today = datetime.now()
    today = today.strftime('%Y%m%d')
    for i in df_mdays:
        i = i.strftime('%Y%m%d')
        if (i < today):
            preday = i
        else:
            break
    return preday


def etf_lp_trading_trends(driver, tradedate):
    try:
        print('URL(ISU_CD_LIST): ', URL_GET_ETP_ISU_LIST)
        driver.get(URL_GET_ETP_ISU_LIST)
        try:
            element_present = expected_conditions.presence_of_element_located((By.CLASS_NAME, 'CI-GRID-BODY-INNER'))
            WebDriverWait(driver, MAX_TIMEOUT).until(element_present)
        except TimeoutException:
            print("Timed out waiting for page to load.")
            return None
        html_isu_list = driver.page_source
        if not len(html_isu_list):
            print("[ISU_LIST] : Can't get list of ISU_CODE")
            return None
    except Exception as ex:
        print("[ISU_LIST] ", ex)
        return None
    soup = bs4.BeautifulSoup(html_isu_list, 'html.parser')
    all_select = soup.find_all("select", {'name': 'isu_cd'})

    # 종목코드 리스트 저장(각각을 딕셔너리에 넣고, 리스트에 추가)
    isu_cd_list = []
    for s_item in all_select:
        for opt_item in s_item.find_all('option'):
            isu_cd = {}
            isu_cd['isu_cd'] = opt_item.get('value')
            isu_cd['isu_nm'] = opt_item.text
            # 종목코드 전체(ALL)는 제외
            if isu_cd['isu_cd'] != 'ALL':
                isu_cd_list.append(isu_cd)

    # 종목별로 결과를 합치기 위해 빈 DataFrame을 정의해준다
    dfs = pandas.DataFrame([{"일자": "일자", "종목코드": "종목코드", "종목명": "종목명", "상품구분": "상품구분", "매도LP명": "매도LP명", "매도거래량": "매도거래량","매도거래대금": "매도거래대금", "매수LP명": "매수LP명", "매수거래량": "매수거래량", "매수거래대금": "매수거래대금"}])

    for isu in isu_cd_list:
        print(isu['isu_cd'], '[',isu['isu_nm'],']')
        headers = {'User-Agent': USER_AGENT[platform.system()]}
        try:
            url = URL_FMT_GET_ISU_INFO_CODE.format(int(time.time() * 1000))
            html_isu_info_code = requests_retry_session().get(url, headers=headers, timeout=MAX_TIMEOUT)
        except Exception as ex:
            print('[ISU_INFO_CODE] ', ex)
            return None

        try:
            params_get_isu_info = {
                'domforn': '00',  # 기초시장
                'uly_gubun': '00',  # 기초자산
                'gubun': '00',  # 추적배수
                'isu_cd': isu['isu_cd'],  # 종목
                'fromdate': tradedate,  # 전일
                'todate': tradedate,  # 전일
                'pagePath': PARAMS_GET_ISU_INFO_PAGEPATH,
                'code': html_isu_info_code.text,
                'pageFirstCall': 'Y'
            }
            html_isu_info = requests_retry_session().post(URL_GET_ISU_INFO, data=params_get_isu_info, headers=headers
                                                          , timeout=MAX_TIMEOUT)
        except Exception as ex:
            print("[ISU_INFO] ", ex)
            return None

        try:
            df = json_normalize(json.loads(html_isu_info.text), 'block1')
            df = df[['mem_kor_shrt_nm_ofr', 'sofr_vl', 'sofr_amt', 'mem_kor_shrt_nm_bid', 'sbid_vl', 'sbid_amt']]
            df.columns = ['매도LP명', '매도거래량', '매도거래대금', '매수LP명', '매수거래량', '매수거래대금']
            df = df.assign(일자=tradedate)
            df = df.assign(종목코드=isu['isu_cd'])
            df = df.assign(종목명=isu['isu_nm'])
            df = df.assign(상품구분='ETF')
            df['매도LP명'] = df['매도LP명'].str.replace('㈜', '').str.replace('\(주\)', '')
            df['매도거래량'] = df['매도거래량'].str.replace(',', '')
            df['매도거래대금'] = df['매도거래대금'].str.replace(',', '')
            df['매수LP명'] = df['매수LP명'].str.replace('㈜', '').str.replace('\(주\)', '')
            df['매수거래량'] = df['매수거래량'].str.replace(',', '')
            df['매수거래대금'] = df['매수거래대금'].str.replace(',', '')
            print(df)
            # 종목별로 결과를 합쳐 dfs에 저장
            dfs = pandas.concat([dfs, df])
        except Exception as ex:
            # 예외처리필요!!
            print('json_normalize() exception:', ex)
            return None
    return dfs


def etn_lp_trading_trends(driver, tradedate):
    pass

if __name__ == '__main__':
    trade_date = get_pre_trade_date()  # 1일전 영업일을 구한다.
    if 0 > int(trade_date):
        print("[ERROR]: can't get trade_date")
        sys.exit(1)
    print('TRADE_DATE:', trade_date)

    # 크롬 웹드라이버 옵션 지정
    chopts = webdriver.ChromeOptions()
    chopts.add_argument('headless')  # Headless 모드
    chopts.add_argument('window-size=1920x1080')  # 해상도 지정
    #chopts.add_argument('disable-gpu')  # 그래픽가속 사용하지 않음
    chopts.add_argument('user-agent=' + USER_AGENT[platform.system()])
    # 크롬 웹드라이버 위치를 지정해준다
    chdrv = webdriver.Chrome(CMD_CHROMEDRIVER, options=chopts)
    # 암묵적으로 웹 자원 로드를 위해 최대 60초까지 기다려 준다.
    chdrv.implicitly_wait(60)

    dfs_etf = etf_lp_trading_trends(chdrv, trade_date)
    dfs_etn = etn_lp_trading_trends(chdrv, trade_date)
