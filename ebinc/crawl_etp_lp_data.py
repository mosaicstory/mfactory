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
import copy
import inspect
import json
import os
import pandas
import platform
import requests
import sys
import time
import traceback

MAX_TIMEOUT = 30
MAX_RETRIES = 6
USER_AGENT = {
    "Darwin": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36",
    "Windows": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36"
}
# 크롬드라이버 위치
CMD_CHROMEDRIVER = '/Applications/chromedriver'

# 아래 URL과 기타 파라미터값들은 사이트가 변경되었을경우, 적절하게 조정해야함.
URL_GET_INFO = 'https://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx'
# 휴장일정보용
URL_GEN_OTP4DATE = 'https://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx?bld=MKD%2F01%2F0110%2F01100305%2Fmkd01100305_01&name=form&_={}'
PARAMS_DATE_PAGEPATH = '/contents/MKD/01/0110/01100305/MKD01100305.jsp'
# ETF용
URL_GEN_OTP4ETF = 'https://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx?bld=MKD%2F13%2F1304%2F13040106%2Fmkd13040106&name=form&_={}'
URL_GET_ETF_LIST = 'https://marketdata.krx.co.kr/mdi#document=13040106'
PARAMS_ETF_PAGEPATH = '/contents/MKD/13/1304/13040106/MKD13040106.jsp'
# ETN용
URL_GEN_OTP4ETN = 'https://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx?bld=MKD%2F13%2F1304%2F13040206%2Fmkd13040206&name=form&_={}'
URL_GET_ETN_LIST = 'https://marketdata.krx.co.kr/mdi#document=13040206'
PARAMS_ETN_PAGEPATH = '/contents/MKD/13/1304/13040206/MKD13040206.jsp'

# Dataframe 초기화용
EMPTY_DATAFRAME = pandas.DataFrame([{
                                        "일자": "일자",
                                        "종목코드": "종목코드",
                                        "종목명": "종목명",
                                        "상품구분": "상품구분",
                                        "매도LP명": "매도LP명",
                                        "매도거래량": "매도거래량",
                                        "매도거래대금": "매도거래대금",
                                        "매수LP명": "매수LP명",
                                        "매수거래량": "매수거래량",
                                        "매수거래대금": "매수거래대금"
                                    }])

def requests_retry_session(retries=3, backoff_factor=3,  # backoff_factor * { 2 ^ (retry_count - 1)} second(s).
                           status_forcelist=(500, 502, 504), session=None, ):
    session = session or requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor,
                  status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def load_webdriver():
    # 크롬 웹드라이버 옵션 지정
    chopts = webdriver.ChromeOptions()
    chopts.add_argument('headless')  # Headless 모드
    chopts.add_argument('window-size=1920x1080')  # 해상도 지정
    # chopts.add_argument('disable-gpu')  # 그래픽가속 사용하지 않음
    chopts.add_argument('user-agent=' + USER_AGENT[platform.system()])
    # 크롬 웹드라이버 위치를 지정해준다
    chdrv = webdriver.Chrome(CMD_CHROMEDRIVER, options=chopts)
    # 암묵적으로 웹 자원 로드를 위해 최대 60초까지 기다려 준다.
    chdrv.implicitly_wait(60)
    return chdrv


def get_session_code(url, headers):
    return requests_retry_session().get(url, headers=headers, timeout=MAX_TIMEOUT)


def get_pre_trade_date(headers):
    try:
        code = get_session_code(URL_GEN_OTP4DATE.format(int(time.time() * 1000)), headers)
    except Exception as ex:
        print("[TRADE_DATE] Can't get session code : ", ex)
        outputFile = open('lp_trading_trends_error.txt', 'w')
        traceback.print_exc(file=outputFile)
        outputFile.close()
        return -1

    try:
        date_yyyy = datetime.now().strftime('%Y')
        params_get_trade_date = {
            'search_bas_yy': date_yyyy,  # 검색년도(YYYY)
            'gridTp': 'KRX',  # 거래소구분
            'pagePath': PARAMS_DATE_PAGEPATH,  # 페이지경로
            'code': code.text,
            'pageFirstCall': 'Y'
        }
        html_trade_date = requests_retry_session().post(URL_GET_INFO, data=params_get_trade_date, headers=headers,
                                                        timeout=MAX_TIMEOUT)
    except Exception as ex:
        print("[TRADE_DATE] ", ex)
        outputFile = open('lp_trading_trends_error.txt', 'w')
        traceback.print_exc(file=outputFile)
        outputFile.close()
        return -1
    # TRADE_DATE가 들어있는 HTML 페이지를 JSON 포맷으로 변환후, 지정태그가 있는것만 추출한다.
    try:
        df_hdays = json_normalize(json.loads(html_trade_date.text), 'block1')
        df_hdays = df_hdays['calnd_dd_dy'].str.extract('(\d{4}-\d{2}-\d{2})', expand=False)
        df_hdays = pandas.to_datetime(df_hdays)
    except Exception as ex:
        print(html_trade_date.text)
        print("[JSON] ", ex)
        outputFile = open('lp_trading_trends_error.txt', 'w')
        traceback.print_exc(file=outputFile)
        outputFile.close()
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


def etp_get_isu_list(url):
    driver = load_webdriver()
    try:
        print('URL(ISU_LIST): ', url)
        driver.get(url)
        try:
            # !!TODO!! 종목정보가 나오는부분의 태그 클래스명: 'CI-GRID-BODY-INNER'.
            element_present = expected_conditions.presence_of_element_located((By.CLASS_NAME, 'CI-GRID-BODY-INNER'))
            WebDriverWait(driver, MAX_TIMEOUT).until(element_present)
        except TimeoutException:
            print("Timed out waiting for page to load: ", url)
            outputFile = open('lp_trading_trends_error.txt', 'w')
            traceback.print_exc(file=outputFile)
            outputFile.close()
            driver.quit()
            return None
        html_isu_list = driver.page_source
        if not len(html_isu_list):
            print("[ISU_LIST] : Can't get list of ISU_CODE")
            driver.quit()
            return None
    except Exception as ex:
        print("[ISU_LIST] ", ex)
        outputFile = open('lp_trading_trends_error.txt', 'w')
        traceback.print_exc(file=outputFile)
        outputFile.close()
        driver.quit()
        return None
    soup = bs4.BeautifulSoup(html_isu_list, 'html.parser')
    all_select = soup.find_all("select", {'name': 'isu_cd'})
    driver.quit()
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
    return isu_cd_list


def etp_get_isu_info(url, isu, tradedate, pagepath, code, headers):
    params = {
        'domforn': '00',  # 기초시장
        'uly_gubun': '00',  # 기초자산
        'gubun': '00',  # 추적배수
        'isu_cd': isu['isu_cd'],  # 종목
        'fromdate': tradedate,  # 전일
        'todate': tradedate,  # 전일
        'pagePath': pagepath,
        'code': code,
        'pageFirstCall': 'Y'
    }
    return requests_retry_session().post(url, data=params, headers=headers, timeout=MAX_TIMEOUT)


def etp_convert_isuinfo_to_dataframe(html, tradedate, isu, etp_type):
    try:
        df = json_normalize(json.loads(html.text), 'block1')
        df = df[['mem_kor_shrt_nm_ofr', 'sofr_vl', 'sofr_amt', 'mem_kor_shrt_nm_bid', 'sbid_vl', 'sbid_amt']]
        df.columns = ['매도LP명', '매도거래량', '매도거래대금', '매수LP명', '매수거래량', '매수거래대금']
        df = df.assign(일자=tradedate)
        df = df.assign(종목코드=isu['isu_cd'])
        df = df.assign(종목명=isu['isu_nm'])
        df = df.assign(상품구분=etp_type)
        df['매도LP명'] = df['매도LP명'].str.replace('㈜', '').str.replace('\(주\)', '')
        df['매도거래량'] = df['매도거래량'].str.replace(',', '')
        df['매도거래대금'] = df['매도거래대금'].str.replace(',', '')
        df['매수LP명'] = df['매수LP명'].str.replace('㈜', '').str.replace('\(주\)', '')
        df['매수거래량'] = df['매수거래량'].str.replace(',', '')
        df['매수거래대금'] = df['매수거래대금'].str.replace(',', '')
    except Exception as ex:
        print('[json_normalize, dataframe] : ', ex)
        outputFile = open('lp_trading_trends_error.txt', 'w')
        traceback.print_exc(file=outputFile)
        outputFile.close()
        return None
    return df

def etf_lp_trading_trends(tradedate, headers):
    driver = load_webdriver()
    dfs = copy.deepcopy(EMPTY_DATAFRAME)
    isu_cd_list = etp_get_isu_list(URL_GET_ETF_LIST)
    if isu_cd_list is None:
        driver.quit()
        return dfs
    try:
        code = get_session_code(URL_GEN_OTP4ETF.format(int(time.time() * 1000)), headers)
    except Exception as ex:
        print("[ETF] Can't get session code : ", ex)
        outputFile = open('lp_trading_trends_error.txt', 'w')
        traceback.print_exc(file=outputFile)
        outputFile.close()
        driver.quit()
        return dfs

    total_cnt = len(isu_cd_list)
    for idx, isu in enumerate(isu_cd_list):
        try:
            print('[ETF] ', idx, '/', total_cnt, ' : ', isu['isu_cd'], '[', isu['isu_nm'], ']')
            html_isu_info = etp_get_isu_info(URL_GET_INFO, isu, tradedate, PARAMS_ETF_PAGEPATH, code.text, headers)
        except Exception as ex:
            print("[ETF] Can't get ISU info : ", ex)
            outputFile = open('lp_trading_trends_error.txt', 'w')
            traceback.print_exc(file=outputFile)
            outputFile.close()
            time.sleep(1)
            continue
        df = etp_convert_isuinfo_to_dataframe(html_isu_info, tradedate, isu, 'ETF')
        if df is None:
            outputFile = open('lp_trading_trends_error.txt', 'w')
            traceback.print_exc(file=outputFile)
            outputFile.close()
            time.sleep(1)
            continue
        dfs = pandas.concat([dfs, df], sort=False) # 종목별로 결과를 합쳐 dfs에 저장

    driver.quit()
    return dfs


def etn_lp_trading_trends(tradedate, headers):
    driver = load_webdriver()
    dfs = copy.deepcopy(EMPTY_DATAFRAME)
    isu_cd_list = etp_get_isu_list(URL_GET_ETN_LIST)
    if isu_cd_list is None:
        driver.quit()
        return dfs
    try:
        code = get_session_code(URL_GEN_OTP4ETN.format(int(time.time() * 1000)), headers)
    except Exception as ex:
        print("[ETN] Can't get session code : ", ex)
        outputFile = open('lp_trading_trends_error.txt', 'w')
        traceback.print_exc(file=outputFile)
        outputFile.close()
        driver.quit()
        return dfs

    total_cnt = len(isu_cd_list)
    for idx, isu in enumerate(isu_cd_list):
        try:
            print('[ETN] ', idx, '/', total_cnt, ' : ', isu['isu_cd'], '[', isu['isu_nm'], ']')
            html_isu_info = etp_get_isu_info(URL_GET_INFO, isu, tradedate, PARAMS_ETN_PAGEPATH, code.text, headers)
        except Exception as ex:
            print("[ETN] Can't get ISU info : ", ex)
            time.sleep(1)
            continue
        df = etp_convert_isuinfo_to_dataframe(html_isu_info, tradedate, isu, 'ETN')
        if df is None:
            time.sleep(1)
            continue
        dfs = pandas.concat([dfs, df], sort=False) # 종목별로 결과를 합쳐 dfs에 저장

    driver.quit()
    return dfs


if __name__ == '__main__':
    try:
        headers = {'User-Agent': USER_AGENT[platform.system()]}
        tradedate = get_pre_trade_date(headers)  # 1일전 영업일을 구한다.
        if 0 > int(tradedate):
            print("[ERROR]: can't get trade_date")
            sys.exit(1)
        print('TRADE_DATE:', tradedate)

        dfs_etf = etf_lp_trading_trends(tradedate, headers)
        dfs_etn = etn_lp_trading_trends(tradedate, headers)

        # 컬럼 정의 순서대로 맞춤
        dfs_etf = dfs_etf[['일자', '종목코드', '종목명', '상품구분', '매도LP명', '매도거래량', '매도거래대금', '매수LP명', '매수거래량', '매수거래대금']]
        dfs_etf = dfs_etf[1:len(dfs_etf)]

        dfs_etn = dfs_etn[['일자', '종목코드', '종목명', '상품구분', '매도LP명', '매도거래량', '매도거래대금', '매수LP명', '매수거래량', '매수거래대금']]
        dfs_etn = dfs_etn[1:len(dfs_etn)]

        # ETF, ETN의 결과를 합쳐 하나의 파일로 저장한다
        dfs_all = pandas.concat([dfs_etf, dfs_etn])

        writer = pandas.ExcelWriter('lp_trading_trends.xls')
        dfs_all.to_excel(writer, sheet_name='Sheet1', index=False, header=False, na_rep=' ', encoding='utf-8')
        writer.save()
    except Exception as ex:
        print("Exception: ", ex)
        # 에러가 발생한 경우 StackTrace를 파일로 기록한다.
        outputFile = open('lp_trading_trends_error.txt', 'w')
        traceback.print_exc(file=outputFile)
        outputFile.close()
