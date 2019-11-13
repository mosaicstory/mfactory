#-*- encoding:utf-8 -*-
# KRX 휴장일(01023)
#   marketdata.krx.co.kr > 시장동향 > 공시 > 증시일정 > 휴장일   또는 01023 검색
#   http://marketdata.krx.co.kr/mdi#document=01100305
# ETF LP별 매매동향(80116)
#   marketdata.krx.co.kr > 통계 > ETF/ETN/ELW > ETF > LP별 매매동향   또는 80116 검색
#   http://marketdata.krx.co.kr/mdi#document=13040106
# ETN LP별 매매동향(80175)
#   marketdata.krx.co.kr > 통계 > ETF/ETN/ELW > ETN > LP별 매매동향   또는 80175 검색
#   http://marketdata.krx.co.kr/mdi#document=13040206
# 크롬 웹드라이버 다운로드 URL
#   https://sites.google.com/a/chromium.org/chromedriver/downloads
# 참고사이트
#   https://nbviewer.jupyter.org/gist/FinanceData/05271d722d48a6130871c9ad637f9db0
#   https://financedata.github.io/posts/pandas-market-days-krx.html

import time
import requests
import json
import pandas as pd
from pandas.io.json import json_normalize
import sys, os, traceback
import xlwt
from bs4 import BeautifulSoup
from selenium import webdriver
from seleniumrequests import Chrome
from datetime import datetime, timedelta
import calendar

# 일자 설정
# today = datetime.now()
# yesterday = today - timedelta(1)
# strdate = yesterday.strftime('%Y%m%d')
# strdate = 20180202
# print(strdate)

CMD_CHROMEDRIVER="/Applications/chromedriver"
MAX_RETRIES = 3


# KRX 전일영업일 구하기
def preday_search():
    url_tmpl = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx?bld=MKD%2F01%2F0110%2F01100305%2Fmkd01100305_01&name=form&_={}'
    url = url_tmpl.format(int(time.time() * 1000))
    #r = requests.get(url)
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=MAX_RETRIES)
    #session.mount('https://', adapter)
    session.mount('http://', adapter)
    r = session.get(url)
    code = r.text

    u_year = datetime.now().strftime('%Y')
    url = 'http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx'
    data = {
        'search_bas_yy': u_year,  # 검색년도 YYYY
        'gridTp': 'KRX',  # 거래소구분
        'pagePath': '/contents/MKD/01/0110/01100305/MKD01100305.jsp',
        'code': code,
        'pageFirstCall': 'Y'
    }

    #r = requests.post(url, data=data)
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=MAX_RETRIES)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    r = session.post(url, data=data)

    df_hdays = json_normalize(json.loads(r.text), 'block1')
    df_hdays = df_hdays['calnd_dd_dy'].str.extract('(\d{4}-\d{2}-\d{2})', expand=False)
    df_hdays = pd.to_datetime(df_hdays)
    # df_hdays.name = '날짜'
    # print(df_hdays)

    u_start_date = datetime.now().strftime('%Y-01-01')  # 1년의 날짜 생성 시작일 YYYY-01-01
    u_end_date = datetime.now().strftime(
        '%Y-12-' + str(calendar.monthrange(int(u_year), 12)[1]))  # 1년의 날짜 생성 마지막일 YYYY-12-DD
    # 1년의 날짜를 생성하되 freq 를 'B'로 지정(business day)하여 주말(토,일)은 제외한다
    df_mdays = pd.date_range(u_start_date, u_end_date, freq='B')
    # print(df_mdays)
    # 개장일 중 휴장일 일자는 drop
    df_mdays = df_mdays.drop(df_hdays)
    # print(df_mdays)

    # 오늘일자와 비교하여 오늘보다 작은 전일자를 가져온다
    today = datetime.now()
    today = today.strftime('%Y%m%d')
    for i in df_mdays:
        i = i.strftime('%Y%m%d')
        if (i < today):
            preday = i
        else:
            break

    # 종목코드 리스트 저장(각각을 딕셔너리에 넣고, 리스트에 추가)
    # print(preday)
    return preday


# ETF LP별 매매동향
def etf_lp_trading_trends(driver, basedate):
    # url 접근
    driver.get('http://marketdata.krx.co.kr/mdi#document=13040106')
    chdrv.implicitly_wait(20)
    html = driver.page_source  # 페이지의 elements 모두 가져오기
    soup = BeautifulSoup(html, 'html.parser')
    all_select = soup.find_all("select", {'name': 'isu_cd'})
    #print("select:", all_select)
    isu_cd_list = []
    for s in all_select:
        for o in s.find_all('option'):
            isu_cd = {}
            isu_cd['isu_cd'] = o.get('value')
            isu_cd['isu_nm'] = o.text
            # 종목코드 전체(ALL)는 제외
            if isu_cd['isu_cd'] != 'ALL':
                isu_cd_list.append(isu_cd)

    #for i in isu_cd_list:
        #print(i['isu_cd'], i['isu_nm'])
    #print('x'*100)

    # 종목별로 결과를 합치기 위해 빈 DataFrame을 정의해준다
    dfs = pd.DataFrame([{"일자": "일자", "종목코드": "종목코드", "종목명": "종목명", "상품구분": "상품구분", "매도LP명": "매도LP명", "매도거래량": "매도거래량","매도거래대금": "매도거래대금", "매수LP명": "매수LP명", "매수거래량": "매수거래량", "매수거래대금": "매수거래대금"}])

    print("ISU list", isu_cd_list)
    for i in isu_cd_list:
        print(i['isu_cd'], i['isu_nm'])

        try:
            url_tmpl = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx?bld=MKD%2F13%2F1304%2F13040106%2Fmkd13040106&name=form&_={}'
            url = url_tmpl.format(int(time.time() * 1000))
            # r = requests.get(url)
            #session = requests.Session()
            #adapter = requests.adapters.HTTPAdapter(max_retries=MAX_RETRIES)
            #session.mount('https://', adapter)
            #session.mount('http://', adapter)
            #r = session.get(url)
            #code = r.text
            driver.get(url)
            chdrv.implicitly_wait(20)
            html = driver.page_source  # 페이지의 elements 모두 가져오기
            soup = BeautifulSoup(html, 'html.parser')

            url = 'http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx'
            data = {
                'domforn': '00',  # 기초시장
                'uly_gubun': '00',  # 기초자산
                'gubun': '00',  # 추적배수
                'isu_cd': i['isu_cd'],  # 종목
                'fromdate': basedate,  # 전일
                'todate': basedate,  # 전일
                'pagePath': '/contents/MKD/13/1304/13040106/MKD13040106.jsp',
                'code': soup.text,
                'pageFirstCall': 'Y'
            }
            session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(max_retries=MAX_RETRIES)
            session.mount('https://', adapter)
            session.mount('http://', adapter)
            r = session.post(url, data=data)
            print(r.text)
            return None
        except Exception as ex:
            print("Exception:", ex)
            return None

        try:
            df = json_normalize(json.loads(r.text), 'block1')
            print(df)
            df = df[['mem_kor_shrt_nm_ofr', 'sofr_vl', 'sofr_amt', 'mem_kor_shrt_nm_bid', 'sbid_vl', 'sbid_amt']]
            df.columns = ['매도LP명', '매도거래량', '매도거래대금', '매수LP명', '매수거래량', '매수거래대금']
            df = df.assign(일자=basedate)
            df = df.assign(종목코드=i['isu_cd'])
            df = df.assign(종목명=i['isu_nm'])
            df = df.assign(상품구분='ETF')
            df['매도LP명'] = df['매도LP명'].str.replace('㈜', '').str.replace('\(주\)', '')
            df['매도거래량'] = df['매도거래량'].str.replace(',', '')
            df['매도거래대금'] = df['매도거래대금'].str.replace(',', '')
            df['매수LP명'] = df['매수LP명'].str.replace('㈜', '').str.replace('\(주\)', '')
            df['매수거래량'] = df['매수거래량'].str.replace(',', '')
            df['매수거래대금'] = df['매수거래대금'].str.replace(',', '')
            print(df)
            # 종목별로 결과를 합쳐 dfs에 저장
            dfs = pd.concat([dfs, df])
        except Exception as ex:
            # 예외처리필요!!
            print('json_normalize() exception:', ex)

    return dfs


# ETN LP별 매매동향
def etn_lp_trading_trends(drier, basedate):
    # url 접근
    driver.get('http://marketdata.krx.co.kr/mdi#document=13040206')
    html = driver.page_source  # 페이지의 elements 모두 가져오기
    soup = BeautifulSoup(html, 'html.parser')
    select = soup.find_all('select', attrs={'name': 'isu_cd'})

    # 종목코드 리스트 저장(각각을 딕셔너리에 넣고, 리스트에 추가)
    isu_cd_list = []
    for l_select in select:
        for l_option in l_select.find_all('option'):
            isu_cd = {}
            isu_cd['isu_cd'] = l_option.get('value')
            isu_cd['isu_nm'] = l_option.text
            # 종목코드 전체(ALL)는 제외
            if isu_cd['isu_cd'] != 'ALL':
                isu_cd_list.append(isu_cd)

    # print(isu_cd_list)
    # for i in isu_cd_list:
    #    print(i['isu_cd'], i['isu_nm'])

    # 종목별로 결과를 합치기 위해 빈 DataFrame을 정의해준다
    dfs = pd.DataFrame([{"일자": "일자", "종목코드": "종목코드", "종목명": "종목명", "상품구분": "상품구분", "매도LP명": "매도LP명", "매도거래량": "매도거래량",
                         "매도거래대금": "매도거래대금", "매수LP명": "매수LP명", "매수거래량": "매수거래량", "매수거래대금": "매수거래대금"}])

    for i in isu_cd_list:
        print(i['isu_cd'], i['isu_nm'])

        url_tmpl = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx?bld=MKD%2F13%2F1304%2F13040206%2Fmkd13040206&name=form&_={}'
        url = url_tmpl.format(int(time.time() * 1000))
        #r = requests.get(url)
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=MAX_RETRIES)
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        r = session.get(url)
        code = r.text
        url = 'http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx'
        data = {
            'domforn': '00',  # 기초시장
            'uly_gubun': '00',  # 기초자산
            'gubun': '00',  # 추적배수
            'isu_cd': i['isu_cd'],  # 종목
            'fromdate': basedate,  # 전일
            'todate': basedate,  # 전일
            'pagePath': '/contents/MKD/13/1304/13040206/MKD13040206.jsp',
            'code': code,
            'pageFirstCall': 'Y'
        }

        # r = requests.post(url, data=data)
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=MAX_RETRIES)
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        r = session.post(url, data=data)

        df = json_normalize(json.loads(r.text), 'block1')
        # print(df)

        # 결과가 있는 경우에만 결과물을 엑셀로 저장한다
        if len(df) != 0:
            df = df[['mem_kor_shrt_nm_ofr', 'sofr_vl', 'sofr_amt', 'mem_kor_shrt_nm_bid', 'sbid_vl', 'sbid_amt']]
            df.columns = ['매도LP명', '매도거래량', '매도거래대금', '매수LP명', '매수거래량', '매수거래대금']
            #df = df.assign(일자=basedate)
            #df = df.assign(종목코드=i['isu_cd'])
            #df = df.assign(종목명=i['isu_nm'])
            #df = df.assign(상품구분='ETN')
            df['매도LP명'] = df['매도LP명'].str.replace('㈜', '').str.replace('\(주\)', '')
            df['매도거래량'] = df['매도거래량'].str.replace(',', '')
            df['매도거래대금'] = df['매도거래대금'].str.replace(',', '')
            df['매수LP명'] = df['매수LP명'].str.replace('㈜', '').str.replace('\(주\)', '')
            df['매수거래량'] = df['매수거래량'].str.replace(',', '')
            df['매수거래대금'] = df['매수거래대금'].str.replace(',', '')
            # print(df)
            # 종목별로 결과를 합쳐 dfs에 저장
            dfs = pd.concat([dfs, df])

    return dfs

try:
    # 이전에 에러가 기록된 파일 및 결과물 파일은 삭제한다.
    os.unlink('lp_trading_trends_error.txt')
    os.unlink('lp_trading_trends.xls')
except:
    pass

try:
    # 평소에는 전일영업일을 구하지만, 일자를 인자로 받은 경우에는 입력받은 일자로 진행한다
    if len(sys.argv) > 1:
        base_date = str(sys.argv[1])
    else:
        #base_date = preday_search()
        base_date = '20191111'

    print('기준일자:', base_date)

    # 크롬 웹드라이버 옵션 지정
    chopts = webdriver.ChromeOptions()
    chopts.add_argument('headless')  # Headless 모드
    chopts.add_argument('window-size=1920x1080')  # 해상도 지정
    chopts.add_argument('disable-gpu')  # 그래픽가속 사용하지 않음
    print("="*80)
    # 크롬 웹드라이버 위치를 지정해준다
    chdrv = webdriver.Chrome(CMD_CHROMEDRIVER, options=chopts)
    # 암묵적으로 웹 자원 로드를 위해 최대 60초까지 기다려 준다.
    chdrv.implicitly_wait(60)

    dfs_etf = etf_lp_trading_trends(chdrv, base_date)
    sys.exit(0)
    dfs_etn = etn_lp_trading_trends(chdrv, base_date)

    # 웹드라이버 닫기
    driver.close()

    # 컬럼 정의 순서대로 맞춤
    dfs_etf = dfs_etf[['일자', '종목코드', '종목명', '상품구분', '매도LP명', '매도거래량', '매도거래대금', '매수LP명', '매수거래량', '매수거래대금']]
    dfs_etn = dfs_etn[['일자', '종목코드', '종목명', '상품구분', '매도LP명', '매도거래량', '매도거래대금', '매수LP명', '매수거래량', '매수거래대금']]
    # ETF, ETN의 결과를 합쳐 하나의 파일로 저장한다
    dfs_etf = dfs_etf[1:len(dfs_etf)]
    dfs_etn = dfs_etn[1:len(dfs_etn)]
    dfs_all = pd.concat([dfs_etf, dfs_etn])

    writer = pd.ExcelWriter('lp_trading_trends.xls')
    dfs_all.to_excel(writer, sheet_name='Sheet1', index=False, header=False, na_rep=' ', encoding='utf-8')
    writer.save()
except Exception as ex:
    print("Exception: ", ex)
    # 에러가 발생한 경우 StackTrace를 파일로 기록한다.
    outputFile = open('lp_trading_trends_error.txt', 'w')
    traceback.print_exc(file=outputFile)
    outputFile.close()
