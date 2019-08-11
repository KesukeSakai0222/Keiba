import requests
from tqdm import tqdm
import time
from bs4 import BeautifulSoup
import pandas as pd
import os
import sys
import logging


def numStr(num: int):
    '''
    数字を0埋めして2桁で返す
    '''
    return str(num).zfill(2)


def scrape_common(soup: BeautifulSoup, designated_year: int):
    """
    ページ上の共通部分を抜き出します．
    @param soup:BeautifulSoup
    @param designated_year: int
    @retun CommonParam:dict
    """
    CommonParam = {}
    CommonParam["year"] = designated_year
    CommonParam["date"] = soup.find_all('div', attrs={'class', 'Change_Btn Day'})[0].string.strip()
    CommonParam["field"] = soup.find_all('div', attrs={'class', 'Change_Btn Course'})[0].string.strip()
    CommonParam["race"] = soup.find_all('div', attrs={'Race_Num'})[0].span.string
    CommonParam["race_name"] = soup.find_all('dt', attrs={'class', 'Race_Name'})[0].contents[0].strip()
    CommonParam["course"] = soup.find_all('dd', attrs={'Race_Data'})[0].span.string
    CommonParam["head_count"] = soup.find_all('dd', attrs={'class', 'Race_Data'})[0].contents[3].split()[1]

    return CommonParam


def get_one_record(soup, common, df_col, m):
    """
    ページ上の一頭の馬についての結果を抜き出しpd.Seriesとして返します
    @param soup: BeautifulSoup
    @param common: 共通部分のdict
    @param df_col: カラム名のlist
    @param m: 調べる馬の順位int
    @return dst: pd.Series
    """
    dst = pd.Series(index=df_col)

    dst['year'] = common['year']
    dst['date'] = common["date"]
    dst['field'] = common['field']
    dst['race'] = common['race']
    dst['race_name'] = common['race_name']
    dst['course'] = common['course']
    dst['head_count'] = common['head_count']
    dst['rank'] = soup.find_all('div', attrs='Rank')[m].contents[0]
    dst['horse_name'] = soup.find_all('dt', attrs=['class', 'Horse_Name'])[m].a.string

    detailL = soup.find_all('span', attrs=['class', 'Detail_Left'])[m]
    dst['gender'] = list(detailL.contents[0].split()[0])[0]
    dst['age'] = list(detailL.contents[0].split()[0])[1]
    dst['trainerA'] = detailL.span.string.split('･')[0]
    dst['trainerB'] = detailL.span.string.split('･')[1]
    if len(detailL.contents[0].split()) >= 2:
        dst['weight'] = detailL.contents[0].split()[1].split('(')[0]
        if len(detailL.contents[0].split()[1].split('(')) >= 2:
            dst['c_weight'] = detailL.contents[0].split()[1].split('(')[1].strip(')')
    detailR = soup.find_all('span', attrs=['class', 'Detail_Right'])[m].contents
    if  detailR[0] in ("\n", "\n▲", '\n☆', "\n△"):
        detailR.pop(0)
    dst['jackie'] = detailR[0].string.strip()
    dst['j_weight'] = detailR[2].strip().replace('(', '').replace(')', '')
    Odds = soup.find_all('td', attrs=['class', 'Odds'])[m].contents[1]
    if Odds.dt.string is not None:
        dst['odds'] = Odds.dt.string.strip('倍')
        dst['popu'] = Odds.dd.string.strip('人気')

    return dst


if __name__ == "__main__":
    designated_year = 2018
    fmt = "%(asctime)s %(levelname)s %(name)s :%(message)s"
    logging.basicConfig(filename='logfile/logger.log', level=logging.INFO, format=fmt)
    logging.info("CSV取り込みを開始")
    DF_MEMORY_SIZE = 4000000
    
    BASE = "http://race.sp.netkeiba.com/?pid=race_result&race_id="
    DF_COL = ['year', 'date', 'field', 'race', 'race_name',
              'course', 'head_count', 'rank', 'horse_name',
              'gender', 'age', 'trainerA', 'trainerB', 'weight',
              'c_weight', 'jackie', 'j_weight','odds', 'popu']

    if not os.path.exists('./data'):
        os.mkdir('./data')

    df = pd.DataFrame(columns=DF_COL)
    csv_count = 0
    logging.debug('DataFrameの作成 完了')

    for i in tqdm(range(1, 11)):
        logging.info("i = {}".format(i))
        for j in range(1, 11):
            logging.info("j = {}".format(j))
            logging.info("df size:{}bite".format(sys.getsizeof(df)))
            for k in range(1, 11):
                for l in range(1, 13):
                    # urlでぶっこ抜く
                    page_id = str(designated_year) + numStr(i) + numStr(j) + numStr(k) + numStr(l)
                    url = BASE + page_id
                    html = requests.get(url)
                    time.sleep(1)
                    logging.debug('html取得')
                    html.encoding = 'EUC-JP'

                    # scraping
                    soup = BeautifulSoup(html.text, 'html.parser')
                    logging.debug('parser完了')
                    # ページがあるかの判定
                    if soup.find_all('div', attrs={'class', 'Result_Guide'}) != []:
                        logging.debug('ページ無し')
                        break
                    else:
                        logging.debug('ページ有り')
                        # 共通部分を抜き出す
                        common = scrape_common(soup, designated_year)
                        for m in range(len(soup.find_all('div', attrs='Rank'))):
                            try:
                                dst = get_one_record(soup, common, DF_COL, m)
                            except:
                                logging.error(url + ' {}番でレコード取得失敗\n'.format(m))
                                logging.error("i={}, j={}, k={}, l={}, m={}".format(i, j, k, l, m))
                                sys.exit(1)
                            dst.name = page_id + numStr(m)
                            df = df.append(dst)
                            if sys.getsizeof(df) >= DF_MEMORY_SIZE:
                                df.to_csv('./data/keiba' + str(designated_year) + str(csv_count) + '.csv', encoding='sjis')
                                logging.info('csv No.{} 出力完了'.format(csv_count))
                                df = pd.DataFrame(columns=DF_COL)
                                csv_count += 1

    df.to_csv('./data/keiba' + str(designated_year) + str(csv_count) + '.csv', encoding='sjis')
    logging.info('csv No.{} 出力完了'.format(csv_count))
    logging.info('プログラムが正常に終了しました')
