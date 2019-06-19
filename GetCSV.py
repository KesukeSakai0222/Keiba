import requests
from tqdm import tqdm
import time
from bs4 import BeautifulSoup
import pandas as pd
import os
import sys
import logging


def numStr(num:int):
    '''
    数字を0埋めして2桁で返す
    '''
    return str(num).zfill(2)


logging.basicConfig(filename='logfile/logger.log', level=logging.INFO)

Base = "http://race.sp.netkeiba.com/?pid=race_result&race_id="
dst = ''
df_col = ['year', 'date', 'field', 'race', 'race_name'
          , 'course', 'head_count', 'rank', 'horse_name'
          , 'gender', 'age', 'trainerA', 'trainerB', 'weight', 'c_weight', 'jackie', 'j_weight'
          , 'odds','popu']

if not os.path.exists('./data'):
    os.mkdir('./data')

designated_year = 2018

for year in range(designated_year, designated_year+1):
    for i in tqdm(range(1, 11)):
        df = pd.DataFrame()
        logging.info('DataFrameの作成 完了')
        for j in range(1, 11):
            logging.info("j = {}".format(j))
            for k in range(1, 11):
                for l in range(1, 13):
                    # urlでぶっこ抜く
                    url = Base + str(year) + numStr(i) + numStr(j) + numStr(k) + numStr(l)
                    html = requests.get(url)
                    logging.info('html取得')
                    html.encoding = 'EUC-JP'
                    
                    # scraping
                    soup = BeautifulSoup(html.text, 'html.parser')
                    logging.info('parser完了')
                    # ページがあるかの判定
                    if soup.find_all('div', attrs={'class', 'Result_Guide'})!=[]:
                        logging.info('ページ無し')
                        time.sleep(1)
                        break
                    else:
                        logging.info('ページ有り')
                        #共通部分を抜き出す
                        CommonYear = year
                        CommonDate = soup.find_all('div', attrs={'class', 'Change_Btn Day'})[0].string.strip()
                        CommonField= soup.find_all('div', attrs={'class', 'Change_Btn Course'})[0].string.strip()
                        CommonRace = soup.find_all('div', attrs={'Race_Num'})[0].span.string
                        CommonRname= soup.find_all('dt', attrs={'class', 'Race_Name'})[0].contents[0].strip()
                        CommonCourse= soup.find_all('dd', attrs={'Race_Data'})[0].span.string
                        CommonHcount= soup.find_all('dd', attrs={'class', 'Race_Data'})[0].contents[3].split()[1]
                        
                        logging.info('共通部分抜き出し 完了')
                        for m in range(len(soup.find_all('div', attrs='Rank'))):
                            dst = pd.Series(index = df_col)
                            try:
                                dst['year'] = CommonYear
                                dst['date'] = CommonDate
                                dst['field']= CommonField
                                dst['race'] = CommonRace
                                dst['race_name'] = CommonRname
                                dst['course'] = CommonCourse
                                dst['head_count'] = CommonHcount
                                dst['rank'] = soup.find_all('div', attrs='Rank')[m].contents[0]
                                dst['horse_name'] = soup.find_all('dt', attrs=['class', 'Horse_Name'])[m].a.string
                                detailL = soup.find_all('span', attrs=['class', 'Detail_Left'])[m]
                                dst['gender'] = list(detailL.contents[0].split()[0])[0]
                                dst['age'] = list(detailL.contents[0].split()[0])[1]
                                dst['trainerA'] = detailL.span.string.split('･')[0]
                                dst['trainerB'] = detailL.span.string.split('･')[1]
                                if len(detailL.contents[0].split())>=2:
                                    dst['weight'] = detailL.contents[0].split()[1].split('(')[0]
                                    if len(detailL.contents[0].split()[1].split('('))>=2:
                                        dst['c_weight'] = detailL.contents[0].split()[1].split('(')[1].strip(')')
                                detailR = soup.find_all('span', attrs=['class', 'Detail_Right'])[m].contents
                                if  "\n" in detailR or "\n▲" in detailR or '\n☆' in detailR:
                                    detailR.pop(0)
                                dst['jackie'] = detailR[0].string.strip()
                                dst['j_weight'] = detailR[2].strip().replace('(', '').replace(')', '')
                                Odds = soup.find_all('td', attrs=['class', 'Odds'])[m].contents[1]
                                if Odds.dt.string is not None:
                                    dst['odds'] = Odds.dt.string.strip('倍')
                                    dst['popu'] = Odds.dd.string.strip('人気')
                            
                                dst.name = str(year) + numStr(i) + numStr(j) + numStr(k) + numStr(l) + numStr(m)
                                df = df.append(dst)
                            except:
                                pass
                            
        df.to_csv('./data/keiba'+ str(year) + str(i) + '.csv', encoding='shift-jis')
        logging.info('csv 出力完了')
