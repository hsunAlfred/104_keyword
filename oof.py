from typing import Generator
import requests
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import pandas as pd
import traceback
import logging.config
from datetime import datetime
import time
import re
from pathlib import Path
from random import randint


class OneOFour:
    def __init__(self):
        self.__rs = requests.session()
        self.__urlHome = "https://www.104.com.tw/"
        self.__urlSearch = "https://www.104.com.tw/jobs/search/"
        self.__userAgent = ""

        logging.config.fileConfig(f'logging.conf')
        self.__logger = logging.getLogger('timeRotateLogger')

        self.__new_dir = ""

    def search(self, kwd: str, maxPages: int = 3):
        try:
            self.__getCookies(kwd)
        except Exception as e:
            return self.__errLog(e)

        datas = self.__listPage(kwd, maxPages)

        detailSource = ([i[0], i[1], i[2], i[3]]for i in datas)

        return self.__detailPage(detailSource, len(datas))

    def __getCookies(self, kwd: str) -> None:
        '''get cookies'''
        self.__userAgent = UserAgent().random
        headers = {
            "user-agent": self.__userAgent
        }

        self.__rs.get(self.__urlHome, headers=headers)

    def __listPage(self, kwd: str, maxPages: int):
        pageDatas = []
        for i in range(maxPages):
            print(f'\r{kwd}, page:{i+1}', end='')
            try:
                pageContent = self.__loadPage(kwd, i+1)
            except Exception as e:
                return self.__errLog(e)

            try:
                pageData = self.__sourceExtractList(pageContent)
                pageDatas.extend(pageData)
            except Exception as e:
                return self.__errLog(e)

            time.sleep(1+randint(1, 10)/10)

        df = pd.DataFrame(pageDatas, columns=(
            "更新日期", "職缺連結", "職缺名稱", "公司名稱", "公司連結",
            "所屬產業", "所在區域", "工作經歷", "學歷要求", "職缺簡述"))

        fileTime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")

        self.__new_dir = Path(
            f"./104_{kwd}_first {maxPages} pages_{fileTime}").resolve()  # 取得絕對路徑
        if not self.__new_dir.exists():
            self.__new_dir.mkdir()

        df.to_excel(
            f'{self.__new_dir}/104_{kwd}_索引_first {maxPages} pages_{fileTime}.xlsx', index=False)

        return pageDatas

    def __detailPage(self, detailSource: Generator, n: int):
        i = 1
        print()
        for detail in detailSource:
            print(f'\rDetail {i:3}/{n:3}', end='')
            no = re.findall('.*/job/(.*)\?.*', detail[1])[0]
            page = self.__loadDetail(no)
            temp = '_'.join((detail[0], detail[2], detail[3])).replace(
                '/', '').replace('|', '')

            with open(f'{self.__new_dir}/{temp}.json', 'w', encoding='utf-8') as f:
                f.write(page)
            i += 1

            time.sleep(1+randint(1, 10)/10)
        return 'Done'

    def __loadPage(self, kwd: str, page: int) -> bytes:
        data = {
            "ro": 0,
            "kwop": 11,
            "keyword": kwd,
            "expansionType": "job",
            "order": 14,
            "asc": 0,
            "page": page,
            "mode": "s",
            "jobsource": "2018indexpoc",
            "langFlag": 0
        }

        referer = self.__urlSearch + "?"
        for k, v in data.items():
            referer += f"{k}={v}&"

        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Host": "www.104.com.tw",
            "Referer": referer[:-1].encode('utf-8'),
            "sec-ch-ua": '"Microsoft Edge";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "Windows",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": self.__userAgent,
        }

        temp = self.__rs.get(referer, headers=headers)
        # print(self.__rs.cookies)
        return temp.content

    def __loadDetail(self, no: str) -> bytes:
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Host": "www.104.com.tw",
            "Referer": f'https://www.104.com.tw/job/{no}?jobsource=hotjob_chr',
            "sec-ch-ua": '"Microsoft Edge";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "Windows",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": self.__userAgent,
        }

        temp = self.__rs.get(
            f"https://www.104.com.tw/job/ajax/content/{no}", headers=headers)

        return temp.text.encode('utf-8').decode("unicode_escape")

    def __sourceExtractList(self, pageContent: bytes) -> list:
        soup = BeautifulSoup(pageContent, 'html5lib')
        article = soup.find_all(
            "article", {"class": "b-block--top-bord job-list-item b-clearfix js-job-item"})

        pageData = []
        for i in range(len(article)):
            temp = article[i]
            # 更新日期、職缺連結、職缺名稱
            dateAndUrl = temp.select(".b-tit")
            update = dateAndUrl[0].select(
                ".b-tit__date")[0].text.replace(' ', '')
            joblink = dateAndUrl[0].find('a')['href'][2:]
            job = dateAndUrl[0].find('a').text

            # 公司名稱、公司連結、所屬產業
            temp = article[i].find_all('ul')
            company = temp[0].find_all('li')[1].text.replace(
                ' ', '').replace('\n', '')
            companyLink = temp[0].find_all('a')[0]['href'][2:]
            industry = temp[0].find_all(
                'li')[-1].text.replace(' ', '').replace('\n', '')

            # 所在區域、工作經歷、學歷要求
            area = temp[1].find_all('li')[0].text.replace(
                ' ', '').replace('\n', '')
            experience = temp[1].find_all('li')[1].text.replace(
                ' ', '').replace('\n', '')
            Education = temp[1].find_all('li')[2].text.replace(
                ' ', '').replace('\n', '')

            # 職缺描述
            temp = article[i].find_all('p')[0]
            jobDescribe = temp.text.replace('\n', '')

            # 更新日期、職缺連結、職缺名稱、公司名稱、公司連結、所屬產業、所在區域、工作經歷、學歷要求、職缺描述
            pageData.append([update, joblink, job, company, companyLink, industry,
                             area, experience, Education, jobDescribe])

        return pageData

    def __errLog(self, e):
        self.__logger.error(f'{str(e)}\n{traceback.format_exc()}')
        return f'{e}\nDetail: err.log'


if __name__ == '__main__':
    try:
        #kwd, maxPages = '軟體', "1"
        kwd, maxPages = sys.argv[1:]
    except Exception as e:
        print(e)
    else:
        if maxPages.isdigit():
            res = OneOFour().search(kwd, maxPages=int(maxPages))
            print(f'\n{res}')
        else:
            print("""在命令列使用python 104.py arg1(關鍵字) arg2(前n頁) arg3(easy or detail))
                    ex. python 104.py 軟體 3 easy
                    代表到104爬取關鍵字為軟體的前3頁
                """)
