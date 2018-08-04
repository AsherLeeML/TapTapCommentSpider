# -*- coding: utf-8 -*-
"""
Created on Sun Jul 29 18:38:08 2018

@author: Asher

Only crawl Andriod Games.



output: 
    - top30.csv:
        columns: {0:game_name|1:cate|2:score|3:company|4:tags|5:1-5 pop(split by';')|6:totalCommentNB|7:crawledNB|8:crawl_date|9:url}
        
        
    - game_name.csv:
        columns: {0:id | 1:time | 2:score | 3:comment(str) | 4: cellPhoneStamp | 5: proNB | 6: conNB}
"""

import requests
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pandas as pd
import pdb
import aiohttp
from pathlib import Path
import os
import logging
from tqdm import tqdm

# Configs
def dateParse(s):
    year, mon, day = s.split('-')
    return datetime(int(year), int(mon), int(day))
# savefile config
today = datetime.now().isoformat().split('T')[0]
saveFolder = Path('./results/')
fileName = 'taptapTop30_'+today+'.csv'

EARLIEST = dateParse(today) - timedelta(days = 180)


# log configs
log = logging.getLogger(__name__)
handlers = [logging.FileHandler(saveFolder/(today+'.log')), logging.StreamHandler()]
logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.INFO, handlers=handlers)

# url
topurl = 'https://www.taptap.com/top/played' # Android games only

MAXPAGE = int(2)




def crawlComment(URLS):
    logging.info('Run spider for each on "top".')
    
    cols = ['id', '时间' ,'打分', '评论内容', '使用机型', '支持人数', '反对人数']
    
    for url in URLS:
        IDs = []; DATEs = []; SCOREs = []; CONTENTs = []; CELLPHONEs = []
        SUPPORTPOP = []; AGAINSTPOP = []
        Vs = [IDs, DATEs, SCOREs, CONTENTs, CELLPHONEs, SUPPORTPOP, AGAINSTPOP]
        COMMENTS = {k:v for (k, v) in zip(cols, Vs)}
        # header
        #pdb.set_trace()
        
        req = requests.get(url)
        bf = BeautifulSoup(req.text, 'lxml')        
        gameName = bf.find('h1').text.strip()
        if '/' in gameName:
            gameName = gameName.replace('/', '-')
        commentFile = gameName+'.csv'
        if os.path.exists(saveFolder/commentFile):
            logging.info('This one already crawled.')
            continue
        logging.info('Start crawling ' + gameName)
        for sc in range(1, 6):
            post_fix1 = '/review?order=update&score=%d' % sc
            for pid in range(1, MAXPAGE+1):
                post_fix2 = '&page=%d#review-list' % pid
                commentUrl = url + post_fix1 + post_fix2
                #logging.info('Crawling page %d' % pid)
                bf = BeautifulSoup(requests.get(commentUrl).text, 'lxml')
                if bf.find('div', class_='taptap-review-none'):
                    #logging.info('No more comments: break.')
                    break
                body = bf.find_all('li', class_='taptap-review-item collapse in')
                for b in body:
                    singleComment = b.find('div', class_='item-text-body').text.strip()
                    if len(singleComment) < 15:
                        #logging.info('too short.')
                        continue
                    bid = b.get('data-user')
                    timeStamp = b.find('a', class_='text-header-time').text.strip().split('\n')[1]
                    dateI = dateParse(timeStamp.split(' ')[0])
                    if dateI <= EARLIEST:
                        #logging.info('Too early contents, stop crawling.')
                        break;
                    phoneType = b.find('span', class_='text-footer-device')
                    if phoneType is not None:
                        phoneType = phoneType.text
                    else:
                        phoneType = '未公开'
                    procount = b.find('button', 'btn btn-sm taptap-button-opinion vote-btn vote-up').find('span').text
                    concount = b.find('button', 'btn btn-sm taptap-button-opinion vote-btn vote-down').find('span').text
                    score = int(int(b.find('div', 'item-text-score').find('i').get('style').split(': ')[1][:2])/14)
                    if len(procount) > 0: procount = int(procount)
                    else: procount = 0
                    if len(concount) > 0: concount = int(concount)
                    else: concount = 0                
                    IDs.append(bid)
                    SCOREs.append(score)
                    CONTENTs.append(singleComment)
                    DATEs.append(dateI)
                    CELLPHONEs.append(phoneType)
                    SUPPORTPOP.append(procount)
                    AGAINSTPOP.append(concount)

        
        cpd = pd.DataFrame(COMMENTS)
        cpd = cpd[cols]
        cpd.to_csv(saveFolder/commentFile, header=True, index=False, encoding='utf-8')
        logging.info(gameName+' crawled and Saved.')
        

#    


def crawlTopInfoDict(url):
    
    if os.path.exists(saveFolder/fileName):
        TOPInfo = pd.read_csv(saveFolder/fileName, encoding='utf-8')
        TOPUrls = TOPInfo['TapTap链接']
        logging.info("Today's data already there.")
        return TOPUrls
    req = requests.get(url=url)
    html = req.text
    bf = BeautifulSoup(html, 'lxml')
    top30 = bf.find_all('div', class_='taptap-top-card')
    TOPNames = []
    TOPCompanies = []
    TOPCates = []
    TOPScores = []
    TOPUrls = []
    TOPTags = []
    TOPDes = []
    TOPInfos = [TOPNames, TOPCompanies, TOPDes, TOPCates, TOPScores, TOPTags, TOPUrls]
    cols = ['游戏名', '研发/运营', '简介', '类型', '总评分', '标签', 'TapTap链接']
    
    logging.info('Run spider for tops.')
    for top in top30:
        top = BeautifulSoup(str(top), 'lxml')
        topInfo = top.find('div', class_='top-card-middle')
        # 0: Name
        topName = topInfo.find('h4').text
        TOPNames.append(topName)
        topfoot = topInfo.find('div', class_='card-middle-footer')
        # 1: cate
        topCate = topfoot.find('a').text
        TOPCates.append(topCate)
        # 2: score
        topScore = float(topfoot.find('span').text)
        TOPScores.append(topScore)
        # plus: description
        topDescription = topInfo.find('p', class_='card-middle-description').text.strip()
        TOPDes.append(topDescription)
        # 3: company
        topCompany = topInfo.find('p', class_='card-middle-author').text.strip()
        TOPCompanies.append(topCompany)
        # 8: url
        topUrl = topInfo.find('a', class_='card-middle-title').get('href')
        TOPUrls.append(topUrl)
        # 4:tags
        topTags = [item.text for item in topInfo.find_all('a', class_='btn btn-xs btn-default ')]
        topTags = ",".join(topTags)
        TOPTags.append(topTags)
        
    TOPInfo = {k:v for (k, v) in zip(cols, TOPInfos)}
    TOPInfo = pd.DataFrame(TOPInfo)
    TOPInfo = TOPInfo[cols]
    TOPInfo.to_csv(saveFolder/fileName, header=True, index=False, encoding='utf-8')
    logging.info('Top crawled and Saved.')
    return TOPUrls
    
    
    
#def crawlComments(urlin):
    

if __name__ == '__main__':
    if not os.path.exists(saveFolder):
        os.system('mkdir '+saveFolder)
    urls = crawlTopInfoDict(topurl)
    #print(urls)
    crawlComment(urls)