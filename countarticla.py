import glob
import json
import xml.etree.cElementTree as ET
import gzip
import os
import time

articlesset=set()



workpath='./../analysis/data/'
filepath='./../raw_data/pubmed/'
workpath=''
filepath=''



def getarticle(XmlData):
    articl=0
    articlunique=0
    xmltree = ET.fromstring(XmlData)
    for article in xmltree.findall(".//PubmedArticle"):
        pmid = article.find('.//PMID').text
        articl+=1
        if pmid not in articlesset:
            articlesset.add(pmid)
            articlunique+=1
    return [articl,articlunique]



workedlist=list()

num=[0,0]

for path in glob.glob( filepath+ 'reduntion*txt'):
    print 'work on'+path
    if path not in workedlist:
        articlesfiles= open( path, 'r')
        i =0
        for line in articlesfiles:
            i+=1
            result=getarticle(line.decode('utf8').encode('utf8'))
            num[0]+=result[0]
            num[1]+=result[1]
            print(num)
        articlesfiles.close()
print num

