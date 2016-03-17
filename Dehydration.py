import glob
import json
import threading
import xml.etree.cElementTree as ET
import gzip
import os
from subprocess import check_call

import time

articlesset=set()

seeinglist=set()

workedlist=list()

changeAuthorList=list()

workpath='./../analysis/data/'
filepath='./../raw_data/pubmed/'
outpath='./../raw_data/pubmed/Dehydration/'
#workpath=''
#filepath=''
#outpath='./Dehydration/'


def redunceticle(path):
    global workedlist
    print 'working on %s'%path
    buffer=u''
    articlesfiles= gzip.open( path, 'r')
    offset=len(filepath)
    for line in articlesfiles:
        numbyline=0
        XmlData=line.decode('utf8').encode('utf8')
        xmltree = ET.fromstring(XmlData)
        for article in xmltree.findall(".//PubmedArticle"):
            pmid = article.find('.//PMID').text
            if pmid not in articlesset:
               articlesset.add(pmid)
               numbyline+=1
            else:
                xmltree.remove(article)
        if numbyline is not 0:
            Etstring=ET.tostring(xmltree).encode('utf-8')
            try:
                buffer+=(Etstring.replace('\n','')+'\n')
            except MemoryError:
                 with open(outpath+path[offset:-3]+'Dehydration.txt.temp','a')as readlist:
                    readlist.writelines(buffer)
                    buffer=u''
                    buffer+=(Etstring.replace('\n','')+'\n')
    articlesfiles.close()
    with open(outpath+path[offset:-3]+'Dehydration.txt.temp','a')as readlist:
        readlist.writelines(buffer)

    #os.rename(outpath+path[offset:-3]+'Dehydration.txt.temp',outpath+path[offset:-3]+'Dehydration.txt')
    g = gzip.GzipFile(filename=outpath+path[offset:-3]+'Dehydration.txt', mode="wb", compresslevel=9, fileobj=open(outpath+path[offset:-3]+'Dehydration.txt.gz', 'wb'))
    g.write(open(outpath+path[offset:-3]+'Dehydration.txt.temp').read())
    g.close()
    os.remove(outpath+path[offset:-3]+'Dehydration.txt.temp')
    workedlist.append(path)
    with open(workpath+'articlesset.txt','w') as readlist2:
        for ar in articlesset:
            readlist2.writelines(ar+'\n')
    with open(workpath+'Dehydrationfile.txt','a') as readlist23:
        readlist23.writelines(path+'\n')


if os.path.isfile(workpath +'Dehydrationfile.txt'):
    with open(workpath + 'Dehydrationfile.txt', 'r') as readlist:
        for line in readlist:
            workedlist.append(line.replace('\n', '').decode('utf8'))

if os.path.isfile(workpath +'articlesset.txt'):
    with open(workpath + 'articlesset.txt', 'r') as readlist:
        for line in readlist:
            articlesset.add(line.replace('\n', '').decode('utf8'))

for path in glob.glob( filepath+ '*.gz'):
    if path not in workedlist and path.find('Dehydration.gz') is -1:
        redunceticle(path)





