import glob
import random
import threading
import xml.etree.cElementTree as ET
import gzip
import os
import time
import sqlite3
#os.remove('PubmedData.db')
import sys

workpath='./../analysis/data/'
filepath='./../raw_data/pubmed/'
#workpath=''
#filepath=''

if len(sys.argv) == 1:
        filesname=''
else:
    filesname = sys.argv[1]

connINIT = sqlite3.connect(workpath+'PubmedData'+filesname+'.db')
connINIT.execute('''CREATE TABLE IF NOT EXISTS AUTHORS
       (ID INTEGER PRIMARY KEY     ,
       Fullname      TEXT    NOT NULL UNIQUE,
       Lastname      TEXT    ,
       Forname       TEXT     ,
       Initials    TEXT     ,
       Suffix        TEXT    );''')

connINIT.execute('''CREATE TABLE IF NOT EXISTS AUTHORS_DESCRIPTIONS
       ( Authors_ID  INT NOT NULL,
         Affiliation   TEXT    NOT NULL ,
       FOREIGN KEY (Authors_ID)  REFERENCES AUTHORS(ID)  ,
       PRIMARY KEY (Authors_ID,Affiliation));''')


connINIT.execute('''CREATE TABLE IF NOT EXISTS ARTICLES
       (PmID            INT PRIMARY KEY     NOT NULL,
       Title           TEXT   UNIQUE ,
       CreatDate        Date);''')

connINIT.execute('''CREATE TABLE IF NOT EXISTS ARTICLES_TERMS
       ( Article_ID   INT    NOT NULL,
         DescriptorName       TEXT    NOT NULL ,
         QualifierName        TEXT ,
       FOREIGN KEY (Article_ID)  REFERENCES ARTICLES(PmID));''')

connINIT.execute('''CREATE TABLE IF NOT EXISTS ARTICLES_WRITERS
       ( Article_ID  INT    NOT NULL,
         Authors_ID   INT    NOT NULL ,
       FOREIGN KEY (Article_ID)  REFERENCES ARTICLES(PmID)  ,
       FOREIGN KEY (Authors_ID)  REFERENCES AUTHORS(ID) ,
       PRIMARY KEY (Article_ID,Authors_ID));''')

connINIT.execute('''CREATE TABLE IF NOT EXISTS JOURNALS
       (ID INTEGER  PRIMARY KEY     ,
       ISSN TEXT,
       Title TEXT UNIQUE);''')

connINIT.execute('''CREATE TABLE IF NOT EXISTS ARTICLES_PUBLISHER
       (Article_ID   INT    NOT NULL,
       Journal_ID    INT NOT NULL,
       FOREIGN KEY (Journal_ID) REFERENCES JOURNALS(ID),
       FOREIGN KEY (Article_ID)  REFERENCES ARTICLES(PmID)  ,
       PRIMARY KEY (Article_ID,Journal_ID));''')

connINIT.close()

def gettimestamp():
    format_type = u'%Y_%m_%d_%H_%M_%S'
    return time.strftime(format_type, time.localtime(time.time()))

def insertAticle(cur,pmid,myarticletitle,creatdate):
    sucess=True
    while sucess:
        try:
            cur.execute("insert into ARTICLES (PmID,Title,CreatDate) values('%s','%s','%s')" %(pmid,myarticletitle,creatdate))
            sucess=False
        except sqlite3.OperationalError:
            time.sleep(random.uniform(1, 2) )

def insertAticleTerm(cur,pmid,DescriptorName,QualifierNames):

    if len(QualifierNames) is 0:
        sucess=True
        while sucess:
            try:
                cur.execute("insert into ARTICLES_TERMS (Article_ID,DescriptorName) values('%s','%s')"%(pmid,DescriptorName))
                sucess=False
            except sqlite3.OperationalError:
                time.sleep(random.uniform(1, 2) )
    else:
        for subterm in QualifierNames:
            sucess=True
            while sucess:
                try:
                    cur.execute("insert into ARTICLES_TERMS (Article_ID,DescriptorName,QualifierName) values('%s','%s','%s')"%(pmid,DescriptorName,subterm.text.replace('\'',"\'\'")))
                    sucess=False
                except sqlite3.OperationalError:
                    time.sleep(random.uniform(1, 2) )

def insertAuthor(cur,fullname,lastname,forename,initials,suffix):
    sucess=True
    while sucess:
        try:
            cur.execute("insert into AUTHORS (ID,Fullname,Lastname,Forname,Initials,Suffix) values(null,'%s','%s','%s','%s','%s')" %(fullname,lastname,forename,initials,suffix))
            sucess=False
        except sqlite3.OperationalError:
            time.sleep(random.uniform(1, 2) )

def insertDes(cur,AutorID,descriptionID,description):
    if descriptionID is None:
        sucess=True
        while sucess:
            try:
                cur.execute("insert into AUTHORS_DESCRIPTIONS (Authors_ID,Affiliation) values('%s','%s')" %(AutorID,description))
                sucess=False
            except sqlite3.OperationalError:
                time.sleep(random.uniform(1, 2) )

def insertWriter(cur,AutorID,pmid):
    sucess=True
    while sucess:
        try:
            cur.execute("insert into ARTICLES_WRITERS (Article_ID,Authors_ID) values('%s','%s')" %(pmid,AutorID))
            sucess=False
        except sqlite3.OperationalError:
            time.sleep(random.uniform(1, 2) )

def insertJonral(cur,journalTitle,journalISSN):
    sucess=True
    while sucess:
        try:
            cur.execute("insert into JOURNALS (ID,Title,ISSN) values(null,'%s','%s') "%(journalTitle,journalISSN))
            sucess=False
        except sqlite3.OperationalError:
            time.sleep(random.uniform(1, 2) )


def inserPublish(cur,pmid,journalID):
    sucess=True
    while sucess:
        try:
            cur.execute("insert into ARTICLES_PUBLISHER (Article_ID,Journal_ID) values('%s','%s')" %(pmid,journalID))
            sucess=False
        except sqlite3.OperationalError:
            time.sleep(random.uniform(1, 2) )

def selectfromDB(cur,query):
    sucess=True
    while sucess:
        try:
            cur.execute(query)
            sucess=False
        except sqlite3.OperationalError:
            time.sleep(random.uniform(1, 2) )
seeingpmidlist=set()
pmidlock=threading.Lock()
def getarticle(XmlData,conn):

    cur = conn.cursor()
    xmltree = ET.fromstring(XmlData)
    for article in xmltree.findall(".//PubmedArticle"):
        pmid = article.find('.//PMID').text
        pmidlock.acquire()
        if pmid not in seeingpmidlist and cur.execute("select PmID from ARTICLES where PmID = '%s'"%(pmid)).fetchone() is None :
            seeingpmidlist.add(pmid)
            pmidlock.release()
            #print pmid
            titleofarticle=article.find(u'.//ArticleTitle')
            if titleofarticle is not None and titleofarticle.text is not None:
                myarticletitle=(titleofarticle.text.encode('utf-8').decode('utf-8')).replace('\'',"\'\'")
            else:
                myarticletitle=((article.find(u'.//VernacularTitle').text).encode('utf-8').decode('utf-8')).replace('\'',"\'\'")
            ArticleDate=article.find(u'.//DateCreated')
            creatdate = ArticleDate.find('Year').text.encode('utf-8').decode('utf-8')+'-'+(ArticleDate.find('Month').text).encode('utf-8').decode('utf-8')+'-'+(ArticleDate.find('Day').text).encode('utf-8').decode('utf-8')
            insertAticle(cur,pmid,myarticletitle,creatdate)
            for terms in article.findall(u".//MeshHeading"):
                DescriptorName=(terms.find('DescriptorName').text).encode('utf-8').decode('utf-8').replace('\'',"\'\'")
                QualifierNames=terms.findall(u'QualifierName')
                insertAticleTerm(cur,pmid,DescriptorName,QualifierNames)

            journal =article.find(u'.//Journal')
            journalTitle=journal.find(u'Title').text.replace('\'',"\'\'")
            jounalid=cur.execute("select ID from JOURNALS where Title='%s'"%(journalTitle)).fetchone()
            if jounalid is None:
                ISSNs=journal.find('ISSN')
                if ISSNs is not None and ISSNs.text is not None:
                    journalISSN=ISSNs.text.encode('utf-8').decode('utf-8')
                else:
                    journalISSN=''
                insertJonral(cur,journalTitle,journalISSN)
                jounalid=cur.execute("select ID from JOURNALS where Title='%s'"%(journalTitle)).fetchone()
            inserPublish(cur,pmid,jounalid[0])

            for myAuthor in article.findall(u".//Author"):
                LastName=myAuthor.find(u'LastName')
                if LastName is not None and LastName.text is not None:
                    lastname = (LastName.text).encode('utf-8').decode('utf-8').replace('\'',"\'\'")
                else:
                    lastname = u''
                Forename=myAuthor.find(u'ForeName')
                if Forename is not None and Forename.text is not None:
                    forename = (Forename.text).encode('utf-8').decode('utf-8').replace('\'',"\'\'")
                else:
                    forename = u''
                Au=myAuthor.find(u'Initials')
                if Au is not None and Au.text is not None:
                    initials = (Au.text).encode('utf-8').decode('utf-8').replace('\'',"\'\'")
                else:
                    initials = u''
                Su=myAuthor.find(u'Suffix')
                if Su is not None and Su.text is not None:
                    suffix = (Su.text).encode('utf-8').decode('utf-8')
                else:
                    suffix = u''
                if lastname is u'' and forename is u'':
                    continue
                if suffix is u'':
                    fullname = (lastname + ', ' + forename )
                else:
                    fullname = (lastname + ', ' + forename + ' ' + suffix)
                authorID=cur.execute("select ID from AUTHORS where Fullname = '%s'"%(fullname)).fetchone()
                if authorID is None:
                    insertAuthor(cur,fullname,lastname,forename,initials,suffix)
                    authorID=cur.execute("select ID from AUTHORS where Fullname = '%s'"%(fullname)).fetchone()
                Aff=myAuthor.find(u'Affiliation')
                if Aff is not None and Aff.text is not None:
                    description= Aff.text.encode('utf-8').decode('utf-8').replace('\'',"\'\'")
                    descriptionID=cur.execute("select Authors_ID from AUTHORS_DESCRIPTIONS where Authors_ID='%s' and Affiliation='%s'"%(authorID[0],description)).fetchone()
                    insertDes(cur,authorID[0],descriptionID,description)
                if cur.execute("select Authors_ID from ARTICLES_WRITERS where Authors_ID='%s' and Article_ID='%s'"%(authorID[0],pmid)).fetchone() is None:
                    insertWriter(cur,authorID[0],pmid)
            conn.commit()
        else:
            pmidlock.release()




class MyThread(threading.Thread):

    def __init__(self,path):

        threading.Thread.__init__(self)
        self.path=path
        #self.start =False
    def run(self):
        print ('work on'+path)

        conn = sqlite3.connect(workpath+'PubmedData'+filesname+'.db')

        articlesfiles= gzip.open( self.path, 'r')
        for line in articlesfiles:
            getarticle(line.decode('utf8').encode('utf8'),conn)
        sucess=True
        while sucess:
            try:
                conn.commit()
                sucess=False
            except:
                time.sleep(10)
                scuess=True

        articlesfiles.close()

        with open(workpath+'readlist'+filesname+'.txt','a') as readlist:
            readlist.writelines(self.path+'\n')
        workedlist.append(self.path)

seeinglist=set()
seelock=threading.Lock()
threadlist=set()
workedlist=list()
if os.path.isfile(workpath + 'readlist'+filesname+'.txt'):
    print('open '+'readlist'+filesname+'.txt')
    with open(workpath + 'readlist'+filesname+'.txt', 'r') as readlist:
            for line in readlist:
                workedlist.append(line.replace('\n', '').decode('utf8'))

for path in glob.glob( filepath+'*'+filesname+ '*.gz'):
    seelock.acquire()
    if path not in workedlist and path not in seeinglist:
        seeinglist.add(path)
        seelock.release()
        thread=MyThread(path)
        thread.setDaemon(True)
        stop=True
        while stop:
            num =1
            for th in threadlist:
                if th.isAlive():
                    num+=1
            if num <= 10:
                thread.start()
                threadlist.add(thread)
                stop =False
            else:
                time.sleep(10)



        seeinglist.remove(path)
    else:
        seelock.release()
stop=True
while stop:
    time.sleep(100)
    stop =False
    for th in threadlist:
        if th.isAlive():
            stop=True
            break
