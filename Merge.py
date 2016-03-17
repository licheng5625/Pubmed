import glob
import xml.etree.cElementTree as ET
import gzip
import os
import time
import sqlite3
import sys

workpath='./../analysis/data/'
workpath=''
if len(sys.argv) == 1:
    DatabaseBase='PubmedData2014_11_26_0.db'
    DatabaseMerge='PubmedData2014_11_26_2.db'

else:
    DatabaseBase = sys.argv[1]
    DatabaseMerge = sys.argv[2]

 #workpath=''
#filepath=''
connBase = sqlite3.connect(workpath+DatabaseBase)
connMerge=sqlite3.connect(workpath+DatabaseMerge)


curBase = connBase.cursor()
curMerge = connMerge.cursor()

connBase.execute('''CREATE TABLE IF NOT EXISTS AUTHORS
       (ID INTEGER PRIMARY KEY     ,
       Fullname      TEXT    NOT NULL,
       Lastname      TEXT    ,
       Forname       TEXT     ,
       Initials    TEXT     ,
       Suffix        TEXT    );''')

connBase.execute('''CREATE TABLE IF NOT EXISTS AUTHORS_DESCRIPTIONS
       ( Authors_ID  INT NOT NULL,
         Affiliation   TEXT    NOT NULL ,
       FOREIGN KEY (Authors_ID)  REFERENCES AUTHORS(ID)  ,
       PRIMARY KEY (Authors_ID,Affiliation));''')


connBase.execute('''CREATE TABLE IF NOT EXISTS ARTICLES
       (PmID            INT PRIMARY KEY     NOT NULL,
       Title           TEXT    ,
       CreatDate        Date);''')

connBase.execute('''CREATE TABLE IF NOT EXISTS ARTICLES_TERMS
       ( Article_ID   INT    NOT NULL,
         DescriptorName       TEXT    NOT NULL ,
         QualifierName        TEXT ,
       FOREIGN KEY (Article_ID)  REFERENCES ARTICLES(PmID));''')

connBase.execute('''CREATE TABLE IF NOT EXISTS ARTICLES_WRITERS
       ( Article_ID  INT    NOT NULL,
         Authors_ID   INT    NOT NULL ,
       FOREIGN KEY (Article_ID)  REFERENCES ARTICLES(PmID)  ,
       FOREIGN KEY (Authors_ID)  REFERENCES AUTHORS(ID) ,
       PRIMARY KEY (Article_ID,Authors_ID));''')

connBase.execute('''CREATE TABLE IF NOT EXISTS JOURNALS
       (ID INTEGER  PRIMARY KEY     ,
       ISSN TEXT,
       Title TEXT);''')

connBase.execute('''CREATE TABLE IF NOT EXISTS ARTICLES_PUBLISHER
       (Article_ID   INT    NOT NULL,
       Journal_ID    INT NOT NULL,
       FOREIGN KEY (Journal_ID) REFERENCES JOURNALS(ID),
       FOREIGN KEY (Article_ID)  REFERENCES ARTICLES(PmID)  ,
       PRIMARY KEY (Article_ID,Journal_ID));''')

BasePmid= set(curBase.execute("select PmID from ARTICLES ").fetchall())
MergPmid=set(curMerge.execute("select PmID from ARTICLES ").fetchall())

if len(BasePmid & MergPmid) is not None:
    print 'error'
    exit()
#exit()

MergArticlset=set(curMerge.execute("select * from ARTICLES ").fetchall())
for newArticle in MergArticlset:
    curBase.execute("insert into ARTICLES (PmID,Title,CreatDate) values('%s','%s','%s')" %(newArticle[0],newArticle[1].replace('\'','\'\''),newArticle[2]))
MergTermsset=set(curMerge.execute("select * from ARTICLES_TERMS where QualifierName is not null").fetchall())
for term in MergTermsset:
    curBase.execute("insert into ARTICLES_TERMS (Article_ID,DescriptorName,QualifierName) values('%s','%s','%s')" %(term[0],term[1].replace('\'','\'\''),term[2].replace('\'','\'\'')))
MergTermsset=set(curMerge.execute("select * from ARTICLES_TERMS where QualifierName is  null").fetchall())
for term in MergTermsset:
    curBase.execute("insert into ARTICLES_TERMS (Article_ID,DescriptorName,QualifierName) values('%s','%s',null)" %(term[0],term[1].replace('\'','\'\'')))

jounalBaseset=(curBase.execute("select Title from JOURNALS ").fetchall())
jounalMergeset=(curMerge.execute("select ISSN ,Title from JOURNALS ").fetchall())

for journal in jounalMergeset:
    if (journal[1],) not in jounalBaseset:
        if journal[0] is '' or  journal[0] is None:
             curBase.execute("insert into JOURNALS (ID,Title,ISSN) values(null,'%s',null) "%(journal[1].replace('\'','\'\'')))

        else:
            curBase.execute("insert into JOURNALS (ID,Title,ISSN) values(null,'%s','%s') "%(journal[1].replace('\'','\'\''),journal[0]))

curBase.execute("update  JOURNALS SET ISSN = null where ISSN='' or  ISSN='None'").fetchall()


mapjour=dict()
for pub in MergPmid:

    jorName=curMerge.execute("select Title from ARTICLES_PUBLISHER inner join JOURNALS on Journal_ID =ID where Article_ID='%s'"%(pub[0])).fetchone()[0]
    if jorName  not in set(mapjour.keys()):
        newjorID=curBase.execute("select ID from JOURNALS  where Title='%s'"%(jorName.replace('\'','\'\''))).fetchone()[0]
        mapjour[jorName]=newjorID
    else:
        newjorID=mapjour[jorName]

    curBase.execute("insert into ARTICLES_PUBLISHER (Article_ID,Journal_ID) values('%s','%s') "%(pub[0],newjorID))


AuthorBaseset=(curBase.execute("select Fullname from AUTHORS ").fetchall())
AuthorMergeset=(curMerge.execute("select  Fullname ,Lastname , Forname,Initials,Suffix from AUTHORS ").fetchall())
for art in AuthorMergeset:
    curBase.execute("insert into AUTHORS (ID,Fullname ,Lastname , Forname,Initials,Suffix) values(null,'%s','%s','%s','%s','%s') "%(art[0].replace('\'','\'\''),art[1].replace('\'','\'\''),art[2].replace('\'','\'\''),art[3].replace('\'','\'\''),art[4].replace('\'','\'\'')))

curBase.execute("update  AUTHORS SET Lastname = null where Lastname='' or  Lastname='None'").fetchall()
curBase.execute("update  AUTHORS SET Forname = null where Forname='' or  Forname='None'").fetchall()
curBase.execute("update  AUTHORS SET Initials = null where Initials='' or  Initials='None'").fetchall()
curBase.execute("update  AUTHORS SET Suffix = null where Suffix='' or  Suffix='None'").fetchall()

Authordict=dict()
AuthorBaseset=(curBase.execute("select ID,Fullname from AUTHORS ").fetchall())
for au in   AuthorBaseset:
    Authordict[au[1]]   =au[0]
DesMergeset=(curMerge.execute("select  Fullname ,Affiliation from AUTHORS_DESCRIPTIONS inner join AUTHORS on Authors_ID = ID ").fetchall())
for des in DesMergeset:
    auid=  Authordict[des[0]]
    try:
        curBase.execute("insert into AUTHORS_DESCRIPTIONS (Authors_ID,Affiliation) values('%s','%s')" %(auid,des[1].replace('\'','\'\'')))
    except sqlite3.IntegrityError:
        pass

wriMergeset=(curMerge.execute("select Fullname,Article_ID from ARTICLES_WRITERS inner join AUTHORS on Authors_ID = ID ").fetchall())
for wir in wriMergeset:
    wrid=  Authordict[wir[0]]
    curBase.execute("insert into ARTICLES_WRITERS (Authors_ID,Article_ID) values('%s','%s')" %(wrid,wir[1]))
