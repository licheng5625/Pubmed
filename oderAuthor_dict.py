import glob
import xml.etree.cElementTree as ET
import gzip
import os
import time
import sqlite3

workpath='./../analysis/data/'
filepath='./../raw_data/pubmed/'
workpath=''
filepath=''
conn = sqlite3.connect(workpath+'PubmedData.db')
conn.execute('''CREATE TABLE IF NOT EXISTS AUTHORS
       (ID INTEGER PRIMARY KEY     ,
       Fullname      TEXT    NOT NULL,
       Lastname      TEXT    ,
       Forname       TEXT     ,
       Initials    TEXT     ,
       Suffix        TEXT    );''')

conn.execute('''CREATE TABLE IF NOT EXISTS AUTHORS_DESCRIPTIONS
       ( Authors_ID  INT NOT NULL,
         Affiliation   TEXT    NOT NULL ,
       FOREIGN KEY (Authors_ID)  REFERENCES AUTHORS(ID)  ,
       PRIMARY KEY (Authors_ID,Affiliation));''')


conn.execute('''CREATE TABLE IF NOT EXISTS ARTICLES
       (PmID            INT PRIMARY KEY     NOT NULL,
       Title           TEXT    ,
       CreatDate        Date);''')

conn.execute('''CREATE TABLE IF NOT EXISTS ARTICLES_TERMS
       ( Article_ID   INT    NOT NULL,
         DescriptorName       TEXT    NOT NULL ,
         QualifierName        TEXT ,
       FOREIGN KEY (Article_ID)  REFERENCES ARTICLES(PmID));''')

conn.execute('''CREATE TABLE IF NOT EXISTS ARTICLES_WRITERS
       ( Article_ID  INT    NOT NULL,
         Authors_ID   INT    NOT NULL ,
       FOREIGN KEY (Article_ID)  REFERENCES ARTICLES(PmID)  ,
       FOREIGN KEY (Authors_ID)  REFERENCES AUTHORS(ID) ,
       PRIMARY KEY (Article_ID,Authors_ID));''')

conn.execute('''CREATE TABLE IF NOT EXISTS JOURNALS
       (ID INTEGER  PRIMARY KEY     ,
       ISSN TEXT,
       Title TEXT);''')

conn.execute('''CREATE TABLE IF NOT EXISTS ARTICLES_PUBLISHER
       (Article_ID   INT    NOT NULL,
       Journal_ID    INT NOT NULL,
       FOREIGN KEY (Journal_ID) REFERENCES JOURNALS(ID),
       FOREIGN KEY (Article_ID)  REFERENCES ARTICLES(PmID)  ,
       PRIMARY KEY (Article_ID,Journal_ID));''')



def gettimestamp():
    format_type = u'%Y_%m_%d_%H_%M_%S'
    return time.strftime(format_type, time.localtime(time.time()))


authorsset=set(conn.execute("select ID,Fullname from AUTHORS").fetchall())
Authorsdict=dict()
for au in authorsset:
    Authorsdict[au[1]]=au[0]


def getarticle(XmlData):
    cur = conn.cursor()
    xmltree = ET.fromstring(XmlData)
    for article in xmltree.findall(".//PubmedArticle"):
        pmid = article.find('.//PMID').text
        if cur.execute("select PmID from ARTICLES where PmID = '%s'"%(pmid)).fetchone() is None:
            #print pmid
            if article.find(u'.//ArticleTitle') is not None and article.find(u'.//ArticleTitle').text is not None:
                myarticletitle=((article.find(u'.//ArticleTitle').text).encode('utf-8').decode('utf-8')).replace('\'',"\'\'")
            else:
                myarticletitle=((article.find(u'.//VernacularTitle').text).encode('utf-8').decode('utf-8')).replace('\'',"\'\'")
            ArticleDate=article.find(u'.//DateCreated')
            creatdate = ArticleDate.find('Year').text.encode('utf-8').decode('utf-8')+'-'+(ArticleDate.find('Month').text).encode('utf-8').decode('utf-8')+'-'+(ArticleDate.find('Day').text).encode('utf-8').decode('utf-8')
            cur.execute("insert into ARTICLES (PmID,Title,CreatDate) values('%s','%s','%s')" %(pmid,myarticletitle,creatdate))
            for terms in article.findall(u".//MeshHeading"):
                DescriptorName=(terms.find('DescriptorName').text).encode('utf-8').decode('utf-8').replace('\'',"\'\'")
                QualifierNames=terms.findall(u'QualifierName')
                if len(QualifierNames) is 0:
                    cur.execute("insert into ARTICLES_TERMS (Article_ID,DescriptorName) values('%s','%s')"%(pmid,DescriptorName))
                else:
                    for subterm in QualifierNames:
                        cur.execute("insert into ARTICLES_TERMS (Article_ID,DescriptorName,QualifierName) values('%s','%s','%s')"%(pmid,DescriptorName,subterm.text.replace('\'',"\'\'")))

            journal =article.find(u'.//Journal')
            journalTitle=journal.find(u'Title').text.replace('\'',"\'\'")
            jounalid=cur.execute("select ID from JOURNALS where Title='%s'"%(journalTitle)).fetchone()
            if jounalid is None:
                issnelement=journal.find('ISSN')
                if issnelement is not None and issnelement.text is not None:
                    journalISSN=issnelement.text.encode('utf-8').decode('utf-8')
                    cur.execute("insert into JOURNALS (ID,Title,ISSN) values(null,'%s','%s') "%(journalTitle,journalISSN))
                else:
                    cur.execute("insert into JOURNALS (ID,Title,ISSN) values(null,'%s',null) "%(journalTitle))
                jounalid=cur.execute("select ID from JOURNALS where Title='%s'"%(journalTitle)).fetchone()

            cur.execute("insert into ARTICLES_PUBLISHER (Article_ID,Journal_ID) values('%s','%s')" %(pmid,jounalid[0]))

            for myAuthor in article.findall(u".//Author"):
                lastnameelement=myAuthor.find(u'LastName')
                if lastnameelement is not None and lastnameelement.text is not None:
                    lastname = (lastnameelement.text).encode('utf-8').decode('utf-8').replace('\'',"\'\'")
                else:
                    lastname = u''
                forenameelement=myAuthor.find(u'ForeName')
                if forenameelement is not None and forenameelement.text is not None:
                    forename = (forenameelement.text).encode('utf-8').decode('utf-8').replace('\'',"\'\'")
                else:
                    forename = u''
                initialselement=myAuthor.find(u'Initials')
                if initialselement is not None and initialselement.text is not None:
                    initials = (initialselement.text).encode('utf-8').decode('utf-8').replace('\'',"\'\'")
                else:
                    initials = u''
                suffixelement=myAuthor.find(u'Suffix')
                if suffixelement is not None and suffixelement.text is not None:
                    suffix = (suffixelement.text).encode('utf-8').decode('utf-8')
                else:
                    suffix = u''
                if lastname is u'' and forename is u'':
                    continue
                if suffix is u'':
                    fullname = (lastname + ', ' + forename )
                else:
                    fullname = (lastname + ', ' + forename + ' ' + suffix)

                if fullname.replace('\'\'',"\'") in set(Authorsdict.keys()):
                    authorID=(Authorsdict[fullname],)
                else:
                    authorID=cur.execute("select ID from AUTHORS where Fullname = '%s'"%(fullname)).fetchone()
                if authorID is None:
                    cur.execute("insert into AUTHORS (ID,Fullname,Lastname,Forname,Initials,Suffix) values(null,'%s','%s','%s','%s','%s')" %(fullname,lastname,forename,initials,suffix))
                    authorID=cur.execute("select ID from AUTHORS where Fullname = '%s'"%(fullname)).fetchone()
                    Authorsdict[fullname]=authorID[0]
                affilation=myAuthor.find(u'Affiliation')
                if affilation is not None and affilation.text is not None:
                    description= affilation.text.encode('utf-8').decode('utf-8').replace('\'',"\'\'")
                    #descriptionID=cur.execute("select Authors_ID from AUTHORS_DESCRIPTIONS where Authors_ID='%s' and Affiliation='%s'"%(authorID[0],description)).fetchone()
                    #if descriptionID is None:
                         #print "insert into AUTHORS_DESCRIPTIONS (Authors_ID,Affiliation) values('%s','%s')" %(authorID[0],description.replace('\'',"\'\'"))
                    try:
                        cur.execute("insert into AUTHORS_DESCRIPTIONS (Authors_ID,Affiliation) values('%s','%s')" %(authorID[0],description))
                    except sqlite3.IntegrityError:
                        pass
                #if cur.execute("select Authors_ID from ARTICLES_WRITERS where Authors_ID='%s' and Article_ID='%s'"%(authorID[0],pmid)).fetchone() is None:
                try:
                    cur.execute("insert into ARTICLES_WRITERS (Article_ID,Authors_ID) values('%s','%s')" %(pmid,authorID[0]))
                except sqlite3.IntegrityError:
                    pass

workedlist=list()
if os.path.isfile(workpath + 'Author.txt'):
    with open(workpath + 'Author.txt', 'r') as readlist:
            for line in readlist:
                workedlist.append(line.replace('\n', '').decode('utf8'))

for path in glob.glob( filepath+ '*.gz'):
    print ('work on'+path)
    if path not in workedlist:
        articlesfiles= gzip.open( path, 'r')
        for line in articlesfiles:
            getarticle(line.decode('utf8').encode('utf8'))
        conn.commit()
        articlesfiles.close()

        timestamp = gettimestamp()
        workedlist.append(path)
        with open(workpath+'readlist.txt','a') as readlist:
            readlist.writelines(path+'\n')

