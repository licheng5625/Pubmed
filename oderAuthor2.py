import glob
import json
import xml.etree.cElementTree as ET
import gzip
import os
import time

articlesset=dict()
AuthorNameset=dict()
journalset=dict()

AuthorList=list()
ArticleList=list()
Journallist=list()

changeAuthorList=list()

workpath='./../analysis/data/'
filepath='./../raw_data/pubmed/'
workpath=''
filepath=''
def gettimestamp():
    format_type = u'%Y_%m_%d_%H_%M_%S'
    return time.strftime(format_type, time.localtime(time.time()))

class Journal:
    def __init__(self,id,ISSN,Title):
        self.id=id
        self.ISSN=ISSN
        self.Title=Title
        self.Articles=set()
        self.arthors=set()

    def MakeJSON(self):
        dic = {'ID':self.id,'Title':self.Title,'ISSN':self.ISSN,'Articles':list(self.Articles),'arthors':list(self.arthors)}
        JSON = json.dumps(dic,ensure_ascii=False)
        return JSON

    def resume(self,dictdata):
        self.title=dictdata['Title']
        self.ISSN=dictdata['ISSN']
        self.id=dictdata['ID']
        self.Articles=set(dictdata['Articles'])
        self.arthors=set(dictdata['arthors'])

class Article:
    def __init__(self, id):
        self.title=''
        self.pmid=''
        self.id=id
        self.date=dict()
        self.authors=set()
        self.terms=dict()
        self.Journal=None
    def MakeJSON(self):
        dic = {'ID':self.id,'Title':self.title,'Pmid':self.pmid,'Date':self.date,'Terms':self.terms,'Journal':self.Journal,'Authors':list(self.authors)}
        JSON = json.dumps(dic,ensure_ascii=False)
        return JSON

    def resume(self,dictdata):
        self.title=dictdata['Title']
        self.pmid=dictdata['Pmid']
        self.id=dictdata['ID']
        self.date=dictdata['Date']
        self.authors=set(dictdata['Authors'])
        self.terms=dictdata['Terms']
        self.Journal=dictdata['Journal']


class Author:
    def __init__(self, id):
        self.id = id
        self.lastname = ''
        self.forename = ''
        self.initials = ''
        self.suffix = ''
        self.fullname=''
        self.coauthors=set()
        self.articles=set()
        self.Affiliation=[]

    def MakeJSON(self):
        dic = {'ID':self.id,'Lastname':self.lastname,'Forename':self.forename,'Initials':self.initials,'Suffix':self.suffix,'Coauthor':list(self.coauthors),'Articles':list(self.articles),'Affiliation':self.Affiliation,'FAU':self.fullname}
        JSON = json.dumps(dic,ensure_ascii=False)
        return JSON
    def resume(self,dictdata):
        self.id=dictdata['ID']
        self.lastname=dictdata['Lastname']
        self.forename = dictdata['Forename']
        self.initials = dictdata['Initials']
        self.suffix = dictdata['Suffix']
        self.fullname = dictdata['FAU']
        self.coauthors=set(dictdata['Coauthor'])
        self.articles=set(dictdata['Articles'])
        self.Affiliation=dictdata['Affiliation']



def coatricle(articlelist):
    ids=set()
    for af in articlelist:
         ids.add(af.id)
    for ds in articlelist:
        ds.coauthors=ds.coauthors.union(ids)
        ds.coauthors.remove(ds.id)



def getarticle(XmlData):

    xmltree = ET.fromstring(XmlData)
    for article in xmltree.findall(".//PubmedArticle"):
        pmid = article.find('.//PMID').text
        if pmid not in articlesset:
            #print pmid
            myarticle=Article(len(ArticleList))
            ArticleList.append(myarticle)
            journal =article.find(u'.//Journal')
            journalTitle=journal.find(u'Title').text
            if journalTitle not in journalset:
                if journal.find('ISSN') is not None and journal.find('ISSN').text is not None:
                    journalISSN=journal.find('ISSN').text.encode('utf-8').decode('utf-8')
                else:
                    journalISSN=None
                myjournal=Journal(len(Journallist),journalISSN,journalTitle)
                journalset[journalTitle]=len(Journallist)
                Journallist.append(myjournal)
            else:
                myjournal=Journallist[journalset[journalTitle]]
            AuthorlistINonearticle=list()
            for myAuthor in article.findall(u".//Author"):
                if myAuthor.find(u'LastName') is not None and myAuthor.find(u'LastName').text is not None:
                    lastname = (myAuthor.find(u'LastName').text).encode('utf-8').decode('utf-8')
                else:
                    lastname = u''
                if myAuthor.find(u'ForeName') is not None and myAuthor.find(u'ForeName').text is not None:
                    forename = (myAuthor.find(u'ForeName').text).encode('utf-8').decode('utf-8')
                else:
                    forename = u''
                if myAuthor.find(u'Initials') is not None and myAuthor.find(u'Initials').text is not None:
                    initials = (myAuthor.find('Initials').text).encode('utf-8').decode('utf-8')
                else:
                    initials = u''
                if myAuthor.find(u'Suffix') is not None and myAuthor.find(u'Suffix').text is not None:
                    suffix = (myAuthor.find(u'Suffix').text).encode('utf-8').decode('utf-8')
                else:
                    suffix = u''
                if lastname is u'' and forename is u'':
                    continue
                fullname = (lastname + ', ' + forename + ' ' + suffix)
                if fullname not in AuthorNameset:
                    id=len(AuthorList)
                    AuthorNameset[fullname]=id
                    tempAuthor = Author(id)
                    tempAuthor.lastname = lastname
                    tempAuthor.forename = forename
                    tempAuthor.initials = initials
                    tempAuthor.suffix = suffix
                    tempAuthor.fullname=fullname
                    AuthorList.append(tempAuthor)
                else:
                    tempAuthor=AuthorList[AuthorNameset[fullname]]
                if myAuthor.find(u'Affiliation') is not None and myAuthor.find(u'Affiliation').text is not None:
                    if myAuthor.find(u'Affiliation').text  not in tempAuthor.Affiliation:
                        tempAuthor.Affiliation.append(myAuthor.find(u'Affiliation').text.encode('utf-8').decode('utf-8'))
                if myarticle.id not in tempAuthor.articles:
                    tempAuthor.articles.add(myarticle.id)
                if tempAuthor.id not in myarticle.authors:
                    myarticle.authors.add(tempAuthor.id)

                AuthorlistINonearticle.append(tempAuthor)

                if tempAuthor.id not in  myjournal.arthors:
                    myjournal.arthors.add(tempAuthor.id)

                if myarticle.id not in  myjournal.Articles:
                    myjournal.Articles.add(myarticle.id)

            coatricle(AuthorlistINonearticle)

            articlesset[pmid]=len(ArticleList)
            myarticle.pmid=pmid
            myarticle.Journal = myjournal.id
            if article.find(u'.//ArticleTitle') is not None and article.find(u'.//ArticleTitle').text is not None:
                myarticle.title=(article.find(u'.//ArticleTitle').text).encode('utf-8').decode('utf-8')
            else:
                myarticle.title=(article.find(u'.//VernacularTitle').text).encode('utf-8').decode('utf-8')
            ArticleDate=article.find(u'.//DateCreated')
            myarticle.date = {'year':(ArticleDate.find('Year').text).encode('utf-8').decode('utf-8'),'mon':(ArticleDate.find('Month').text).encode('utf-8').decode('utf-8'),'day':(ArticleDate.find('Day').text).encode('utf-8').decode('utf-8')}

            for terms in article.findall(u".//MeshHeading"):
                DescriptorName=(terms.find('DescriptorName').text).encode('utf-8').decode('utf-8')
                QualifierNames=list()
                for QualifierName in terms.findall(u'QualifierName'):
                    QualifierNames.append(QualifierName.text)
                myarticle.terms[DescriptorName]=QualifierNames



workedlist=list()
oldfilename= gettimestamp()


if os.path.isfile(workpath+'Author.txt'):
    Authordumplist=list()
    with open(workpath+'Author.txt', 'r') as Name:
        for line in Name:
            Authordumplist.append(json.loads(line.decode('utf8')))
    AuthorList=[None]*len(Authordumplist)
    for aut in Authordumplist:
        tempaut=Author(0)
        tempaut.resume(aut)
        AuthorList[tempaut.id]=tempaut
        AuthorNameset[tempaut.fullname]=tempaut.id

    JournalName=list()
    with open(workpath+'journal.txt', 'r') as journalfile:
        for line in journalfile:
            JournalName.append( json.loads(line.decode('utf8')))

    Journallist=[None]*len(JournalName)
    for jou in JournalName:
        tempaut= Journal(None,None,None)
        tempaut.resume(jou)
        Journallist[tempaut.id]=tempaut
        journalset[tempaut.Title]=tempaut.id

    ArticleName=list()
    with open(workpath+'article.txt', 'r') as articlefile:
        for line in articlefile:
            ArticleName.append( json.loads(line.decode('utf8')))
    ArticleList=[None]*len(ArticleName)
    for art in ArticleName:
        tempart=Article(0)
        tempart.resume(art)
        articlesset[tempart.pmid]=tempart.id
        ArticleList[tempart.id]=tempart

    with open(workpath+'readlist.txt','r') as readlist:
        for line in readlist:
           workedlist.append(line.replace('\n', '').decode('utf8'))
    with open(workpath+'oldfilename.txt','r') as oldlist:
            oldfilename=oldlist.read()

for path in glob.glob( filepath+ '*.gz'):
    print 'work on'+path
    if path not in workedlist:
        articlesfiles= gzip.open( path, 'r')
        for line in articlesfiles:
            getarticle(line.decode('utf8').encode('utf8'))

        articlesfiles.close()

        timestamp = gettimestamp()
        print 'wrinting do not interupt'
        with open(workpath+'Author.txt','w') as authorfile:
            with open(workpath+'Author'+timestamp+'.txt','w') as authorfile2:
                for eachauthor in AuthorList:
                    JSON = eachauthor.MakeJSON()
                    authorfile.writelines(JSON.encode('utf8')+'\n')
                    authorfile2.writelines(JSON.encode('utf8')+'\n')

        with open(workpath+'journal.txt','w') as journalfile:
            with open(workpath+'journal'+timestamp+'.txt','w') as journalfile2:
                for eachjournal in Journallist:
                    JSON = eachjournal.MakeJSON()
                    journalfile.writelines(JSON.encode('utf8')+'\n')
                    journalfile2.writelines(JSON.encode('utf8')+'\n')

        with open(workpath+'article.txt','w') as articlefile:
                with open(workpath+'article'+timestamp+'.txt','w') as articlefile2:
                    for eacharticle in ArticleList:
                        JSON = eacharticle.MakeJSON()
                        articlefile.writelines(JSON.encode('utf8')+'\n')
                        articlefile2.writelines(JSON.encode('utf8')+'\n')
        workedlist.append(path)
        with open(workpath+'readlist.txt','a') as readlist:
            readlist.writelines(path+'\n')
        for dele in glob.glob( workpath+ '*'+oldfilename+'*'):
            os.remove(dele)
        oldfilename=timestamp
        with open(workpath+'oldfilename.txt','w') as writefilename:
            writefilename.writelines(oldfilename)
        print 'wrinting done'
