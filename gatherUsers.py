import query_pubmed
import sys
import json
import time
# -*- coding:utf-8 -*-

PATH='../../../data/getPubmedUsers/'


def stamp2time(stamp, format_type='%Y_%m_%d_%H_%M_%S'):
    return time.strftime(format_type, time.localtime(stamp))

timeStamp = time.time()
timeStamp=stamp2time(timeStamp)
runningtime =0
BackupRate=1000
authorlist=[]
Articlelist=[]
ProcessedArticls={}
processingnum=0


loglist = open(PATH+'log'+timeStamp+'.txt', 'a')

class Author:
    def __init__(self, id):
        self.id = id
        self.name = ''
        self.coauthors=[]
        self.articles=[]
        self.Corporate=''
    
    def MakeJSON(self):
        dic = {'ID':self.id,'Name':self.name,'Description':self.Corporate,'Articles':self.articles,'Coauthors':self.coauthors}
        JSON = json.dumps(dic)
        return JSON
class Article:
    def __init__(self, id):
        self.title=''
        self.pmid=''
        self.id=id
        self.date=''
        self.authors=[]
        self.terms=[]
        self.Journal=''
    def MakeJSON(self):
        dic = {'ID':self.id,'Title':self.title,'Pmid':self.pmid,'Date':self.date,'Terms':self.terms,'Journal':self.Journal,'Authors':self.authors}
        JSON = json.dumps(dic)
        return JSON

def coauthors(autho):
    if(10315 in autho):
        print ('wozai  ')

    for af in autho:
        for ds in autho:
            if(af != ds):
                try:
                    authorlist[af].coauthors.append(ds)
                except IndexError as report:
                    print ("Fehler!  ",report)
                    loglist.writelines ("Fehler!  ")
                    loglist.writelines (  report)
                    loglist.writelines('\n')
                    backup(authorlist,Articlelist)
                    print ("Fehler!  durning %d"%(processingnum))
                    print (str(af)+" in "+str(autho)+" len:  "+str(len(authorlist)))
                    loglist.writelines ('Fehler!  durning '+str(processingnum)+'\n')
                    loglist.writelines (str(af)+" in "+str(autho)+" len:  "+str(len(authorlist)))
                    sys.exit(0)



def changename(name):
    markslist=[' ','.',',','-','1','2','3','4','5','6','7','8','9','0','\'']
    n = len(name)
    name=name.lower()
    word=name
    for i in range(n):
        if(word[i] in markslist):
            name=name.replace(word[i],'')
    return name

def isArticlProcessed(id):
    if(ProcessedArticls.has_key(id)):
        return True
    else:
        ProcessedArticls[id]=True
        return False



def Process(seed):
    
    myArticle=None
    myAuthor=None
    authorlistofoneAr=None
    for terms in seed:
        if(terms.find("PMID- ")!=-1):
            if(myArticle!=None and isArticlProcessed(myArticle.pmid)==False):
                if(myAuthor!=None):
                    authorlist.append(myAuthor)
                myArticle.authors=authorlistofoneAr
                Articlelist.append(myArticle)
                coauthors(authorlistofoneAr)
        #Articleoutput.writelines(myArticle.MakeJSON()+'\n')
            myAuthor=None
            myArticle=None
            authorlistofoneAr=[]
            sr=terms.split('- ')
            myArticle =Article(len(Articlelist))
            myArticle.pmid=sr[1]
            myArticle.id=len(Articlelist)
        if(terms.find("DP  - ")!=-1):
            sr=terms.split('- ')
            myArticle.date=sr[1]
        if (terms.find("TI  - ")!=-1):
            sr=terms.split('- ')
            myArticle.title=sr[1]
        if (terms.find("JT  - ")!=-1):
            sr=terms.split('- ')
            myArticle.Journal=sr[1]
        if (terms.find("MH  - ")!=-1):
            sr=terms.split('- ')
            myArticle.terms.append(sr[1])
        if (terms.find("FAU - ")!=-1):
            if(myAuthor!=None):
                authorlist.append(myAuthor)
            sr=terms.split('- ')
            name=(sr[1])
            if (myTrie.search(name)==False):
                myAuthor =Author(len(authorlist))
                myAuthor.name=(name)
                if(myTrie.add(myAuthor.name,myAuthor.id)==True):
                    authorlistofoneAr.append(myAuthor.id)
                    myAuthor.articles.append(myArticle.id)
                else:
                    myAuthor=None
            else:
                id=myTrie.getID(name)
                myAuthor=authorlist[id]
                myAuthor.articles.append(myArticle.id)
                authorlistofoneAr.append(myAuthor.id)
                myAuthor=None
        if (terms.find("AD  -")!=-1):
            if(myAuthor!=None):
                sr=terms.split('- ')
                myAuthor.Corporate=(sr[1])

    if(myArticle!=None):
        if(myAuthor!=None and isArticlProcessed(myArticle.pmid)==False):
            authorlist.append(myAuthor)
        myArticle.authors=authorlistofoneAr
        Articlelist.append(myArticle)
        coauthors(authorlistofoneAr)
#Articleoutput.writelines(myArticle.MakeJSON()+'\n')

def backup(authors,articles):
    timeStamp = time.time()
    timeStamp=stamp2time(timeStamp)
    loglist.writelines("writing at "+timeStamp+'\n')
    print("writing at %s"%(timeStamp))
    with open(PATH+'user_backup'+timeStamp+'.txt', 'w') as AuthorsOutput:
        for line in authors:
            output_list = line.MakeJSON()
            AuthorsOutput.writelines(output_list+'\n')
    with open(PATH+'artical_backup'+timeStamp+'.txt', 'w') as ArticlesOutput:
        for line in articles:
            output_list = line.MakeJSON()
            ArticlesOutput.writelines(output_list+'\n')




with open(PATH+'seed_Article'+timeStamp+'.txt', 'w') as ArticlesOutput:
    seed = query_pubmed.search("((ebola \"humans\"[MeSH Terms]) AND (\"2002\"[Date - Publication] : \"2014\"[Date - Publication])) ",False)
    AuthorsOutput.writelines(output_list+'\n')

Process(seed)

for author in authorlist:
    #authors=query_pubmed.search(author.replace(" ","+")+"[Author]",False)
    authors=query_pubmed.search(author.name+"[FAU]",False)
    Process(authors)	
    if(processingnum%300==0):
        print("working on subject: %d in %d"%(processingnum, len(authorlist)))
    if(processingnum%BackupRate==0):
        loglist.writelines("working on subject: %d in %d \n"%(processingnum, len(authorlist)))
        loglist.writelines("( "+author.name+"[FAU] AND (\"2002\"[Date - Publication] : \"2014\"[Date - Publication]))"+'\n')
        backup(authorlist,Articlelist)
    processingnum=processingnum+1
        #print output_list
        #jsn = json.dumps(output_list)
        #users = json.loads(jsn)
print ('the end')
backup(authorlist,Articlelist,timeStamp)




