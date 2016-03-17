# -*- coding:utf-8 -*-
import json
import sys
import threading
import query_pubmed
import xml.etree.ElementTree as ET
import time
import os
import glob
import mail_me
import gzip

AuthorName = dict()
seeingAuthor = set()
myerror = ''
SeenAuthor = set()
buffer = u''
Seenflag = False
Authorflag=False
inter_data_path = '../intermediate_data/pubmed/'
raw_data_path = '../raw_data/pubmed/'
Timeofquery = " AND (\"2002\"[Date - Publication] : \"2014\"[Date - Publication]))"
SeedQuery = "((ebola \"humans\"[MeSH Terms])" + Timeofquery
writebuffer = threading.Lock()
writeAuthor = threading.Lock()
querylock= threading.Lock()

writequery = threading.Lock()
gatherlock = threading.Lock()
starlock= threading.Lock()
semaphore = 0
mainthread=True


def gettimestamp():
    format_type = u'%Y_%m_%d_%H_%M_%S'
    return time.strftime(format_type, time.localtime(time.time()))


def getauthor(XmlData, threadlock):
    XmlData = XmlData.encode('utf8')
    xmltree = ET.fromstring(XmlData)
    for Author in xmltree.findall(u".//Author"):
        if Author.find(u'LastName') is not None and Author.find(u'LastName').text is not None:
            lastname = (Author.find(u'LastName').text).encode('utf-8').decode('utf-8')
        else:
            lastname = u''
            if Author.find(u'ForeName') is None:
                return
        if Author.find(u'ForeName') is not None and Author.find(u'ForeName').text is not None:
            forename = (Author.find(u'ForeName').text).encode('utf-8').decode('utf-8')
        else:
            forename = u''
        if Author.find(u'Initials') is not None and Author.find(u'Initials').text is not None:
            initials = (Author.find('Initials').text).encode('utf-8').decode('utf-8')
        else:
            initials = u''
        if Author.find(u'Suffix') is not None:
            if Author.find(u'Suffix').text is not None:
                suffix = (Author.find(u'Suffix').text).encode('utf-8').decode('utf-8')
            else:
                suffix = u''
        else:
            suffix = u''
        threadlock.acquire()
        fullname = (lastname + ', ' + forename + ' ' + suffix)
        shortname = (lastname + ' ' + initials + ' ' + suffix)
        AuthorName[fullname] = shortname
        threadlock.release()


def saveseed(seed):
    with open(raw_data_path + 'seed_Article' + gettimestamp() + '.txt', 'w') as SeedArticle:
        SeedArticle.writelines((str(seed.encode('utf8'))).replace('\n', ''))
        # SeedArticle.writelines(str(seed) + '\n')


def savequery(query):
    with open(raw_data_path + 'Articles.' + gettimestamp() + '.txt', 'a') as Article:
        print('writing ' + raw_data_path + 'Articles.' + gettimestamp() + '.txt')
        Article.writelines(query.encode('utf-8'))


class MyThread(threading.Thread):

    def __init__(self, id):
        threading.Thread.__init__(self)
        self.id = id

    def run(self):
        print('%d runing' % (self.id))
        global mainthread
        global timestamp
        global buffer
        global semaphore
        global SeenAuthor
        global AuthorName
        global myerror
        global seeingAuthor
        global myquery
        global Seenflag
        global Authorflag
        try:
            semaphore += 1
            while myerror is '' and mainthread:
                for Fname in AuthorName.keys():
                    if not mainthread or myerror is not '':
                        break
                    if Fname not in SeenAuthor and Fname not in seeingAuthor:
                        seeingAuthor.add(Fname)
                        query = query_pubmed.search(Fname + "[FAU]" + Timeofquery)
                        #if query == 'no match':
                            #query = query_pubmed.search(AuthorName[Fname] + "[AU]" + Timeofquery)
                        if not query == 'no match':
                            result = ( query.read()).decode('utf8')
                            getauthor(result, gatherlock)
                            writebuffer.acquire()
                            buffer += result.replace(u'\n', u'') + u'\n'
                            if Authorflag and len(SeenAuthor) % 1000 == 0:
                                savequery(buffer)
                                Seenflag=True
                                buffer = ''
                                Authorflag=False
                            writebuffer.release()
                        gatherlock.acquire()
                        SeenAuthor.add(Fname)
                        if Seenflag:
                            timestamp = gettimestamp()
                            JSON = json.dumps(AuthorName, ensure_ascii=False)
                            with open(inter_data_path + 'temp_AuthorName.txt', 'w') as Name:
                                Name.writelines(JSON.encode('utf8'))
                            with open(inter_data_path + 'temp_AuthorName.' + timestamp + '.txt', 'w') as Name:
                                Name.writelines(JSON.encode('utf8'))
                            with open(inter_data_path + 'temp_Seenlist.txt', 'w') as Seenlist:
                                for name in SeenAuthor:
                                    Seenlist.writelines(name.encode('utf8') + '\n')
                            with open(inter_data_path + 'temp_Seenlist.' + timestamp + '.txt', 'w') as Seenlist:
                                for name in SeenAuthor:
                                    Seenlist.writelines(name.encode('utf8') + '\n')
                            Seenflag=False
                        gatherlock.release()
                        print('%d work on %d in %d' % (self.id, len(SeenAuthor), len(AuthorName)))
                        if Authorflag is False and len(SeenAuthor) % 1000 > 800:
                            writebuffer.acquire()
                            Authorflag = id
                            writebuffer.release()
                        if Fname in seeingAuthor:
                            seeingAuthor.remove(Fname)
                    if len(SeenAuthor) == len(AuthorName):
                        print('the end~~~~')
                        semaphore -= 1
                        return
            semaphore -= 1
        except:

            import traceback

            semaphore -= 1
            myerror = str(traceback.format_exc())
            print(myerror)

try:
    if len(sys.argv) == 1:
        seed = query_pubmed.search(SeedQuery).read().decode('utf8')
        getauthor(seed, gatherlock)
        saveseed(seed)
    else:
        if sys.argv[1] == '-r':
            with open(inter_data_path + 'temp_AuthorName.txt', 'r') as Name:
                AuthorName = json.loads(Name.read())
            with open(inter_data_path + 'temp_Seenlist.txt', 'r') as Seenlist:
                for line in Seenlist:
                    SeenAuthor.add(line.replace('\n', '').decode('utf8'))
        else:
            if sys.argv[1] == '-c':
                for path in glob.glob(inter_data_path + '*'):
                    os.remove(path)
                for path in glob.glob(raw_data_path + '*'):
                    os.remove(path)
                print('clean up')
                sys.exit(0)
            else:
                print('illegal input')
                sys.exit(0)
    thread=[]
    for i in range(10):
        thread.append(MyThread(i))

    for t in thread:
        t.setDaemon(True)
    for t in thread:
        t.start()



    while True:

        if myerror is not '':
            raise 'thread'
        time.sleep(1)
except:
    mainthread=False
    info = sys.exc_info()
    error = str(info[0]) + ' : ' + str(info[1])
    print(error)
    global semaphore
    # mail_me.sendmail("Pubmed Error", error + '  ' + gettimestamp(), '')
    while True:
        if semaphore == 0:
            if buffer != '':
                savequery(buffer)
            timestamp = gettimestamp()
            JSON = json.dumps(AuthorName, ensure_ascii=False)
            with open(inter_data_path + 'temp_AuthorName.txt', 'w') as Name:
                Name.writelines(JSON.encode('utf8'))
            with open(inter_data_path + 'temp_AuthorName.' + timestamp + '.txt', 'w') as Name:
                Name.writelines(JSON.encode('utf8'))
            with open(inter_data_path + 'temp_Seenlist.txt', 'w') as Seenlist:
                for name in SeenAuthor:
                    Seenlist.writelines(name.encode('utf8') + '\n')
            with open(inter_data_path + 'temp_Seenlist.' + timestamp + '.txt', 'w') as Seenlist:
                for name in SeenAuthor:
                    Seenlist.writelines(name.encode('utf8') + '\n')
            import traceback

            if myerror is not '':
                print myerror
                mail_me.sendmail("Pubmed Error", myerror + gettimestamp(), '')
            else:
                traceback.print_exc()
                warning = traceback.format_exc(limit=-1).replace('\n', '')
                if warning.find('KeyboardInterrupt') is -1:
                    mail_me.sendmail("Pubmed Error", error + str(traceback.format_exc()) + gettimestamp(), '')
            break

