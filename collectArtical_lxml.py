# -*- coding:utf-8 -*-
import json
import sys
import query_pubmed
from lxml import etree
import time

AuthorName = dict()
SeenAuthor = set()
buffer = str()
inter_data_path = '../intermediate_data/pubmed/'
raw_data_path='../raw_data/pubmed/'
# RawData = open(PATH+'log'+timeStamp+'.txt', 'a')
Timeofquery = " AND (\"2002\"[Date - Publication] : \"2014\"[Date - Publication]))"
SeedQuery = "((ebola \"humans\"[MeSH Terms])" + Timeofquery


def gettimestamp():
    format_type = '%Y_%m_%d_%H_%M_%S'
    return time.strftime(format_type, time.localtime(time.time()))


def getauthor(XmlData):
    xmltree = etree.fromstring(XmlData)
    for Author in xmltree.xpath("//Author"):
        # elements =etree._Element()
        # elements.attrib
        for elements in Author:
            suffix = ''
            if elements.tag == 'LastName':
                lastname = elements.text
            else:
                if elements.tag == 'ForeName':
                    forename = elements.text
                else:
                    if elements.tag == 'Initials':
                        initials = elements.text
                    else:
                        if elements.tag == 'Suffix':
                            suffix = elements.text
        fullname = lastname + ', ' + forename + ' ' + suffix
        shortname = lastname + ' ' + initials + ' ' + suffix
        AuthorName[fullname] = shortname


def saveseed(seed):
    with open(raw_data_path + 'seed_Article' + gettimestamp() + '.txt', 'w') as SeedArticle:
        SeedArticle.writelines(str(seed).replace('\n', ''))
        # SeedArticle.writelines(str(seed) + '\n')


def savequery(query):
    with open(raw_data_path + 'Articles.' + gettimestamp() + 'txt', 'a') as Article:
        Article.writelines(query.replace('\n', '') + '\n')





try:
    if len(sys.argv) == 1:
        seed = query_pubmed.search(SeedQuery).read()
        getauthor(seed)
        saveseed(seed)
    else:
        if sys.argv[1] == 'r':
            with open(inter_data_path + 'temp_AuthorName.txt', 'r') as Name:
                AuthorName = json.loads(Name.read())
            with open(inter_data_path + 'temp_Seenlist.txt', 'r') as Seenlist:
                for line in Seenlist:
                    SeenAuthor.add(line.replace('\n',''))
        else:
            print 'illegal input'
            sys.exit(0)
    while True:
        for Fname in AuthorName.keys():
            if Fname not in SeenAuthor:
                query = query_pubmed.search(Fname + "[FAU]" + Timeofquery)
                if query == 'no match':
                    query = query_pubmed.search(AuthorName[Fname] + "[AU]" + Timeofquery)
                if not query == 'no match':
                    result = query.read()
                    getauthor(result)
                if len(SeenAuthor) % 1000==0:
                    savequery(result)
                SeenAuthor.add(Fname)
            print ('work on %d in %d' % (len(SeenAuthor), len(AuthorName)))
            if len(SeenAuthor) == len(AuthorName):
                print 'the end~~~~'
                break
except:
    info = sys.exc_info()
    print info[0],":",info[1]
    with open(inter_data_path + 'temp_AuthorName.txt', 'w') as Name:
            JSON = json.dumps(AuthorName)
            Name.writelines(JSON)
    with open(inter_data_path + 'temp_Seenlist.txt', 'w') as Seenlist:
            for name in SeenAuthor:
                Seenlist.writelines(name+'\n')



