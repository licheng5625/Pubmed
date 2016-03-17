# -*- coding:utf-8 -*-
import urllib
from Bio import Entrez

Entrez.email = 'licheng5625@gmail.com'
inter_data_path = '../intermediate_data/pubmed/'

def log(logstring):
    with open(inter_data_path + 'log.txt', 'a') as log:
        if isinstance(logstring,unicode):
            log.writelines(logstring.encode('utf8')+'\n')
        else:
            log.writelines(logstring+'\n')



def search(query):
    if isinstance(query,unicode):
        query=query.encode('utf-8')
    handle = Entrez.esearch(db="pubmed", term=query, usehistory="Y")
    search_results = Entrez.read(handle)
    if("IdList" in search_results):
        ids = search_results["IdList"]
        count = len(ids)
    else:
        count = 0

    if count == 0:
        log(query)
        return 'no match'
    if("WebEnv" in search_results):
        webenv = search_results["WebEnv"]
    else:
        log('no webenv')
        log(query)
        return 'no match'
    if("QueryKey" in search_results):
        query_key = search_results["QueryKey"]
    else:
        log('no QueryKey')
        log(query)
        return 'no match'
    batch_size = 5
    data = urllib.urlencode(
        dict(db='pubmed', rettype='medline', retstart=0, retmax=batch_size, webenv=webenv, query_key=query_key,
             retmode='xml'))
    fetch_handle = urllib.urlopen(url='http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?',data=data)
    return fetch_handle
