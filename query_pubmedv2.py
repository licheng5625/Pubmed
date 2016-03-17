# -*- coding:utf-8 -*-
import urllib
from Bio import Entrez

Entrez.email = 'licheng5625@gmail.com'


def search(query):
    handle = Entrez.esearch(db="pubmed", term=query, usehistory="Y")
    search_results = Entrez.read(handle)
    ids = search_results["IdList"]
    count = len(ids)

    if count == 0:
        return 'no match'
    webenv = search_results["WebEnv"]
    query_key = search_results["QueryKey"]
    batch_size = 500
    data = urllib.urlencode(
        dict(db='pubmed', rettype='medline', retstart=0, retmax=batch_size, webenv=webenv, query_key=query_key,
             retmode='xml'))
    fetch_handle = urllib.urlopen('http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?', data)
    return fetch_handle