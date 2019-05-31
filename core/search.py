from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

ES_SERVER = "130.56.248.215:9200"

def es_search_paper_title(title):
    client = Elasticsearch(ES_SERVER, request_timeout=60)
    s = Search(using=client, index="papers")
    s = s.query("match", PaperTitle=title)
    response = s.execute()
    result = response.to_dict()["hits"]["hits"]
    cols = ["PaperId", "PaperTitle", "Year", "ReferenceCount", "CitationCount", "EstimatedCitation"]
    data = {}
    if result:
        data = {c:result[0]["_source"][c] for c in cols}
    else:
        print("[es_search_paper_title] no result")
    return data

def es_search_authors_from_pid(paperid):
    client = Elasticsearch(ES_SERVER, request_timeout=60)
    s = Search(using=client, index="paperauthoraffiliations")
    s = s.query("match", PaperId=paperid)
    response = s.execute()
    result = response.to_dict()["hits"]["hits"]
    cols = ["AuthorId"]
    data = []
    if result:
        data = [{c:res["_source"][c] for c in cols} for res in result]
    else:
        print("[es_search_paper_title] no result")
    return data


def es_search_author_name(authorid):
    client = Elasticsearch(ES_SERVER, request_timeout=60)
    s = Search(using=client, index="authors")
    s = s.query("match", AuthorId=authorid)
    response = s.execute()
    result = response.to_dict()["hits"]["hits"]
    cols = ["DisplayName", "NormalizedName"]
    data = {}
    if result:
        data = {c:result[0]["_source"][c] for c in cols}
    else:
        print("[es_search_paper_title] no result")
    return data
