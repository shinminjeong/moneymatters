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
