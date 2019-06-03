from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from operator import itemgetter

ES_SERVER = "130.56.249.107:9200"

def es_search_paper_title(title):
    client = Elasticsearch(ES_SERVER, request_timeout=60)
    s = Search(using=client, index="papers")
    s = s.query("match", PaperTitle=title)
    response = s.execute()
    result = response.to_dict()["hits"]["hits"]
    data = {}
    if result:
        data = result[0]["_source"]
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
    cols = ["AuthorId", "DisplayName", "NormalizedName"]
    data = {}
    if result:
        data = {c:result[0]["_source"][c] for c in cols}
    else:
        print("[es_search_paper_title] no result")
    return data


def es_author_normalize(firstname, lastname):
    client = Elasticsearch(ES_SERVER, request_timeout=60)
    s = Search(using=client, index="authors")
    s = s.query("match", NormalizedName="{} {}"
        .format(firstname.lower().replace("-", ""), lastname.lower().replace("-", "")))
    s = s.params(size=30)
    response = s.execute()
    result = response.to_dict()["hits"]["hits"]
    sorted_list = sorted([s["_source"] for s in result if s["_score"] > 13], key=itemgetter("Rank"))
    # print(firstname, lastname)
    # print(sorted_list)
    data = {}
    if result:
        data = sorted_list[0]
    else:
        print("[es_search_paper_title] no result")
    return data
