from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from operator import itemgetter

ES_SERVER = "130.56.248.215:9200"

def es_search_paper_title(title):
    client = Elasticsearch(ES_SERVER, request_timeout=120)
    s = Search(using=client, index="papers")
    s = s.query("match", PaperTitle=title)
    response = s.execute()
    result = response.to_dict()["hits"]["hits"]
    data = {}
    if result:
        data = result[0]["_source"]
    else:
        print("[es_search_paper_title] no result", title)
    return data

def es_search_authors_from_pid(paperid):
    client = Elasticsearch(ES_SERVER, request_timeout=60)
    s = Search(using=client, index="paperauthoraffiliations")
    s = s.query("match", PaperId=paperid)
    s = s.params(size=1000)
    response = s.execute()
    result = response.to_dict()["hits"]["hits"]
    cols = ["AuthorId", "AffiliationId"]
    data = None
    if result:
        data = [{c:res["_source"][c] if c in res["_source"] else None for c in cols} for res in result]
    else:
        print("[es_search_authors_from_pid] no result", paperid)
    return data

def es_search_paper_from_pid(paperid):
    client = Elasticsearch(ES_SERVER, request_timeout=60)
    s = Search(using=client, index="papers")
    s = s.query("match", PaperId=paperid)
    response = s.execute()
    result = response.to_dict()["hits"]["hits"]
    data = None
    if result:
        data = result[0]["_source"]
    else:
        print("[es_search_authors_from_pid] no result", paperid)
    return data

def es_search_papers_from_aid(authorid):
    client = Elasticsearch(ES_SERVER, request_timeout=60)
    s = Search(using=client, index="paperauthoraffiliations")
    s = s.query("match", AuthorId=authorid)
    s = s.params(size=1000)
    response = s.execute()
    result = response.to_dict()["hits"]["hits"]
    data = []
    if result:
        data = [res["_source"]["PaperId"] for res in result]
    else:
        print("[es_search_papers_from_authorid] no result", authorid)
    return data

def es_search_author_name(authorid):
    client = Elasticsearch(ES_SERVER, request_timeout=60)
    s = Search(using=client, index="authors")
    s = s.query("match", AuthorId=authorid)
    response = s.execute()
    result = response.to_dict()["hits"]["hits"]
    cols = ["AuthorId", "DisplayName", "NormalizedName", "PaperCount", "CitationCount"]
    data = {}
    if result:
        data = {c:result[0]["_source"][c] for c in cols}
    else:
        print("[es_search_author_name] no result", authorid)
    return data


def es_get_paper_conf_year(confid, year):
    client = Elasticsearch(ES_SERVER, request_timeout=60)
    s = Search(using=client, index="papers")
    s = s.query(Q('bool', must=[Q('match', ConferenceSeriesId=confid), Q('match', Year=year)]))
    s = s.params(size=500)
    response = s.execute()
    result = response.to_dict()["hits"]["hits"]
    return result


def es_get_paper_fos(paperids):
    client = Elasticsearch(ES_SERVER, request_timeout=60)
    s = Search(using=client, index="paperfieldsofstudy")
    s = s.query("terms", PaperId=paperids)
    s = s.params(size=500)
    response = s.execute()
    result = response.to_dict()["hits"]["hits"]
    return result

def es_get_paper_fos(paperids):
    client = Elasticsearch(ES_SERVER, request_timeout=60)
    s = Search(using=client, index="paperfieldsofstudy")
    s = s.query("terms", PaperId=paperids)
    s = s.params(size=500)
    response = s.execute()
    result = response.to_dict()["hits"]["hits"]
    return result

def es_get_fos_level(fosids):
    client = Elasticsearch(ES_SERVER, request_timeout=60)
    s = Search(using=client, index="fieldsofstudy")
    s = s.query("terms", FieldOfStudyId=fosids)
    s = s.params(size=500)
    response = s.execute()
    result = response.to_dict()["hits"]["hits"]
    return result

def es_author_normalize(name):
    name = name.replace("-", "")
    name = name.replace("'", "")
    client = Elasticsearch(ES_SERVER, request_timeout=300)
    s = Search(using=client, index="authors")
    s = s.query("match", NormalizedName=name)
    s = s.params(size=500)
    response = s.execute()
    result = response.to_dict()["hits"]["hits"]
    sorted_list = sorted([s["_source"] for s in result if s["_score"] > 16], key=itemgetter("Rank"))
    if len(sorted_list) == 0:
        sorted_list = sorted([s["_source"] for s in result if s["_score"] > 13], key=itemgetter("Rank"))
    if len(sorted_list) == 0:
        sorted_list = sorted([s["_source"] for s in result], key=itemgetter("Rank"))
    sorted_list = sorted(sorted_list, key=itemgetter("PaperCount"), reverse=True)
    # print(name)
    # print(sorted_list)
    data = {}
    try:
        data = sorted_list[0]
    except Exception as e:
        print("[es_author_normalize] no result", name, e)
    return data
