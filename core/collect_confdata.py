import os,sys,json
from core.search import *
from core.conf_names import *
from collections import Counter
from datetime import datetime
from itertools import combinations
import pandas as pd
import numpy as np

def collect_conf_papers(conf_acronym, conf_title):
    conf = es_search_conference_name(conf_acronym, conf_title)
    conf_id = conf["ConferenceSeriesId"]
    conf_papercnt = conf["PaperCount"]
    papers = es_search_papers_from_confid(conf_id, conf_papercnt)
    print(len(papers))

    data = []
    for i, p in enumerate(papers):
        pinfo = {}
        pid = pinfo["PaperId"] = p["PaperId"]
        pinfo["Year"] = p["Year"]
        pinfo["OriginalTitle"] = p["OriginalTitle"]
        pinfo["PAA"] = es_search_aff_info_from_pid(pid)
        pinfo["FOS"] = es_get_paper_fos(pid)
        print(conf_acronym, "{}/{}".format(i+1, len(papers)), pid)
        data.append(pinfo)

    with open("../data/conferences/{}_papers.json".format(conf_acronym), "w") as outfile:
        json.dump(data, outfile)


def refine_conf_data(conf_acronym):
    confpapers = json.load(open("../data/conferences/{}_papers.json".format(cc), "r"))
    data = []
    for p in confpapers:
        fos = [res["_source"] for res in p["FOS"]]
        p["FOS"] = fos
        data.append(p)
    with open("../data/conferences/{}_papers_new.json".format(conf_acronym), "w") as outfile:
        json.dump(data, outfile)

for cc, ctitle in ai_conf.items():
    if os.path.exists("../data/conferences/{}_papers.json".format(cc)):
        print("File exists", cc)
        continue
    collect_conf_papers(cc, ctitle)