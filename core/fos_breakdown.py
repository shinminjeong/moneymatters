import os,sys,json
import requests
from core.search import *

yearmap = {}
ranges = (range(1994,1999), range(1999,2004), range(2004,2009), range(2009,2014), range(2014,2019))
for range in ranges:
    aggfos = {}
    totalp = 0
    for year in range:
        res_p = es_get_paper_conf_year("1135342153", year)
        # print(year, len(res_p))
        totalp += len(res_p)
        papers = [r["_source"]["PaperId"] for r in res_p]
        # print(papers)

        res_fos = es_get_paper_fos(papers)
        fosset = set([r["_source"]["FieldOfStudyId"] for r in res_fos])

        foslevel = es_get_fos_level(list(fosset))
        fosmap = {f["_source"]["FieldOfStudyId"]:{"level": f["_source"]["Level"], "name": f["_source"]["DisplayName"]} for f in foslevel}
        # print(fosmap)

        for r in res_fos:
            fos_id = r["_source"]["FieldOfStudyId"]
            fos_score = r["_source"]["Similarity"]
            fos_name = fosmap[fos_id]["name"]
            if fosmap[fos_id]["level"] != 1:
                continue
            if fos_id in aggfos:
                aggfos[fos_id]["score"] += fos_score
            else:
                aggfos[fos_id] = {"score": fos_score, "name": fos_name}

    # print(aggfos)
    print("number of papers = ", totalp)
    aggfos = sorted(aggfos.values(), key=lambda x: x["score"], reverse=True)
    for v in aggfos:
        # print("{},{}".format(v["name"],v["score"]))
        if v["name"] not in yearmap:
            yearmap[v["name"]] = {}
        yearmap[v["name"]][range[0]] = v["score"]

# print(yearmap)
for k, v in yearmap.items():
    print(k, end=",")
    for range in ranges:
        print(v[range[0]] if range[0] in v else 0, end=",")
    print()
