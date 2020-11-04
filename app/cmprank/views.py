import os, sys, json
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from core.categories import *
from core.core_utils import *
from core.embedvec import load_summary_file

categories = {
    "CORE_New": {
        "name": "CORE New",
        "list": CORE_FoR.values(),
        "rank": ["A*", "A", "B", "C"]
    },
    "CORE_Legacy": {
        "name": "CORE Legacy",
        "list": CORE_FoR_Legacy.values(),
        "rank": ["A*", "A", "B", "C"]
    },
    "KIISE": {
        "name": "KIISE",
        "list": CORE_FoR.values(),
        "rank": ["S", "A"]
    },
    "CCF": {
        "name": "CCF",
        "list": CCF_FoR.values(),
        "rank": ["A", "B", "C"]
    },
}

rank_file = {
    "CORE_New": {
        "file": filenames[0], # core 2020
        "category": {v:k for k,v in CORE_FoR.items()}
    },
    "CORE_Legacy": {
        "file": filenames[1], # core 2018
        "category": {v:k for k,v in CORE_FoR_Legacy.items()}
    },
    "KIISE": {
        "file": kiise_fos_filenames[0], # 2018
        "category": {v:k for k,v in CORE_FoR.items()}
    },
    "CCF": {
        "file": ccf_filenames[0], # 2019
        "category": {v:k for k,v in CCF_FoR.items()}
    }
}


def main(request):
    data = json.loads(open("emb_conf.json").read())
    print(categories["CCF"])
    return render(request, "main.html", {
        "categories": categories,
        "input_data": data,
    })

@csrf_exempt
def get_conf_list(request):
    print("get_conf_list", request.POST)
    rank_type = request.POST.get("type")
    conf_category = request.POST.get("category")
    rank_above = request.POST.get("rank")

    rinfo = rank_file[rank_type]
    if rank_type == "CORE_New" or rank_type == "CORE_Legacy":
        clist = get_core_conflist(rinfo["file"], rinfo["category"][conf_category], rank_above)
    elif rank_type == "KIISE":
        clist = get_kiise_conflist(rinfo["file"], rinfo["category"][conf_category], rank_above)
    elif rank_type == "CCF":
        clist = get_ccf_conflist(rinfo["file"], conf_category, rank_above)
    gid = list(rank_file.keys()).index(rank_type)
    print(gid, clist)

    return JsonResponse({"gid": gid, "conflist": clist})

@csrf_exempt
def get_conf_info(request):
    print("get_conf_info", request.POST)
    conf_name = request.POST.get("name")
    cdata = load_summary_file(conf_name);
    countries = {k: v for k, v in sorted(cdata["Country"].items(), key=lambda item: item[1], reverse=True)}
    top10 = list(countries.items())[:10]
    return JsonResponse({"paperCount": cdata["PaperCount"], "topCountries": top10})
