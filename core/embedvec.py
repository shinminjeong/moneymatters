import os, csv, json, itertools
import numpy as np
from core.search import *
from core.conf_names import *
from collections import Counter
from datetime import datetime
from elasticsearch import Elasticsearch
from multiprocessing import Pool
from sklearn.decomposition import PCA
import umap

gridmap = None

def get_vector_score(cname, bov, author_fos, year=0, norm_ref=True):
    c = {fos:0 for fos in bov}
    for fos, score in author_fos:
        c[fos] += score
    author_arr = [float(c[b]) for b in bov]
    res_arr = np.array(author_arr)
    if year != 0:
        res_arr /= name_id_pairs[cname]["numpaper"][year]
    if norm_ref and len(author_fos) > 0:
        res_arr /= len(author_fos)
    return res_arr


def extract_conf_data(cc):
    confpapers = json.load(open("../data/conferences/{}_papers.json".format(cc), "r"))
    fosscores = {}
    affscores = {}
    for p in confpapers:
        if p["Year"] < 2010: # consider papers after 2010
            continue

        # calculate affiliation -- normalised by number of authors
        for paa in p["PAA"]:
            if "AffiliationId" not in paa:
                continue
            if paa["AffiliationId"] not in affscores:
                affscores[paa["AffiliationId"]] = 0
            affscores[paa["AffiliationId"]] += 1/len(p["PAA"])
        # calculate field of science score
        for fos in p["FOS"]:
            if fos["FieldOfStudyId"] not in fosscores:
                fosscores[fos["FieldOfStudyId"]] = 0
            fosscores[fos["FieldOfStudyId"]] += fos["Similarity"]
    return len(confpapers), fosscores, affscores

def get_country_from_gid(grid_id):
    global gridmap
    if gridmap == None:
        creader = csv.reader(open("../data/grid/grid.csv"))
        gridmap = {r[0]: r[4] for r in creader}
    return gridmap[grid_id] if grid_id and grid_id in gridmap else ""

def search_aff_country(aff_id):
    aff_info = es_search_affiliation_id(aff_id)
    country_name = get_country_from_gid(aff_info["GridId"])
    return country_name

def create_emb_vector():
    summary_path = "../data/conf_summary"
    fos_set = set()
    summary_files = os.listdir(summary_path)
    for sfile in [os.path.join(summary_path, f) for f in summary_files]:
        data = json.load(open(sfile, "r"))
        fos_set = fos_set.union(set(list(data["FOS"].keys())))
        print("reding...", sfile)
    print("fos_set", len(fos_set))
    fos_list = list(fos_set)

    vec = {}
    for sfile in [f for f in summary_files]:
        data = json.load(open(os.path.join(summary_path, sfile), "r"))
        confname = data["Acronym"]
        vec[confname] = get_vector(fos_list, data["FOS"].items(), data["PaperCount"])
    return reduce_vec_umap(vec, len(fos_list))

def get_vector(fos_set, conf_fos, paper_count):
    c = {fos:0 for fos in fos_set}
    for fos, score in conf_fos:
        c[fos] += score
    conf_arr = [float(c[b]) for b in fos_set]
    res_arr = np.array(conf_arr)
    if paper_count > 0: ## divide by conf papercount for normalisation
        res_arr /= paper_count
    return res_arr

def reduce_vec_umap(vec, number_of_venues):
    reducer = umap.UMAP()
    X = np.zeros((len(vec),number_of_venues))
    for i, v in enumerate(vec.values()):
        X[i] = v
    embedding = reducer.fit_transform(X)
    print(embedding.shape)
    result = {}
    for i, k in enumerate(vec.keys()):
        result[k] = embedding[i].tolist()
    return result


def generate_conf_summary(cc):
    print("generate_conf_summary", cc)
    numpapers, fosscores, affscores = extract_conf_data(cc)
    aff_total = sum([v for k, v in affscores.items()])
    aff_sorted = {k: 100*v/aff_total for k, v in sorted(affscores.items(), key=lambda item: item[1], reverse=True)}

    country_count = {}
    for affid, value in aff_sorted.items():
        c_name = search_aff_country(affid)
        if c_name not in country_count:
            country_count[c_name] = 0
        country_count[c_name] += value
    data = {
        "Acronym": cc,
        "PaperCount": numpapers,
        "Country": country_count,
        "FOS": fosscores
    }
    with open("../data/conf_summary/{}_summary.json".format(cc), "w") as outfile:
        json.dump(data, outfile)

# for cc in ml_conf.keys():
#     if os.path.exists("../data/conf_summary/{}_summary.json".format(cc)):
#         print("File exists", cc)
#         continue
#     generate_conf_summary(cc)

vec2d = create_emb_vector()
with open("../app/emb_conf.json", "w") as outfile:
    json.dump(vec2d, outfile)
