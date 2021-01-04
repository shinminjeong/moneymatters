import os, csv, json, itertools
import numpy as np
from core.search import *
from core.conf_names import *
from core.core_utils import *
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
    # print(aff_info["DisplayName"], country_name)
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


def load_summary_file(cc):
    data = json.load(open("../data/conf_summary/{}_summary.json".format(cc), "r"))
    return data


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


def extract_conf_data_year(cc):
    confpapers = json.load(open("../data/conferences/{}_papers.json".format(cc), "r"))

    years = range(2008,2021,1)
    data = {}
    for y in years:
        fosscores = {}
        affscores = {}
        pcount  = 0
        for p in confpapers:
            if p["Year"] != y:
                continue
            pcount += 1
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
        data[y] = {
            "PaperCount": pcount,
            "FOS": fosscores,
            "PAA": affscores,
        }
    return data


AffiliationMap = {}
def generate_conf_summary_year(cc):
    global AffiliationMap
    print("generate_conf_summary_year", cc)
    data = extract_conf_data_year(cc)
    output = {}
    for year, ydata in data.items():
        affscores = ydata["PAA"]
        aff_total = sum([v for k, v in affscores.items()])
        aff_sorted = {k: 100*v/aff_total for k, v in sorted(affscores.items(), key=lambda item: item[1], reverse=True)}

        country_count = {}
        count_inthemap = 0
        count_search = 0
        for affid, value in aff_sorted.items():
            if affid in AffiliationMap:
                c_name = AffiliationMap[affid]
                count_inthemap += 1
            else:
                c_name = search_aff_country(affid)
                AffiliationMap[affid] = c_name
                count_search += 1

            if c_name not in country_count:
                country_count[c_name] = 0
            country_count[c_name] += value
        print("Map:{}, Search:{}".format(count_inthemap, count_search))
        print(cc, year, country_count.keys())
        output[year] = {
            "PaperCount": ydata["PaperCount"],
            "Countries": country_count
        }

    with open("../data/year_country/{}_year_country.json".format(cc), "w") as outfile:
        json.dump(output, outfile)


irregular_conf_map = {
    "AAAI": [2000, 2002, 2004, 2005, 2006, 2007, 2008] + list(range(2010, 2020)),
    "ACCV": [2000, 2002, 2006, 2007, 2009] + list(range(2010, 2020, 2)),
    "COLING": list(range(2000, 2020, 2)),
    "ICCV": list(range(1999, 2020, 2)),
    "ECCV": list(range(2000, 2020, 2)),
}


def generate_year_country_summary():
    csv_writer = csv.writer(open("../data/year_country_summary.csv", "w"))

    dir_path = "../data/year_country"
    fos_set = set()
    df_core = read_core_data()
    df_kiise = read_kiise_data()
    df_ccf = read_ccf_fos_data()
    for f in os.listdir(dir_path):
        print(f)
        data = json.load(open(os.path.join(dir_path, f), "r"))
        cc = f.split("_")[0]
        pre_rank1 = ""
        pre_rank2 = ""
        pre_rank3 = ""
        for y, v in data.items():
            if cc in irregular_conf_map and int(y) not in irregular_conf_map[cc]:
                continue

            citem = [cc, y, v["PaperCount"]]
            for country in ["United States", "Australia", "China", "South Korea"]:
                citem.append(v["PaperCount"]*v["Countries"][country]/100 if country in v["Countries"] else 0.0)
                citem.append(v["Countries"][country] if country in v["Countries"] else 0.0)

            # core
            df1 = df_core[(df_core["Year"] == "CORE{}".format(y)) & (df_core["Acronym"] == cc)][["Rank"]]
            rank1 = df1.values.tolist()[0][0] if len(df1.values.tolist()) == 1 else pre_rank1
            pre_rank1 = rank1

            # kiise
            df2 = df_kiise[(df_kiise["Year"] == int(y)) & (df_kiise["Acronym"] == cc)][["Rank"]]
            rank2 = df2.values.tolist()[0][0] if len(df2.values.tolist()) == 1 else pre_rank2
            pre_rank2 = rank2

            # ccf
            df3 = df_ccf[(df_ccf["Year"] == int(y)) & (df_ccf["Acronym"] == cc)][["Rank"]]
            rank3 = df3.values.tolist()[0][0] if len(df3.values.tolist()) == 1 else pre_rank3
            pre_rank3 = rank3

            citem.extend([rank1, rank2, rank3])
            csv_writer.writerow(citem)


# generate_year_country_summary()

# for cc in ai_conf.keys():
#     if os.path.exists("../data/year_country/{}_year_country.json".format(cc)):
#         print("File exists", cc)
#         continue
#     generate_conf_summary_year(cc)

# vec2d = create_emb_vector()
# with open("../app/emb_conf.json", "w") as outfile:
#     json.dump(vec2d, outfile)
