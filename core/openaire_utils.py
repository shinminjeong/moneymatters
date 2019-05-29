import os,sys,json
import requests
from core.search import *
from collections import Counter
from datetime import datetime
from itertools import combinations
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import networkx as nx

openaire_pub_api_prefix = "http://api.openaire.eu/search/publications"
ARC_metadata = "../data/ARC/raw/arc_projects.csv"
pubs_path = "../data/ARC/pubs"

def read_arc_grant_data():
    df_cols = ["Project ID",
        "Scheme Code",
        "Commencement Year",
        "Administering Organisation",
        "State/ Territory",
        "Investigators",
        "Primary code type",
        "Primary FoR/RFCD code and description",
        "Funding Award"]
    # for y in range(2002, 2019):
    #     df_cols.append(str(y))
    # print(df_cols)
    data = {}
    df = pd.read_csv(ARC_metadata, encoding = "ISO-8859-1")
    for id, scheme, year, inst, state, pis, code, desc, amount in (df.loc[:, df_cols]).values:
        if type(id) == float:
            continue
        data[id] = {
            "id": id,
            "scheme": scheme.split(" ")[0] if type(pis) != float else None,
            "year": year,
            "instutition": inst, "state": state,
            "investigators": pis.split(";") if type(pis) != float else [],
            "pri_code": code,
            "div": desc[:2],
            "amount": amount.replace(",", "").split("$")[-1] if type(amount) != float else 0,
        }
        if data[id]["scheme"] == "CoE":
            data[id]["scheme"] = "CE"
        if data[id]["scheme"] == "FF":
            data[id]["scheme"] = "FT"
    return data


def query_pub_outcome(award_id):
    ns = "{http://namespace.openaire.eu/oaf}"
    root = query_openaire(award_id)
    print(root.find("results").findall("result"))
    data = []
    for child in root.find("results").findall("result"):
        pub = child.find("metadata").find("{}entity".format(ns)).find("{}result".format(ns))
        title = pub.find("title").text
        authors = [t.text for t in pub.findall("creator")]
        data.append((title, authors))
    return data


# get publication from project id
def query_openaire(award_id):
    payload = {"projectID": award_id}
    response = requests.get(openaire_pub_api_prefix, params=payload)
    # print('curl: ' + response.url)
    # print('return statue: ' + str(response.status_code))
    if response.status_code != 200:
        print("return statue: " + str(response.status_code))
        print("ERROR: problem with the request.")
        exit()
    return ET.fromstring(response.content)


def arc_grant_analysis(award_id, award_v):
    print(award_id)
    title = ""
    grant_type = award_v["scheme"]
    grant_amount = int(award_v["amount"])
    code = award_v["pri_code"]
    state = award_v["state"]
    year = award_v["year"]
    inst = award_v["instutition"]
    num_pi = len(award_v["investigators"])

    num_authors = []
    num_citations = []
    titles = []
    publications = query_pub_outcome(award_id)
    nun_pubs = len(publications)
    if nun_pubs > 0:
        for t, authors in publications:
            if t.lower() in titles:
                continue
            titles.append(t.lower())
            num_authors.append(len(authors))

            paper_info = es_search_paper_title(t)
            if paper_info:
                num_citations.append(paper_info["CitationCount"])
        if not num_authors:
            num_authors = [0]
        if not num_citations:
            num_citations = [0]
    else:
        num_authors = num_citations = [0]
    return year, award_id, state, num_pi, grant_type, grant_amount, nun_pubs, np.mean(num_authors), np.mean(num_citations)


def read_pub_outcome(root):
    ns = "{http://namespace.openaire.eu/oaf}"
    # print(root.find("results").findall("result"))
    data = []
    for child in root.find("results").findall("result"):
        pub = child.find("metadata").find("{}entity".format(ns)).find("{}result".format(ns))
        title = pub.find("title").text
        authors = [t.text for t in pub.findall("creator")]
        data.append((title, authors))
    return data


def count_pub_amount(year):
    all_grant_data = read_arc_grant_data()
    awards = dict()
    path = os.path.join(pubs_path, str(year))
    for filename in sorted(os.listdir(path)):
        award_id, file_format = filename.split(".")
        if file_format != "xml":
            continue
        try:
            root = ET.parse(os.path.join(path, filename)).getroot()
            data = read_pub_outcome(root)
            # print(data)
        except:
            # print("[ET parse error]", os.path.join(data_path, str(year), filename))
            continue
        award_id = award_id.replace("_", "/")
        awards[award_id] = {
            "type": all_grant_data[award_id]["scheme"],
            "amount": int(all_grant_data[award_id]["amount"]),
            "div": all_grant_data[award_id]["div"],
            "investigators": all_grant_data[award_id]["investigators"],
            "num_pubs": len(data),
            "authors": [a for t, a in data]
        }
    outfile = open(os.path.join(path, "numpub.json"), 'w')
    json.dump(awards, outfile)

def load_numpub_data(year):
    data = json.load(open(os.path.join(pubs_path, str(year), "numpub.json"), 'r'))
    return data

def download_publications(award_id, year):
    path = os.path.join(pubs_path, str(int(year)), "{}.xml").format(award_id.replace("/", "_"))
    if os.path.isfile(path):
        return
    print(award_id, year)
    tree = ET.tostring(query_openaire(award_id))
    # print(tree)
    f = open(path, "wb")
    f.write(tree)

def team_analysis(year):
    data = {}
    for id, value in load_numpub_data(year).items():
        G = nx.Graph()
        authors_set = value["authors"]
        for authors in authors_set:
            for a1, a2 in combinations(authors, 2):
                G.add_edge(a1, a2)
        ncc = nx.number_connected_components(G)
        # print(list(nx.connected_components(G)))
        data[id] = {
            "year": year,
            "amount": value["amount"],
            # "pis": pis,
            "num_pis": len(value["investigators"]),
            "num_cc": ncc,
            "num_pubs": value["num_pubs"],
            "team_size": [len(t) for t in list(nx.connected_components(G))] if ncc > 0 else [0]
        }
    return data

if __name__ == '__main__':
    # print(query_openaire("DP140102185"))
    # data = read_arc_grant_data()
    # for k, v in data.items():
    #     download_publications(k, v["year"])
        # print(arc_grant_analysis(k, v))
    # for y in range(2009, 2019):
    #     count_pub_amount(y)
    team_analysis(2009)
