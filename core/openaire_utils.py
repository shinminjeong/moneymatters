import os,sys,json
import requests
from core.search import *
from collections import Counter
from datetime import datetime
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET

openaire_pub_api_prefix = "http://api.openaire.eu/search/publications"
ARC_metadata = "../data/ARC/raw/arc_projects.csv"

def read_arc_grant_data():
    df_cols = ["Project ID",
        "Scheme Code",
        "Commencement Year",
        "Administering Organisation",
        "State/ Territory",
        "Investigators",
        "Primary code type",
        "Funding Award"]
    # for y in range(2002, 2019):
    #     df_cols.append(str(y))
    # print(df_cols)
    data = {}
    df = pd.read_csv(ARC_metadata, encoding = "ISO-8859-1")
    for id, scheme, year, inst, state, pis, code, amount in (df.loc[:, df_cols]).values:
        if type(id) == float:
            continue
        data[id] = {
            "id": id,
            "scheme": scheme.split(" ")[0] if type(pis) != float else None,
            "year": year,
            "instutition": inst, "state": state,
            "investigators": pis.split(";") if type(pis) != float else [],
            "pri_code": code,
            "amount": amount.replace(",", "").split("$")[-1] if type(amount) != float else 0,
        }
        if data[id]["scheme"] == "CoE":
            data[id]["scheme"] = "CE"
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



if __name__ == '__main__':
    # print(query_openaire("DP140102185"))
    data = read_arc_grant_data()
    for k, v in data.items():
        print(arc_grant_analysis(k, v))
