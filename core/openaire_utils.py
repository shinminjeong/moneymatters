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
    for id, code, year, inst, state, pis, code, amount in (df.loc[:, df_cols].head()).values:
        data[id] = {
            "id": id, "scheme": code, "year": year,
            "instutition": inst, "state": state,
            "investigators": pis.split(";"),
            "pri_code": code, "amount": amount
        }
    return data


def query_pub_outcome(award_id):
    ns = "{http://namespace.openaire.eu/oaf}"
    root = query_openaire(award_id)
    print(root.find("results").findall("result"))
    for child in root.find("results").findall("result"):
        pub = child.find("metadata").find("{}entity".format(ns)).find("{}result".format(ns))
        # print(pub)
        # .find("oaf:result", ns)
        print(pub.find("title").text)
        print([t.text for t in pub.findall("creator")])


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

if __name__ == '__main__':
    # print(query_openaire("DP140102185"))
    data = read_arc_grant_data()
    for k, v in data.items():
        print(k, v)
        query_pub_outcome("DP140102185")
        break
