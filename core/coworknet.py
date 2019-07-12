import os,sys,json
import requests
from core.search import *
from core.generator import *
from collections import Counter
from datetime import datetime
from itertools import combinations
from multiprocessing import Pool
import numpy as np
import xml.etree.ElementTree as ET
import networkx as nx

def get_grant_coworknet_pis(grant_id):
    award = CleanedNSFAward(grant_id)
    award.generate_award_info()
    award.normalize_investigator()
    award_info = award.get_award_info()
    time_s = datetime.strptime(award_info["startTime"], "%m/%d/%Y")
    time_e = datetime.strptime(award_info["endTime"], "%m/%d/%Y")
    G = award.generate_pi_G()
    nx.set_edge_attributes(G, grant_id, "grant")

    ptable = {}
    papers = {}
    for k, data in G.nodes.data():
        authorid = data["id"]
        papers[authorid] = set(es_search_papers_from_aid(authorid))
    # print(papers)
    for n1, n2, data in G.edges.data():
        n1_id = G.nodes[n1]["id"]
        n2_id = G.nodes[n2]["id"]
        # print(n1_id, n2_id, data)
        inters = papers[n1_id].intersection(papers[n2_id])
        inters_grant = [g["paper"] for g in G[n1][n2].values()]
        for paper in inters:
            pinfo = es_search_paper_from_pid(paper)
            ingrant = False
            if paper in inters_grant:
                ingrant = True
            if paper in ptable:
                ptable[paper]["authors"].add(n1)
                ptable[paper]["authors"].add(n2)
            else:
                ptable[paper] = {
                    "year": pinfo["Year"],
                    "date": datetime.strptime(pinfo["date"], "%Y-%m-%dT%X"),
                    "type": ingrant,
                    "authors": set([n1, n2])
                }

    newG = nx.MultiGraph()
    for k, v in ptable.items():
        for n1, n2 in combinations(v["authors"], 2):
            newG.add_edge(n1, n2, paper=k, grant=grant_id if v["type"] else "other")
            # print(n1, n2, pinfo["Year"], pinfo["date"], paper, ingrant)
    return ptable, time_s, time_e, newG


if __name__ == '__main__':
    path = os.path.join("../data/NSF/cache", str(2004))
    for filename in sorted(os.listdir(path)):
        award_id, file_format = filename.split(".")
        table, ts, te = get_grant_coworknet_pis("0427794")
        print("--------------------")
        print(ts, te)
        print(table)
        break
