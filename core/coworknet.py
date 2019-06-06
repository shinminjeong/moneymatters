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

    papers = {}
    for k, data in G.nodes.data():
        authorid = data["id"]
        papers[authorid] = set(es_search_papers_from_aid(authorid))
    # print(papers)
    for n1, n2, data in G.edges.data():
        n1_id = G.nodes[n1]["id"]
        n2_id = G.nodes[n2]["id"]
        # print(n1_id, n2_id)
        inters = papers[n1_id].intersection(papers[n2_id])
        for other_paper in inters:
            # print("** has edge?", G.has_edge(n1,n2), G[n1][n2]["paper"], other_paper)
            if G.has_edge(n1,n2) and G[n1][n2]["paper"] == other_paper:
                G.add_edge(n1, n2, paper="both", grant="both")
            else:
                G.add_edge(n1, n2, paper=other_paper, grant="other")
    return G


if __name__ == '__main__':
    path = os.path.join("../data/NSF/cache", str(2009))
    for filename in sorted(os.listdir(path)):
        award_id, file_format = filename.split(".")
        g = get_grant_coworknet_pis("0964497")
        # print(g.edges.data())
        print(g.nodes.data())
        papers = {}
        for k, data in g.nodes.data():
            authorid = data["id"]
            papers[authorid] = set(es_search_papers_from_aid(authorid))
        # print(papers)
        for n1, n2, data in g.edges.data():
            n1_id = g.nodes[n1]["id"]
            n2_id = g.nodes[n2]["id"]
            # print(n1_id, n2_id)
            inters = papers[n1_id].intersection(papers[n2_id])
            if len(list(inters)) > 1:
                print(award_id)
                print("'{}' and '{}' wrote papers {} for grant {}".format(n1, n2, data["paper"], data["grant"]))
                print("all papers they wrote together", inters)
                # break
        break
