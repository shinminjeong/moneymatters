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

cache_path = "../data/NSF/cache"


def get_grant_coworknet_pis(grant_id, force=False):
    award = CleanedNSFAward(grant_id)
    award.generate_award_info(force=force)
    if force:
        award.normalize_investigator()
    award_info = award.get_award_info()
    time_s = datetime.strptime(award_info["startTime"], "%m/%d/%Y")
    time_e = datetime.strptime(award_info["endTime"], "%m/%d/%Y")
    G = award.generate_pi_G()
    nx.set_edge_attributes(G, grant_id, "grant")

    ptable = {}
    papers = {}
    pis = G.nodes
    newG = nx.MultiGraph()
    newG.add_nodes_from(G.nodes(data=True))
    for k, data in newG.nodes.data():
        data["pi"]=True

    cache_file = os.path.join(cache_path, str(award_info["year"]), "ptable", "{}_ptable.json".format(grant_id))
    if os.path.isfile(cache_file) and not force:
        ptable = json.load(open(cache_file, "r"))
    else:
        inters_grant = [p["paperId"] for p in award.get_award_publications() if "paperId" in p]
        for k, data in G.nodes.data():
            # print(k, data)
            authorid = data["id"]
            papers[authorid] = set(es_search_papers_from_aid(authorid))
        # print(papers)
        pi_intersections = set()
        for n1, n2, data in G.edges.data():
            n1_id = G.nodes[n1]["id"]
            n2_id = G.nodes[n2]["id"]
            # print(n1_id, n2_id, data)
            inters = papers[n1_id].intersection(papers[n2_id])
            pi_intersections = pi_intersections.union(inters)

        for paper in list(pi_intersections)+inters_grant:
            pinfo = es_search_paper_from_pid(paper)
            authors = es_search_authors_from_pid(paper)
            author_names = []
            author_list = {}
            for a in authors:
                oauthor = es_search_author_name(a["AuthorId"])
                author_names.append(oauthor["DisplayName"])
                if oauthor["DisplayName"] not in newG.nodes():
                    newG.add_node(oauthor["DisplayName"], norm=oauthor["NormalizedName"], id=a["AuthorId"],
                                paperCount=oauthor["PaperCount"], citationCount=oauthor["CitationCount"],
                                pi=True if oauthor["DisplayName"] in pis else False)
                author_list[oauthor["DisplayName"]] = {
                    "norm":oauthor["NormalizedName"], "id":a["AuthorId"],
                    "paperCount":oauthor["PaperCount"], "citationCount":oauthor["CitationCount"],
                    "affiliation":a["AffiliationId"],
                    "pi":True if oauthor["DisplayName"] in pis else False
                }

            ingrant = False
            if paper in inters_grant:
                ingrant = True
            if paper not in ptable:
                ptable[paper] = {
                    "year": pinfo["Year"],
                    "date": pinfo["date"], #datetime.strptime(pinfo["date"], "%Y-%m-%dT%X"),
                    "type": ingrant,
                    "citation": pinfo["CitationCount"],
                    "authors": author_list
                }
        json.dump(ptable, open(cache_file, "w"))

    for k, v in ptable.items():
        for n1, n2 in combinations(v["authors"], 2):
            for n in [n1, n2]:
                if n not in newG.nodes():
                    newG.add_node(n,
                        norm=v["authors"][n]["norm"],
                        id=v["authors"][n]["id"],
                        paperCount=v["authors"][n]["paperCount"],
                        citationCount=v["authors"][n]["citationCount"],
                        pi=v["authors"][n]["pi"])
            newG.add_edge(n1, n2, date=v["date"], citation=v["citation"], paper=k, grant=grant_id if v["type"] else "other")
            # print(n1, n2, pinfo["Year"], pinfo["date"], paper, ingrant)
    return award_info, ptable, time_s, time_e, newG


if __name__ == '__main__':
    # path = os.path.join("../data/NSF/cache", str(2009))
    # for filename in sorted(os.listdir(path)):
    #     award_id, file_format = filename.split(".")
    #     print(award_id)
    #     table, ts, te, G = get_grant_coworknet_pis(award_id, force=True)
        # break

    list_error = []
    l = []
    for award_id in l:
        try:
            print(award_id)
            _, table, ts, te, G = get_grant_coworknet_pis(award_id, force=True)
        except Exception as e:
            print("Exception: ", e, award_id)
            list_error.append(award_id)

    print(list_error)
