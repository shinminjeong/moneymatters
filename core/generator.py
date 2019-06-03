import os,sys,json
import requests
from core.search import *
from collections import Counter
from datetime import datetime
from itertools import combinations
from multiprocessing import Pool
import numpy as np
import xml.etree.ElementTree as ET
import networkx as nx

data_path = "../data/NSF/raw"

def get_grant_year_id(grant_id):
    gid = int(grant_id)
    year_2digit = int(gid/100000)
    # print(year_2digit)
    if year_2digit < 20:
        year = 2000+year_2digit
    else:
        year = 1900+year_2digit
    award_id = "{:07d}".format(gid)
    return year, award_id


def get_paper_information(data):
    rawstr, title = data
    # print("[get_paper_information]", title)
    try:
        paper_info = es_search_paper_title(title)
        authors = es_search_authors_from_pid(paper_info["PaperId"])
        mag_authors = []
        for au in authors:
            author = es_search_author_name(au["AuthorId"])
            mag_authors.append(author)
    except Exception as e:
        print("Error:", title)
        return None, []
    return rawstr, paper_info, mag_authors


class CleanedNSFAward:
    def __init__(self, grant_id, thread_pool=None):
        self.thread_pool = thread_pool
        year, id_str = get_grant_year_id(grant_id)
        self.award = {
            "year": year,
            "id": int(id_str),
            "id_str": id_str,
            "awardTitle": "",
            "awardInstrument": "",
            "awardAmount": 0,
            "organization": "",
            "startTime": "",
            "endTime": "",
            "investigators": [],
            "institution": "",
            "publicationResearch": [],
            "publicationConference": [],
        }

    def add_investigator(self, fname, lname, eaddr, role):
        author = es_author_normalize(fname, lname)
        # print("[add_investigator]", fname, lname, author)
        self.award["investigators"].append({
            "firstName": fname,
            "lastName": lname,
            "emailAddress": eaddr,
            "role": role,
            "authorId": author["AuthorId"],
            "normalizedName": author["NormalizedName"],
            "displayName": author["DisplayName"],
        })

    def read_grant_meta_info(self):
        path = os.path.join(data_path, str(self.award["year"]), "{}.xml".format(self.award["id_str"]))
        try:
            root = ET.parse(path).getroot()
            award = grant_type = root.find("Award")
            self.award["awardTitle"] = award.find("AwardTitle").text
            self.award["awardInstrument"] = award.find("AwardInstrument").find("Value").text
            self.award["awardAmount"] = int(award.find("AwardAmount").text)
            self.award["organization"] = award.find("Organization").find("Code").text
            self.award["startTime"] = award.find("AwardEffectiveDate").text
            self.award["endTime"] = award.find("AwardExpirationDate").text
            inst = award.find("Institution")
            if inst:
                self.award["institution"] = inst.find("Name").text
            else:
                self.award["institution"] = ""

            investigators = award.findall("Investigator")
            if not investigators:
                print("No investigator", award_id)
                pass
            for investigator in investigators:
                inv_fname = inv_lname = inv_eaddr = inv_role = ""
                inv_fname = investigator.find("FirstName").text
                inv_lname = investigator.find("LastName").text
                inv_eaddr = investigator.find("EmailAddress").text
                inv_role = investigator.find("RoleCode").text
                self.add_investigator(inv_fname, inv_lname, inv_eaddr, inv_role)

        except Exception as e:
            print("[ET parse error]", e, path)
            pass

    def add_publication(self, pub_type, raw_str, paper_info, authors):
        # print(raw_str)
        # print(paper_info["PaperTitle"], paper_info["Year"])
        publication = {
            "raw_string": raw_str,
            "paperTitle": paper_info["PaperTitle"],
            "paperId": paper_info["PaperId"],
            "year": paper_info["Year"],
            "journalId": paper_info["JournalId"] if "JournalId" in paper_info else None,
            "conferenceId": paper_info["ConferenceSeriesId"] if "ConferenceSeriesId" in paper_info else None,
            "authors": []
        }
        for author in authors:
            publication["authors"].append({
                "authorId": author["AuthorId"],
                "normalizedName": author["NormalizedName"],
                "displayName": author["DisplayName"],
            })
        self.award[pub_type].append(publication)

    def read_grant_publications(self, title_printout=False):
        path = os.path.join(data_path, str(self.award["year"]), "{}.json".format(self.award["id_str"]))
        publications = json.load(open(path, "r"))
        titles = []
        self.dup_title = 0
        if publications["response"]["award"]:
            for pub_type in ["publicationResearch", "publicationConference"]:
                if pub_type in publications["response"]["award"][0]:
                    pubs = publications["response"]["award"][0][pub_type]
                    original_titles = []
                    for pub_str in pubs:
                        pinfo = pub_str.split("~")
                        authors_str = pinfo[0]
                        title = pinfo[1]
                        venue = pinfo[2]
                        version = pinfo[3]
                        pyear = pinfo[4]
                        norm_title = title.lower().replace(".", "").replace(" ", "")
                        if norm_title in titles:
                            self.dup_title += 1
                            continue
                        titles.append(norm_title)
                        original_titles.append((pub_str, title.lower()))

                    if self.thread_pool != None:
                        searched_papers = self.thread_pool.map(get_paper_information, original_titles)
                        for pub_str, paper_info, mag_authors in searched_papers:
                            if title_printout:
                                print([a["DisplayName"] for a in mag_authors], paper_info["PaperTitle"])
                            self.add_publication(pub_type, pub_str, paper_info, mag_authors)
                    else:
                        for ori_title in original_titles:
                            pub_str, paper_info, mag_authors = get_paper_information(ori_title)
                            if title_printout:
                                print([a["DisplayName"] for a in mag_authors], paper_info["PaperTitle"])
                            self.add_publication(pub_type, pub_str, paper_info, mag_authors)

    def generate_G(self):
        G = nx.Graph()
        for pub_type in ["publicationResearch", "publicationConference"]:
            for pub in self.award[pub_type]:
                for a1, a2 in combinations(pub["authors"], 2):
                    G.add_edge(a1["displayName"], a2["displayName"])
        return G

    def get_investigator_names(self):
        return [i["displayName"] for i in self.award["investigators"]]

    def get_num_titles(self):
        num_pub_research = len(self.award["publicationResearch"])
        num_pub_conference = len(self.award["publicationConference"])
        return self.dup_title, num_pub_research+num_pub_conference

    def print_award(self):
        print(json.dumps(self.award))
