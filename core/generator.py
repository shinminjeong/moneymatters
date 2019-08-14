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
cache_path = "../data/NSF/cache"

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
        print("Error:", title, e)
        return rawstr, None, []
    return rawstr, paper_info, mag_authors


class CleanedNSFAward:
    def __init__(self, grant_id, thread_pool=None):
        self.thread_pool = thread_pool
        self.grant_id = grant_id


    def generate_award_info(self, mag_search=True, force=False):
        self.mag_search = mag_search
        year, id_str = get_grant_year_id(self.grant_id)
        cache_file = os.path.join(cache_path, str(year), "{}.json".format(id_str))
        if os.path.isfile(cache_file) and not force:
        # and (datetime.fromtimestamp(os.path.getmtime(cache_file))-datetime.now()).days < -3:
        # if False:
            # print("cache file exist", year, id_str)
            self.award = json.load(open(cache_file, "r"))
        else:
            self.award = {
                "year": year,
                "id": int(id_str),
                "id_str": id_str,
                "awardTitle": "",
                "awardInstrument": "",
                "awardAmount": 0,
                "directorate": "",
                "organization": "",
                "startTime": "",
                "endTime": "",
                "investigators": [],
                "institution": "",
                "numPublications": 0,
                "publicationResearch": [],
                "publicationConference": [],
            }
            self.read_grant_meta_info()
            self.read_grant_publications(mag_search=mag_search)

    def add_investigator(self, fname, lname, eaddr, role):
        self.award["investigators"].append({
            "firstName": fname,
            "lastName": lname,
            "emailAddress": eaddr,
            "role": role
        })

    def normalize_investigator(self):
        author_set = self.get_author_set()
        # print(author_set)
        for pi in self.award["investigators"]:
            fn = pi["firstName"].lower()
            ln = pi["lastName"].lower()
            candidates = []
            for author in author_set:
                # print("normalize_investigator", author)
                author_lower = author[2].lower()
                if fn in author_lower and ln in author_lower:
                    # both first and last name in the author list
                    candidates.append(author)
                elif fn[0] in [a[0] for a in author_lower.split(" ")] and ln in author_lower:
                    # first initial and last name in the author list
                    candidates.append(author)
            # print(count, same_author)
            if len(candidates) == 0:
                same_author = es_author_normalize("{} {}".format(fn, ln))
                pi["authorId"] = same_author["AuthorId"]
                pi["normalizedName"] = same_author["NormalizedName"]
                pi["displayName"] = same_author["DisplayName"]
                pi["paperCount"] = same_author["PaperCount"]
                pi["citationCount"] = same_author["CitationCount"]
            elif len(candidates) == 1:
                pi["authorId"] = candidates[0][0]
                pi["normalizedName"] = candidates[0][1]
                pi["displayName"] = candidates[0][2]
                pi["paperCount"] = candidates[0][3]
                pi["citationCount"] = candidates[0][4]
            else:
                same_author = sorted(candidates, key=lambda x: x[3], reverse=True)[0]
                pi["authorId"] = same_author[0]
                pi["normalizedName"] = same_author[1]
                pi["displayName"] = same_author[2]
                pi["paperCount"] = same_author[3]
                pi["citationCount"] = same_author[4]
            pi["candidates"] = candidates
            # print(pi)

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
                print("No investigator", self.award["id_str"])
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
        if paper_info:
            publication = {
                "raw_string": raw_str,
                "paperTitle": paper_info["PaperTitle"],
                "paperId": paper_info["PaperId"],
                "year": paper_info["Year"],
                "journalId": paper_info["JournalId"] if "JournalId" in paper_info else None,
                "conferenceId": paper_info["ConferenceSeriesId"] if "ConferenceSeriesId" in paper_info else None,
                "authors": [],
                "citationCount": paper_info["CitationCount"],
                "estCitation": paper_info["EstimatedCitation"]
            }
            for author in authors:
                publication["authors"].append({
                    "authorId": author["AuthorId"],
                    "normalizedName": author["NormalizedName"],
                    "displayName": author["DisplayName"],
                    "paperCount": author["PaperCount"],
                    "citationCount": author["CitationCount"],
                })
        else:
            publication = {
                "raw_string": raw_str,
                "authors": [],
            }
        self.award[pub_type].append(publication)

    def read_grant_publications(self, mag_search=True, title_printout=False):
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

                    if not mag_search:
                        continue
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
        self.award["numPublications"] = len(titles)
        self.save_award_info()

    def save_award_info(self):
        outfile = open(os.path.join(cache_path, str(self.award["year"]), "{}.json".format(self.award["id_str"])), 'w')
        json.dump(self.award, outfile)


    def get_award_info(self):
        if self.mag_search: # only save cache if it includes MAG ids
            self.save_award_info()
        return self.award

    def get_author_set(self):
        author_set = set()
        award = self.get_award_info()
        for pub_type in ["publicationResearch", "publicationConference"]:
            for pub in award[pub_type]:
                for a1 in pub["authors"]:
                    author_set.add((a1["authorId"], a1["normalizedName"], a1["displayName"], a1["paperCount"], a1["citationCount"]))
        return author_set

    def get_award_publications(self):
        award = self.get_award_info()
        pubs = []
        for pub_type in ["publicationResearch", "publicationConference"]:
            pubs.extend(award[pub_type])
        return pubs

    def generate_pi_G(self):
        G = nx.MultiGraph()
        award = self.get_award_info()
        pis = []
        for pi in award["investigators"]:
            # if pi["authorId"] in [a[0] for a in self.get_author_set()]:
            G.add_node(pi["displayName"], norm=pi["normalizedName"], id=pi["authorId"],
                            paperCount=pi["paperCount"], citationCount=pi["citationCount"])
            for c in pi["candidates"]:
                G.add_node(c[2], norm=c[1], id=c[0], paperCount=c[3], citationCount=c[4])
            pis.append(pi["authorId"])
            pis.extend([c[0] for c in pi["candidates"]])
        # print(pis)
        for pub_type in ["publicationResearch", "publicationConference"]:
            for pub in award[pub_type]:
                # print(pub["paperId"], pub["paperTitle"], pub["authors"])
                for a1, a2 in combinations(pub["authors"], 2):
                    if a1["authorId"] in pis and a2["authorId"] in pis:
                        G.add_edge(a1["displayName"], a2["displayName"], paper=pub["paperId"]),
        return G

    def generate_author_G(self):
        G = nx.Graph()
        award = self.get_award_info()
        for pub_type in ["publicationResearch", "publicationConference"]:
            for pub in award[pub_type]:
                for a1, a2 in combinations(pub["authors"], 2):
                    G.add_edge(a1["displayName"], a2["displayName"]),
        return G

    def get_investigator_names(self):
        return [i["displayName"] for i in self.award["investigators"]]

    def get_num_titles(self):
        num_pub_research = len(self.award["publicationResearch"])
        num_pub_conference = len(self.award["publicationConference"])
        return num_pub_research+num_pub_conference

    def print_award(self):
        print(json.dumps(self.award))
