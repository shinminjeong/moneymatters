import os,sys,json
import requests
from core.search import *
from collections import Counter
from datetime import datetime
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET

data_path = "../data/NSF/raw"
outf_path = "../data/NSF/summary"

nsf_api_prefix = "https://api.nsf.gov/services/v1/awards/"
nsf_grant_div_name = {
"010": "Office of the Director",
"020": "Office of Information &amp; Resource Mgmt",
"030": "Directorate for Mathematical &amp; Physical Scien",
"040": "Directorate for Social, Behav &amp; Economic Scie",
"050": "Directorate for Computer &amp; Info Scie &amp; Enginr",
"060": "Directorate for Geosciences",
"070": "Directorate for Engineering",
"080": "Directorate for Biological Sciences",
"100": "Office of Budget, Finance, &amp; Award Management",
"110": "Directorate for Education and Human Resource",
"120": "National Coordination Office"
}

def search_grant_data(query, years):
    # query = {key1: value, key2: value}
    data = []
    for year in years:
        path = os.path.join(outf_path, "summary_{}.csv".format(year))
        data.append(pd.read_csv(path, header=0))
    o_df = df = pd.concat(data, ignore_index = True)
    for k, v in query.items():
        df = df[(df[k] == v)]
    return o_df, df


def read_xml_files(years):
    df_cols = ["year", "id", "title", "type", "amount", "code", "start", "end", "firstname", "lastname", "email", "role", "institution"]

    for year in years:
        out_df = pd.DataFrame(columns = df_cols)
        path = os.path.join(data_path, str(year))

        for filename in sorted(os.listdir(path)):
            award_id, file_format = filename.split(".")
            if file_format == "json":
                continue
            try:
                root = ET.parse(os.path.join(data_path, str(year), filename)).getroot()
            except:
                # print("[ET parse error]", os.path.join(data_path, str(year), filename))
                pass
            award = grant_type = root.find("Award")
            title = award.find("AwardTitle").text
            grant_type = award.find("AwardInstrument").find("Value").text
            grant_amount = int(award.find("AwardAmount").text)
            code = award.find("Organization").find("Code").text
            grant_start = award.find("AwardEffectiveDate").text
            grant_end = award.find("AwardExpirationDate").text

            inst = award.find("Institution")
            if inst:
                inst_name = inst.find("Name").text
            else:
                inst_name = ""
                print("No institution name", award_id)


            investigators = award.findall("Investigator")
            if not investigators:
                print("No investigator", award_id)
                continue
            for investigator in investigators:
                inv_fname = inv_lname = inv_eaddr = inv_role = ""
                inv_fname = investigator.find("FirstName").text
                inv_lname = investigator.find("LastName").text
                inv_eaddr = investigator.find("EmailAddress").text
                inv_role = investigator.find("RoleCode").text

                # df_cols = ["year", "id", "title", "type", "amount", "code", "start", "end", "firstname", "lastname", "email", "role", "institution"]
                data = [year, award_id, title, grant_type, grant_amount, code, grant_start, grant_end, inv_fname, inv_lname, inv_eaddr, inv_role, inst_name]
                out_df = out_df.append(pd.Series(data, index = df_cols), ignore_index = True)

        out_df.to_csv(os.path.join(outf_path, "summary_{}.csv".format(year)), sep=',', index=None, header=True)


def num_grants_each_year(years):
    # data = []
    for year in years:
        data = []
        path = os.path.join(outf_path, "summary_{}.csv".format(year))
        data.append(pd.read_csv(path, header=0))
        concat_df = pd.concat(data, ignore_index = True)

        groups = concat_df.groupby(["firstname", "lastname", "email"])
        num_grants = []
        for name, value in groups:
            num_grants.append(value.shape[0])
            # if value.shape[0] > 1:
            #     print(name)
            #     print(value.loc[:, ["year", "type", "amount", "code"]])
        cnt = Counter(num_grants)
        print(year, end =",")
        for i in range(1, 10, 1):
            print(cnt[i], end =",")
        print()


def num_people_num_grants(years):
    data = []
    for year in years:
        path = os.path.join(outf_path, "summary_{}.csv".format(year))
        data.append(pd.read_csv(path, header=0))
    concat_df = pd.concat(data, ignore_index = True)

    print("total number of grants", concat_df["id"].count())
    groups = concat_df.groupby(["firstname", "lastname", "email"])
    print("total number of people", groups["firstname", "lastname", "email"].count())

    num_grants = []
    for name, value in groups:
        num_grants.append(value.shape[0])
        # if value.shape[0] > 1:
        #     print(name)
        #     print(value.loc[:, ["year", "type", "amount", "code"]])
    print(len(num_grants))
    cnt = Counter(num_grants)
    for i in range(1, 31, 1):
        print(i, ", ", cnt[i])


def num_pi_each_year(years):
    for year in years:
        data = []
        path = os.path.join(outf_path, "summary_{}.csv".format(year))
        data.append(pd.read_csv(path, header=0))
        concat_df = pd.concat(data, ignore_index = True)

        groups = concat_df.groupby(["year", "id", "title", "amount"])
        # print("total number of award", groups["year", "id", "title", "amount"].count())

        num_grants = []
        amount_single_stnd = []
        amount_single_cont = []
        amount_single_other = []
        amount_multiple_same_stnd = []
        amount_multiple_same_cont = []
        amount_multiple_same_other = []
        amount_multiple_diff_stnd = []
        amount_multiple_diff_cont = []
        amount_multiple_diff_other = []
        for name, value in groups:
            num_grants.append(value.shape[0])
            grant_amount = value.amount.values[0]
            if value.shape[0] == 1:
                if value.type.values[0] == "Standard Grant":
                    amount_single_stnd.append(grant_amount)
                elif value.type.values[0] == "Continuing grant":
                    amount_single_cont.append(grant_amount)
                else:
                    amount_single_other.append(grant_amount)
            else:
                emails = [str(e).split("@")[-1] for e in value.email.values]
                # print(emails)
                if len(set(emails)) == 1:
                    if value.type.values[0] == "Standard Grant":
                        amount_multiple_same_stnd.append(grant_amount)
                    elif value.type.values[0] == "Continuing grant":
                        amount_multiple_same_cont.append(grant_amount)
                    else:
                        amount_multiple_same_other.append(grant_amount)
                else:
                    if value.type.values[0] == "Standard Grant":
                        amount_multiple_diff_stnd.append(grant_amount)
                    elif value.type.values[0] == "Continuing grant":
                        amount_multiple_diff_cont.append(grant_amount)
                    else:
                        amount_multiple_diff_other.append(grant_amount)

        # print(len(num_grants))
        # print(len(amount_single_stnd), len(amount_single_cont), len(amount_single_other))
        print(year, ",", len(amount_single_stnd), ",", len(amount_single_cont), ",",
            len(amount_multiple_same_stnd), ",", len(amount_multiple_same_cont), ",",
            len(amount_multiple_diff_stnd), ",", len(amount_multiple_diff_cont),
            ",", sum(amount_single_stnd)/len(amount_single_stnd), ",", sum(amount_single_cont)/len(amount_single_cont),
            ",", sum(amount_multiple_same_stnd)/len(amount_multiple_same_stnd), ",", sum(amount_multiple_same_cont)/len(amount_multiple_same_cont),
            ",", sum(amount_multiple_diff_stnd)/len(amount_multiple_diff_stnd), ",", sum(amount_multiple_diff_cont)/len(amount_multiple_diff_cont))

def num_pi_each_year_2(years):
    for year in years:
        data = []
        path = os.path.join(outf_path, "summary_{}.csv".format(year))
        data.append(pd.read_csv(path, header=0))
        concat_df = pd.concat(data, ignore_index = True)

        groups = concat_df.groupby(["year", "id", "title", "amount"])
        # print("total number of award", groups["year", "id", "title", "amount"].count())

        num_grants = []
        amount_single = []
        amount_multiple_same = []
        amount_multiple_diff = []
        for name, value in groups:
            num_grants.append(value.shape[0])
            grant_amount = value.amount.values[0]
            if value.shape[0] == 1:
                amount_single.append(value.shape[0])
            else:
                emails = [str(e).split("@")[-1] for e in value.email.values]
                # print(emails)
                if len(set(emails)) == 1:
                    amount_multiple_same.append(value.shape[0])
                else:
                    amount_multiple_diff.append(value.shape[0])

        # print(len(num_grants))
        # print(len(amount_single_stnd), len(amount_single_cont), len(amount_single_other))
        print(year,
            ",", sum(amount_single)/len(amount_single),
            ",", sum(amount_multiple_same)/len(amount_multiple_same),
            ",", sum(amount_multiple_diff)/len(amount_multiple_diff))


def parse_publication(years):
    df_cols = ["year", "id", "authors", "title", "venue", "paper_year"]

    for year in years:
        out_df = pd.DataFrame(columns = df_cols)
        path = os.path.join(data_path, str(year))
        for filename in sorted(os.listdir(path)):
            award_id, file_format = filename.split(".")
            if file_format == "xml":
                continue
            print(award_id)
            try:
                publications = json.load(open(os.path.join(path, filename), "r"))
                if publications["response"]["award"]:
                    pubs = publications["response"]["award"][0]["publicationResearch"]
                    for p in pubs:
                        pinfo = p.split("~")
                        authors = pinfo[0]
                        title = pinfo[1]
                        venue = pinfo[2]
                        version = pinfo[3]
                        pyear = pinfo[4]
                        # print(authors, title, venue, pyear)

                        data = [year, award_id, authors, title, venue, pyear]
                        out_df = out_df.append(pd.Series(data, index = df_cols), ignore_index = True)
            except Exception as e:
                print("[Error]", e)

        out_df.to_csv(os.path.join(outf_path, "publication_{}.csv".format(year)), sep=',', index=None, header=True)


def num_publication_each_year(years):
    # data = []
    for year in years:
        sum_data = pd.read_csv(os.path.join(outf_path, "summary_{}.csv".format(year)))
        pub_data = pd.read_csv(os.path.join(outf_path, "publication_{}.csv".format(year)))
        # print(sum_data.id, pub_data.id)
        dtype = dict(id=str)
        concat_df = pd.merge(left=sum_data.astype(dtype), right=pub_data.astype(dtype), how="left", on="id")
        # print(concat_df)
        groups = concat_df.groupby(["year_x", "id", "title_x", "amount"])

        amount_single = []
        amount_multiple_same = []
        amount_multiple_diff = []
        for name, value in groups:
            papers = []
            for p in value.title_y.values:
                if str(p) != "nan":
                    papers.append(p.lower())
            num_pis = len(set(value.email.values))
            num_papers = len(set(papers)) # remove duplicated papers
            # num_papers = len(papers)/num_pis # include duplicated papers

            # grant_amount = value.amount.values[0]
            if num_pis == 1:
                amount_single.append(num_papers)
            else:
                emails = [str(e).split("@")[-1] for e in list(set(value.email.values))]
                # print(emails)
                if len(set(emails)) == 1:
                    amount_multiple_same.append(num_papers)
                else:
                    amount_multiple_diff.append(num_papers)

        # print(len(num_grants))
        # print(len(amount_single_stnd), len(amount_single_cont), len(amount_single_other))
        print(year,
            ",", sum(amount_single)/len(amount_single),
            ",", sum(amount_multiple_same)/len(amount_multiple_same),
            ",", sum(amount_multiple_diff)/len(amount_multiple_diff))


def check_files(years):
    for year in years:
        path = os.path.join(data_path, str(year))
        xml_files = []
        json_files = []
        for filename in sorted(os.listdir(path)):
            award_id, file_format = filename.split(".")
            if file_format == "xml":
                xml_files.append(award_id)
            else:
                json_files.append(award_id)
        print(len(xml_files), len(json_files))


def count_numgrant_year(years):
    yamt = {y:{} for y in years}
    ymap = {y:[] for y in years}
    for year in years:
        path = os.path.join(data_path, str(year))
        for filename in sorted(os.listdir(path)):
            award_id, file_format = filename.split(".")
            if file_format == "json":
                continue
            try:
                root = ET.parse(os.path.join(data_path, str(year), filename)).getroot()
            except:
                # print("[ET parse error]", os.path.join(data_path, str(year), filename))
                pass
            grant_type = root.find("Award").find("AwardInstrument").find("Value").text
            grant_amount = int(root.find("Award").find("AwardAmount").text)
            ymap[year].append(grant_type)
            if grant_type in yamt[year]:
                yamt[year][grant_type] += grant_amount
            else:
                yamt[year][grant_type] = 0

    with open(os.path.join(data_path, "summary.json"), 'w') as outfile:
        data = {y:{"count":Counter(values), "amount":yamt[y]} for y, values in ymap.items()}
        json.dump(data, outfile)

def count_numgrant_division_year(years):
    yamt = {y:{} for y in years}
    ymap = {y:[] for y in years}
    for year in years:
        path = os.path.join(data_path, str(year))
        for filename in sorted(os.listdir(path)):
            award_id, file_format = filename.split(".")
            if file_format == "json":
                continue
            try:
                root = ET.parse(os.path.join(data_path, str(year), filename)).getroot()
            except:
                # print("[ET parse error]", os.path.join(data_path, str(year), filename))
                pass
            grant_type = root.find("Award").find("AwardInstrument").find("Value").text
            if grant_type != "Continuing grant":
            # if grant_type != "Standard Grant":
                continue
            code = root.find("Award").find("Organization").find("Code").text[:3]
            grant_div_type = nsf_grant_div_name[code] if code in nsf_grant_div_name else "Other"
            grant_amount = int(root.find("Award").find("AwardAmount").text)
            ymap[year].append(grant_div_type)
            if grant_div_type in yamt[year]:
                yamt[year][grant_div_type] += grant_amount
            else:
                yamt[year][grant_div_type] = 0

    with open(os.path.join(data_path, "summary_cont_div.json"), 'w') as outfile:
    # with open(os.path.join(data_path, "summary_stnd_div.json"), 'w') as outfile:
        data = {y:{"count":Counter(values), "amount":yamt[y]} for y, values in ymap.items()}
        json.dump(data, outfile)


def load_grant_data(filename):
    data = json.load(open(os.path.join(data_path, filename), 'r'))
    return data


# return year, id from grant_id
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

# download publication of a list of grants
def download_pub_grant(grant_ids):
    for gid in grant_ids:
        year, award_id = get_grant_year_id(gid)
        path = os.path.join(data_path, str(year))
        print(award_id)
        rsp = query_nsf(award_id)
        outfile = open(os.path.join(path, "{}.json".format(award_id)), 'w')
        json.dump(rsp, outfile)


# download all publications of year
def download_pub(years):
    for year in years:
        path = os.path.join(data_path, str(year))
        for filename in sorted(os.listdir(path)):
            award_id, file_format = filename.split(".")
            if file_format == "json" or os.path.isfile(os.path.join(path, "{}.json".format(award_id))):
            # if file_format == "json":
                continue
            print(award_id)
            rsp = query_nsf(award_id)
            outfile = open(os.path.join(path, "{}.json".format(award_id)), 'w')
            json.dump(rsp, outfile)

# get publicationResearch and publicationConference from grant id
def query_nsf(award_id):
    payload = {"printFields": "publicationResearch,publicationConference"}
    response = requests.get("{}{}.json".format(nsf_api_prefix, award_id), params=payload)
    # print('curl: ' + response.url)
    # print('return statue: ' + str(response.status_code))
    if response.status_code != 200:
        print("return statue: " + str(response.status_code))
        print("ERROR: problem with the request.")
        exit()
    return json.loads((response.content).decode("utf-8"))


def count_pub_amount(year):
    awards = dict()
    path = os.path.join(data_path, str(year))
    for filename in sorted(os.listdir(path)):
        award_id, file_format = filename.split(".")
        if file_format == "json":
            continue
        try:
            root = ET.parse(os.path.join(data_path, str(year), filename)).getroot()
        except:
            # print("[ET parse error]", os.path.join(data_path, str(year), filename))
            continue
        grant_type = root.find("Award").find("AwardInstrument").find("Value").text
        grant_amount = int(root.find("Award").find("AwardAmount").text)
        grant_start = datetime.strptime(root.find("Award").find("AwardEffectiveDate").text, "%m/%d/%Y")
        grant_end = datetime.strptime(root.find("Award").find("AwardExpirationDate").text, "%m/%d/%Y")
        code = root.find("Award").find("Organization").find("Code").text[:3]
        grant_div_type = nsf_grant_div_name[code] if code in nsf_grant_div_name else "Other"

        num_pubs = 0
        pubs = json.load(open(os.path.join(path, "{}.json".format(award_id)), 'r'))
        if "award" not in pubs["response"]:
            print("[No award error]", award_id)
            continue
        if len(pubs["response"]["award"]) > 0:
            num_pubs = len(pubs["response"]["award"][0]["publicationResearch"])
        awards[award_id] = {
            "type": grant_type,
            "amount": grant_amount,
            "div": grant_div_type,
            "num_pubs": num_pubs,
            "duration": (grant_end-grant_start).days
        }
    outfile = open(os.path.join(path, "numpub.json"), 'w')
    json.dump(awards, outfile)


def load_numpub_data(year):
    data = json.load(open(os.path.join(data_path, str(year), "numpub.json"), 'r'))
    return data


def grant_analysis(grant_id):
    year, award_id = get_grant_year_id(grant_id)
    path = os.path.join(data_path, str(year), award_id)
    # print(path)
    try:
        # read grant data
        root = ET.parse("{}.xml".format(path)).getroot()
        award = grant_type = root.find("Award")
        title = award.find("AwardTitle").text
        grant_type = award.find("AwardInstrument").find("Value").text
        grant_amount = int(award.find("AwardAmount").text)
        code = award.find("Organization").find("Code").text
        grant_start = award.find("AwardEffectiveDate").text
        grant_end = award.find("AwardExpirationDate").text

        inst = award.find("Institution")
        if inst:
            inst_name = inst.find("Name").text
        else:
            inst_name = ""
            print("No institution name", award_id)

        # print([award_id, title, grant_type, grant_amount, code, grant_start, grant_end, inst_name])

        num_pi = 0
        investigators = award.findall("Investigator")
        if not investigators:
            print("No investigator", award_id)
        else:
            for investigator in investigators:
                inv_fname = inv_lname = inv_eaddr = inv_role = ""
                inv_fname = investigator.find("FirstName").text
                inv_lname = investigator.find("LastName").text
                inv_eaddr = investigator.find("EmailAddress").text
                inv_role = investigator.find("RoleCode").text
                num_pi += 1
                # print([inv_fname, inv_lname, inv_role])

        # get publication data
        num_authors = []
        num_citations = []
        nun_pubs = 0
        titles = []
        publications = json.load(open("{}.json".format(path), "r"))
        if publications["response"]["award"]:
            pubs = publications["response"]["award"][0]["publicationResearch"]
            for p in pubs:
                pinfo = p.split("~")
                authors = pinfo[0]
                title = pinfo[1]
                venue = pinfo[2]
                version = pinfo[3]
                pyear = pinfo[4]
                if title.lower() in titles:
                    continue
                titles.append(title.lower())
                nun_pubs += 1
                # print(authors, title, venue, pyear)
                num_authors.append(len(authors.split(",")))
                # print(year, title, "number of authors =", len(authors.split(",")))
                paper_info = es_search_paper_title(title)
                num_citations.append(paper_info["CitationCount"])

            if "publicationConference" in publications["response"]["award"][0]:
                pubs = publications["response"]["award"][0]["publicationConference"]
                for p in pubs:
                    pinfo = p.split("~")
                    authors = pinfo[0]
                    title = pinfo[1]
                    venue = pinfo[2]
                    version = pinfo[3]
                    pyear = pinfo[4]
                    if title.lower() in titles:
                        continue
                    nun_pubs += 1
                    titles.append(title.lower())
                    # print(authors, title, venue, pyear)
                    num_authors.append(len(authors.split(",")))
                    # print(year, title, "number of authors =", len(authors.split(",")))
                    paper_info = es_search_paper_title(title)
                    num_citations.append(paper_info["CitationCount"])
            if not num_authors:
                num_authors = [0]
            if not num_citations:
                num_citations = [0]
        else:
            num_authors = num_citations = [0]

        # print('{},{},"{}",{},{},{},{},{},{},{},{}'.format(year, award_id, title, num_pi, grant_type, grant_amount,
        #     nun_pubs, np.mean(num_authors), np.median(num_authors), np.mean(num_citations), np.median(num_citations)))
        return year, award_id, title, num_pi, grant_type, grant_amount, nun_pubs, np.mean(num_authors), np.mean(num_citations)
    except Exception as e:
        print("[Error]", e)
        return None



if __name__ == '__main__':
    years = range(2000, 2020, 1)
    count_numgrant_division_year(years)
    # count_numgrant_year(years)
    # download_pub([2013, 2017])
    # t_hosking = [509377, 540866, 551658, 702240, 720505, 722210, 811691, 1042905, 1347630, 1405939, 1408896, 1549774, 1832624, 1832624, 1833291]
    # h_jagadish = [2356, 75447, 85945, 208852, 219513, 239993, 303587, 438909, 741620, 808824, 903629, 915782, 1017149, 1017296, 1250880, 1741022]
    # download_pub_grant(h_jagadish)
    # for g in t_hosking:
    #     grant_analysis(g)
