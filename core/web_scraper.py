import os, json
from urllib import request
from bs4 import BeautifulSoup

NSF_URI = "https://www.nsf.gov/awardsearch/showAward"

def scrape_award_page(award_id):
    response = request.urlopen("{}?AWD_ID={}".format(NSF_URI, award_id))
    return response.read()

def get_paper_list(award_id):
    soup = BeautifulSoup(scrape_award_page(award_id), 'html.parser')
    p = soup.find('strong', string="BOOKS/ONE TIME PROCEEDING").parent
    for table in p('table'):
        table.extract()
    print(p.get_text())

    papers = []
    for p in p.get_text().split("\n"):
        p_strip = p.strip()
        if p_strip != "" and len(p_strip.split('"')) > 2 and ",&nbsp" != p_strip[:6]:
            print("[{}]".format(p_strip))
            paper_title = p_strip.split('"')[1]
            print("*", paper_title)
            papers.append((p_strip, paper_title))
    print(award_id, "books and one time proceeding =", len(papers))
    return papers

# get_paper_list("9711673")
# get_paper_list("9988637")
get_paper_list("0702240")
