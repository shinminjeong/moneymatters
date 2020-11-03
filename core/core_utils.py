import os,sys,csv
import pandas as pd

filenames = [
    "../data/CORE/conf2020.csv",
    "../data/CORE/conf2018.csv",
    "../data/CORE/conf2017.csv",
    "../data/CORE/conf2014.csv",
    "../data/CORE/conf2013.csv",
    "../data/CORE/conf2010.csv",
    "../data/CORE/conf2008.csv"
]
kiise_filenames = [
    "../data/KIISE/conf2018.csv",
    "../data/KIISE/conf2016.csv",
    "../data/KIISE/conf2014.csv",
]
kiise_fos_filenames = [
    "../data/KIISE/conf2018_fos.csv",
    "../data/KIISE/conf2016_fos.csv",
    "../data/KIISE/conf2014_fos.csv",
]
ccf_filenames = [
    "../data/CCF/ccf-2019.csv",
    "../data/CCF/ccf-2015.csv",
    "../data/CCF/ccf-2012.csv",
]

def read_core_data():
    colnames = ["ID", "Title", "Acronym", "Year", "Rank", "Data", "FoR1", "FoR2", "FoR3"]
    data = []
    for filename in filenames:
        newf = pd.read_csv(filename, names=colnames)
        data.append(pd.read_csv(filename, names=colnames))

    df = pd.concat(data, ignore_index = True)
    return df

def get_core_conflist(filename, category, rankabove):
    print("get_core_conflist", filename, category, rankabove)
    colnames = ["ID", "Title", "Acronym", "Year", "Rank", "Data", "FoR1", "FoR2", "FoR3"]
    df = pd.read_csv(filename, names=colnames)
    if rankabove == "A*":
        df = df[(df["FoR1"] == category) & (df["Rank"] == "A*")]
    elif rankabove == "A":
        df = df[(df["FoR1"] == category) & ((df["Rank"] == "A*") | (df["Rank"] == "A"))]
    elif rankabove == "B":
        df = df[(df["FoR1"] == category) & ((df["Rank"] == "A*") | (df["Rank"] == "A") | (df["Rank"] == "B"))]
    elif rankabove == "C":
        df = df[(df["FoR1"] == category) & ((df["Rank"] == "A*") | (df["Rank"] == "A") | (df["Rank"] == "B") | (df["Rank"] == "C"))]
    data = df[["Acronym"]].values.tolist();
    return [c[0] for c in data]

def read_kiise_data():
    colnames = ["ID", "Acronym", "Year", "Rank", "Note", "Title", "-"]
    data = []
    for filename in kiise_filenames:
        newf = pd.read_csv(filename, names=colnames)
        data.append(newf)

    df = pd.concat(data, ignore_index = True)
    return df

def read_kiise_fos_data():
    colnames = ["ID", "Acronym", "Year", "Rank", "Note", "Title", "FoR1"]
    data = []
    for filename in kiise_fos_filenames:
        newf = pd.read_csv(filename, names=colnames)
        data.append(newf)

    df = pd.concat(data, ignore_index = True)
    return df

def get_kiise_conflist(filename, category, rankabove):
    print("get_kiise_conflist", filename, category, rankabove)
    colnames = ["ID", "Acronym", "Year", "Rank", "Note", "Title", "FoR1"]
    df = pd.read_csv(filename, names=colnames)
    if rankabove == "S":
        df = df[(df["FoR1"] == category) & (df["Rank"] == "S")]
    elif rankabove == "A":
        df = df[(df["FoR1"] == category) & ((df["Rank"] == "S") | (df["Rank"] == "A"))]
    data = df[["Acronym"]].values.tolist();
    return [c[0] for c in data]

def read_ccf_fos_data():
    colnames = ["Year", "Acronym", "Title", "Rank", "FoR1", "-", "-", "-"]
    data = []
    for filename in ccf_filenames:
        newf = pd.read_csv(filename, names=colnames)
        data.append(newf)

    df = pd.concat(data, ignore_index = True)
    return df

def get_ccf_conflist(filename, category, rankabove):
    print("get_ccf_conflist", filename, category, rankabove)
    colnames = ["Year", "Acronym", "Title", "Rank", "FoR1", "-", "-", "-"]
    df = pd.read_csv(filename, names=colnames)
    if rankabove == "A":
        df = df[(df["FoR1"] == category) & (df["Rank"] == "A")]
    elif rankabove == "B":
        df = df[(df["FoR1"] == category) & ((df["Rank"] == "A") | (df["Rank"] == "B"))]
    elif rankabove == "C":
        df = df[(df["FoR1"] == category) & ((df["Rank"] == "A") | (df["Rank"] == "B") | (df["Rank"] == "C"))]
    data = df[["Acronym"]].values.tolist();
    return [c[0] for c in data]

def find_kiise_fos(year):
    core_df = read_core_data()
    colnames = ["ID", "Acronym", "Year", "Rank", "Note", "Title", "-"]
    newf = pd.read_csv("../data/KIISE/conf{}.csv".format(year), names=colnames)

    core_conf_2020 = core_df.loc[core_df["Year"] == "CORE2020"]
    csv_writer = csv.writer(open("../data/KIISE/conf{}_fos.csv".format(year), "w"))
    for c in newf.values:
        conf_list = [r[0] for r in core_conf_2020[["Acronym"]].values.tolist()]
        fos = "-1"
        if c[1] in conf_list:
            fos = core_conf_2020.loc[core_conf_2020["Acronym"] == c[1]][["FoR1"]].values.tolist()[0][0]
        csv_writer.writerow([c[0], c[1], c[2], c[3], c[4], c[5], fos])


find_kiise_fos(2014)
find_kiise_fos(2016)
find_kiise_fos(2018)


# print(df)
# print(core_df.loc[core_df["Acronym"] == "CIKM"])
# print(core_df.loc[core_df["Acronym"] == "WWW"])
# core_df = read_core_data()
# print(core_df.loc[core_df["Acronym"] == "ISCA"])
# print(df.loc[df["Acronym"] == "ICLR"])
# print(df.loc[df["Acronym"] == "NeurIPS"])
# print(df.loc[df["FoR1"] == 4610.0])

# conf_2020 = df.loc[df["Year"] == "CORE2020"][["Acronym"]].values.tolist()
# conf_2018 = df.loc[df["Year"] == "CORE2018"][["Acronym"]].values.tolist()
# print(conf_2014)
# for c in conf_2018:
#     if c not in conf_2020:
#         print("{} is removed in 2020".format(c[0]))
