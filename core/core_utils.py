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

def read_core_data():
    colnames = ["ID", "Title", "Acronym", "Year", "Rank", "Data", "FoR1", "FoR2", "FoR3"]
    data = []
    for filename in filenames:
        newf = pd.read_csv(filename, names=colnames, header=0)
        data.append(pd.read_csv(filename, names=colnames, header=0))

    df = pd.concat(data, ignore_index = True)
    return df

# df = read_core_data()
# print(df)
# print(df.loc[df["Acronym"] == "WSDM"])
# print(df.loc[df["Acronym"] == "ICLR"])
# print(df.loc[df["Acronym"] == "NeurIPS"])
# print(df.loc[df["FoR1"] == 4610.0])

# conf_2020 = df.loc[df["Year"] == "CORE2020"][["Acronym"]].values.tolist()
# conf_2018 = df.loc[df["Year"] == "CORE2018"][["Acronym"]].values.tolist()
# print(conf_2014)
# for c in conf_2018:
#     if c not in conf_2020:
#         print("{} is removed in 2020".format(c[0]))
