import pymongo
from bson.code import Code
from pymongo import Connection
import ast
from hammer import *

#initialize collection
conn = Connection()
db = conn.profalyzer
db.drop_collection("profs")
db.drop_collection("flags")
db.drop_collection("info")
collection = db.profs
flags = db.flags
info = db.info

#read in file while dumping
file = open("results_SYDE(2003-2011).txt","r")
for row in file.readlines():
	new = {}
	old = ast.literal_eval(row)
	new["instructor"] = old["instructor"].split(",")[0]
	new["year"] = str(old["term"]["year"])
	new["course"] = str(old["course"]["code"])
	new["value"] = old["data"][17]["average"]
	collection.insert(new)
file.close()

keys = uniquekeys(collection)

file = open("all_avgs_std_devs.txt","w")
for doc in hammer(flags,info,collection,keys).find():
	file.write(str(doc) + "\n")
file.close()


