import pymongo
import itertools
from bson.code import Code
from pymongo import Connection

def othertags(flags,id,top,scope):

	for level in range(1,top) :
		for row in itertools.combinations(id.split("|"),level):
			find = flags.find_one({"id":keyfinder(row)})
			if not(find==None) and find["scope"] == scope :
				return True
				
	return False
	
def keymaker(row):
	key = ""
	for x in range(0,len(row)):
		if x == len(row) -1 :
			key+= "this." + str(row[x])
		else :
			key+= "this." + str(row[x]) + "+'|'+"
	return key

def querymaker(row,doc):
	query = {}
	values = doc["_id"].split("|")
	
	for x in range(0,len(row)):
		query[row[x]]= values[x]

	return query
	
def keyfinder(row):
	key = ""
	
	for x in range(0,len(row)):
		if x == len(row) -1 :
			key+= row[x]
		else :
			key+= row[x]+ "|"
	return key

def mr_mean(collection,slice=""):
	
	if slice == "":
		overall = True
	else :
		overall = False
		slice = keymaker(slice)
	
		
	reduce = Code("function(key,values){var ret= {sum:0,count:0};"
				  "for(var i=0;i<values.length;i++){"
				  "	ret.sum+= values[i].sum; ret.count+= values[i].count; }"
				  "return ret;}")
	finalize = Code("function(key,value){return value.sum/value.count;}")
	
	if overall :
		map = Code("function(){emit('overall',{sum: parseFloat(this.value),count: 1});}")
	else :
		map = Code("function(){emit(" + slice +",{sum: parseFloat(this.value),count: 1});}")
		 
	avg = collection.map_reduce(map,reduce,"avg" + slice,finalize=finalize)
	
	return avg

def mr_std_dev(collection,mean,slice="",query=""):

	
	if slice == "":
		overall = True
	else :
		overall = False
		
	reduce = Code("function(key,values){var ret= {sum:0,count:0};"
				  "for(var i=0;i<values.length;i++){"
				  "	ret.sum+= values[i].sum; ret.count+= values[i].count; }"
				  "return ret;}")
	finalize = Code("function(key,value){return Math.sqrt(value.sum/value.count);}")
	
	if overall :
		map = Code("function(){emit('overall',{sum: Math.pow(parseFloat(this.value)-"+str(mean)+",2),count: 1});}")
	else :
		map = Code("function(){emit(this." +  slice +",{sum: parseFloat(this.value),count: 1});}")
	
	if query == "" :
		std_dev = collection.map_reduce(map,reduce,"std_dev" + slice,finalize=finalize)
	else:
		std_dev = collection.map_reduce(map,reduce,"std_dev" + slice,finalize=finalize,query=query)
	
	return std_dev


def mr_poly(collection,fieldname,keys) :	
	
	mr_key = ""
	for i in range(len(keys)):
		if not (keys[i] == fieldname):
			mr_key += "this."  + keys[i] + "+ '|' +" 
	mr_key = mr_key.rstrip("+ '|' +" )		
	
	reduce = Code("function(key,values){var retx=[];var rety=[];"
				  "for(var i=0;i<values.length;i++){"
				  "	retx = retx.concat(values[i].x); rety = rety.concat(values[i].y);}"
				  "return {x:retx, y:rety};}")
				  
	map = Code("function(){emit("+ mr_key +",{x:parseFloat(this."+ fieldname +"),y:parseFloat(this.value)});}")
	poly = collection.map_reduce(map,reduce,"polymapping")

	return poly

def uniquekeys(collection) :
	#determine all types of keys
	map = Code("function(){for(var key in this){emit(key,null);}}")
	reduce = Code("function(key,stuff){return null;}")
	result = collection.map_reduce(map,reduce,"all_keys")
	keys = []
	for doc in result.find():
		if doc["_id"] != "_id" and doc["_id"] != "value":
			keys.append(doc["_id"])
	return keys
	
def hammer(flags,info,collection,keys,preprocessed=False) :
	#overall stats
	average = mr_mean(collection).find_one()["value"]
	std_dev = mr_std_dev(collection,average).find_one()["value"]

	#tolerance for variance
	eps = 1
		
	#Calculate average for everything and loop 
	for x in range(1, len(keys)+1):
		for row in itertools.combinations(keys,x):
			for doc in mr_mean(collection,row).find():
				#std_dev calculation
				doc["std_dev"] = mr_std_dev(collection,doc["value"],"",querymaker(row,doc)).find_one()["value"]
				
				#go through scopes		
				avg = doc["value"]
				doc["flag"] = False
				
				#overall
				if abs(avg - average)/std_dev > eps and (not othertags(flags,doc["_id"],x,"overall")):
					flags.insert({"id":doc["_id"],"scope":"overall","dif": avg - average})
					doc["flag"] = True
				
				#subscope
				for level in range(1,x) :
					for upper_row in itertools.combinations(doc["_id"].split("|"),level):
						find = info.find_one({"_id":keyfinder(upper_row)})
						if not(find==None) : #highest level has no scope
							if abs(avg - doc["value"])/find["std_dev"] > eps and (not othertags(info,doc["_id"],x,find["_id"])):
								flags.insert({"id":doc["_id"],"scope":find["_id"],"dif": avg - find["value"]})
								doc["flag"] = True
				info.insert(doc)
				
		
	for row in flags.find():
		print row["id"] + "(" + row["scope"] + ") " + str(row["dif"])
	return keys
