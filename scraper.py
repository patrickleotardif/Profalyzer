from BeautifulSoup import BeautifulSoup
import pickle
import urllib
import urllib2

#supply your own credentials here
username= ""
password = ""

#add on password stuff
password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
top_level_url = "https://www.eng.uwaterloo.ca/critiques/index.php"
password_mgr.add_password(None, top_level_url, username, password)
handler = urllib2.HTTPBasicAuthHandler(password_mgr)
opener = urllib2.build_opener(handler)
opener.open(top_level_url)
urllib2.install_opener(opener)

#grab local file for request data and list of all possible courses
url = "https://www.eng.uwaterloo.ca/critiques/index.php"
file = open("values.txt","r")
values = pickle.load(file)
file.close()
file = open("hidden-course.txt","r")
courses = file.read().split("|")
file.close()
file = open("results.txt","w")

terms = ["FALL","WINTER","SPRING"]
years = range(2003,2012)
for year in years:
	for term in terms:
		for course in courses:
			#Only SYDE for now...
			if not course.find("SY DE") == -1 :
				values["course"] = course
				values["term"] = term + " " + str(year)
				print course + "(" + values["term"] + ")"
				
				data = urllib.urlencode(values)
				req = urllib2.Request(url, data)
				response = urllib2.urlopen(req)
				the_page = response.read()

				doc = the_page
				soup = BeautifulSoup(doc)
				litmus = soup.findAll(attrs={"id":"primarycontent"})

				if not litmus[0].contents[1].contents[0] == "No results were found.":
					result = soup.findAll("tr")
					dict = {	"term":{"year":int(year), "time": term.lower()},
							"instructor":str(litmus[0].contents[1].contents[1].contents[3].contents[0].lstrip("Instructor: ").rstrip()) ,
							"course": {"faculty":"SYDE","code": int(course.replace("SY DE","")),"year":int(course[6])}}
					data = {}

					for i in range(0,len(result)):
						if i == 22:
							dict["class_size"] = int(result[22].contents[5].contents[0].lstrip("Class Size: "))
							dict["total_replies"] = int(result[22].contents[3].contents[0].lstrip("Total replies: "))
						elif len(result[i]) == 20:
							q = int(result[i].contents[0].contents[0].contents[0])
							data[q] = {}
							data[q]["replies"] = int(result[i].contents[2].contents[0])
							
							data[q]["A"] = int(result[i].contents[6].contents[0])
							data[q]["B"] = int(result[i].contents[8].contents[0])
							data[q]["C"] = int(result[i].contents[10].contents[0])
							data[q]["D"] = int(result[i].contents[12].contents[0])
							data[q]["E"] = int(result[i].contents[14].contents[0])
							
							data[q]["average"] = int(result[i].contents[18].contents[0])


					dict["data"] = data
					file.write(str(dict)+"\n")

	
file.close()

