import string
import itertools
import pyspark
import re
lemmas = {}

def readLemmas(file):
	f = open(file,'r')
	lines = f.readlines()
	for line in lines:
		x = line.strip().split(",")
		y = [str(x[i]) for i in range(len(x)) if i!=0 and x[i]!="" and x[i]!="\n"]
		lemmas[x[0]] = y

def processLine(line):
	#splitLine = ' '.join(line.split()).split("> ")
	line = line.lower().strip();
	line = re.sub("\s+"," ",line)
	#line = line.encode('utf8')
	splitLine = line.split("> ",1)
	if len(splitLine) !=2:
		return []
	loc = splitLine[0] + ">"
	# if len(splitLine) == 1:
	# 	print("Invlid File")
	# 	return
	# splitLine[1] = splitLine[1].lower().strip(string.punctuation)
	splitLine[1] = splitLine[1].replace("j","i").replace("v","u")
	splitLine[1] = "".join(j for j in splitLine[1] if j not in string.punctuation)
	splitLine = splitLine[1].split()
	words = list(itertools.combinations(splitLine,3))
	# op = [] 
	# for word in words:
	# 	op.append(','.join(word))
	val = []
	for word in words:
		x,y,z = word
		if x in lemmas:
			x = lemmas[x]
		else:
			x = [x]
		if y in lemmas:
			y = lemmas[y]
		else:
			y = [y]
		if z in lemmas:
			z = lemmas[z]
		else:
			z = [z]
		for i in x:
			for j in y:
				for k in z:
					val.append([','.join([i,j,k]),[loc]])
	return val



def callFromPython():
	f = open('lucan.bellum_civile.part.1.tess')
	lines = f.readlines()
	for line in lines:
		processLine(line)

readLemmas('new_lemmatizer.csv')
sc = pyspark.SparkContext()
files = sc.textFile("files")
#op = files.flatMap(lambda line: line.split("\n"))
op = files.filter(lambda line: len(line.split(">")) != 1)
res = op.flatMap(lambda line: processLine(line))
key = res.reduceByKey(lambda x,y : x+y)
key.saveAsTextFile("output")