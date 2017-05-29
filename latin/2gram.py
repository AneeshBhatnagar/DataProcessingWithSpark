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
	line = line.lower().strip();
	line = re.sub("\s+"," ",line)
	splitLine = line.split("> ",1)
	if len(splitLine) !=2:
		return []
	loc = splitLine[0] + ">"
	splitLine[1] = splitLine[1].replace("j","i").replace("v","u")
	splitLine[1] = "".join(j for j in splitLine[1] if j not in string.punctuation)
	splitLine = splitLine[1].split()
	words = list(itertools.combinations(splitLine,2))
	val = []
	for word in words:
		y,z = word
		if y in lemmas:
			y = lemmas[y]
		else:
			y = [y]
		if z in lemmas:
			z = lemmas[z]
		else:
			z = [z]
		for i in y:
			for j in z:
				val.append([','.join([i,j]),[loc]])
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
res = op.flatMap(processLine)
key = res.reduceByKey(lambda x,y : x+y)
key.saveAsTextFile("output")