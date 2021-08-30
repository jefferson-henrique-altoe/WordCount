from pyspark import SparkConf, SparkContext
from operator import add


APP_NAME = "My WordCount Spark Application"
FILE_NAME = "sherlock.txt"


def tokenize(text):
	return text.split()


def main(sc):
	text = sc.textFile(FILE_NAME)
	words = text.flatMap(tokenize)
	wc = words.map(lambda x: (x,1))
	counts = wc.reduceByKey(add)
	counts.saveAsTextFile("wc")


if __name__ == "__main__":
	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster("local[*]")
	sc = SparkContext(conf=conf)
	main(sc)
	
