from pyspark import SparkContext  
import re

sc = SparkContext()
hamlet_text = sc.textFile("../data/hamlet.txt")

# Pre-processing-
# 1. convert entire text to lower case. 
# 2. remove all whitespaces and all punctuations (keep matching so long as you get alphanumeric character or apostrophe)
words = hamlet_text.flatMap(lambda line: re.findall(r'[\w\']+', line.lower()) )

# word count 
counts_rdd = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
counts_tuple = counts_rdd.collect()

# write to file
f = open("../spark_output/wordcount.txt", "w")
for r in counts_tuple:
    f.write("{:<100}{:<30}\n".format(r[0], r[1]))
f.close()

