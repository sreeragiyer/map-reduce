from pyspark import SparkContext  
import re

sc = SparkContext()
hamlet_text = sc.textFile("../data/loremipsum.txt")

# Pre-processing-
# 1. convert entire text to lower case. 
# 2. remove all whitespaces and all punctuations (keep matching so long as you get alphanumeric character or apostrophe)
words = hamlet_text.flatMap(lambda line: re.findall(r'[\w\']+', line.lower()) )

# word count 
counts_rdd = words.map(lambda word: (word, word.title())).reduceByKey(lambda a, b: a)
counts_tuple = counts_rdd.collect()


# write to file
f = open("capitalized.txt", "w")
for r in counts_tuple:
    f.write(r[1]+"\t")
f.close()

