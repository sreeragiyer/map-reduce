from pyspark import SparkContext  
import re

sc = SparkContext()
hamlet_text = sc.textFile("../data/loremipsum.txt")

# Pre-processing-
# 1. convert entire text to lower case. 
# 2. remove all whitespaces and all punctuations (keep matching so long as you get alphanumeric character or apostrophe)
words = hamlet_text.flatMap(lambda line: re.findall(r'[\w\']+', line.lower()) )

# group words by length
lengths_rdd = words.map(lambda word: (len(word), word)).distinct().map(lambda word_tuple: (word_tuple[0], [word_tuple[1]])).reduceByKey(lambda a, b: a + b)
lengths_tuple = lengths_rdd.collect()

# write to file
f = open("../spark_output/wordlength.txt", "w")
for r in lengths_tuple:
    f.write("{:<50}{:<30}\n".format(r[0], str(r[1])))
f.close()

