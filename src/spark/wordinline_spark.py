from pyspark import SparkContext  
import re

sc = SparkContext()
hamlet_text = sc.textFile("../data/hamlet.txt")


# Split the text into an array of tuples of the form (line, line_number)
lines = hamlet_text.flatMap(lambda txt: txt.split("\n")).zipWithIndex()
# Split each line into an array tuples of the form (word, line_number) with pre-processing as follows-
# 1. convert entire text to lower case. 
# 2. remove all whitespaces and all punctuations (keep matching so long as you get alphanumeric character or apostrophe)
words = lines.flatMap(lambda line_tuple: [(w, line_tuple[1]) for w in re.findall(r'[\w\']+', line_tuple[0].lower())]).distinct().map(lambda words_tuple: (words_tuple[0], [words_tuple[1]]))

# join all line numbers of the same word into one tuple of the form (word, [line_numbers])
line_numbers_rdd = words.reduceByKey(lambda a, b: a + b)
line_numbers_arr = line_numbers_rdd.collect()

# write to file
f = open("../../spark_output/wordinline.txt", "w")
for r in line_numbers_arr:
    f.write("{:<50}{:<30}\n".format(r[0], str(r[1])))
f.close()
