from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('Wikipedia Popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+



def words_once(line):
    data = tuple(line.split(" "))
    yield data

def convert_int(data):
    data = list(data)
    data[3] = int(data[3])
    data = tuple(data)
    return data

def records_filter(data):
    language = data[1]
    title = data[2]
    if language == "en" and title !="Main_Page" and not (title.startswith("Special:")):
        return data

def get_key(kv):
    return kv[0]


def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])

def output_format(data):
    date = data[0]
    title = data[2]
    view = data[3]
    pair = (view, title)
    return (date, pair)

text = sc.textFile(inputs)
words = text.flatMap(words_once).map(convert_int).filter(records_filter).map(output_format)
wordcount = words.reduceByKey(max)
outdata = wordcount.sortBy(get_key)
outdata.map(tab_separated).saveAsTextFile(output)
