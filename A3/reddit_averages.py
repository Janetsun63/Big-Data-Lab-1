from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary

def make_pairs(data):
    subreddit = data["subreddit"]
    score = data["score"]
    count = 1
    yield (subreddit, (count, score))



def add_pairs(x,y):
    sum_count = x[0]+y[0]
    sum_score = x[1]+y[1]
    return(sum_count, sum_score)


def average_score(x):
    avg = x[1][1]/x[1][0]
    return (x[0], avg)


def get_key(kv):
    return kv[0]


def main(inputs, output):
    text = sc.textFile(inputs).map(json.loads)
    pairs = text.flatMap(make_pairs)
    sum_pairs = pairs.reduceByKey(add_pairs)
    average = sum_pairs.map(average_score)
    outdata = average.sortBy(get_key).map(json.dumps)
    outdata.saveAsTextFile(output)
    


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)


