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


def get_rel_score(avg, comment):
    subreddit = comment[0]
    score = comment[1]["score"]
    author = comment[1]["author"]
    relative_score = score/avg.value[subreddit]
    return (relative_score, author)
    


def main(inputs, output):
    commentdata = sc.textFile(inputs).map(json.loads).cache()
    sum_pairs = commentdata.flatMap(make_pairs).reduceByKey(add_pairs)
    # keys here are small integers;
    average = sum_pairs.map(average_score).filter(lambda x:x[1]>0).collect()
    bcast_average = sc.broadcast(dict(average))
    commentbysub = commentdata.map(lambda c: (c['subreddit'], c))
    rel_score = commentbysub.map(lambda x: get_rel_score(bcast_average,x))
    outdata = rel_score.sortBy(get_key, ascending=False).map(json.dumps)
    outdata.saveAsTextFile(output)
    # print(rel_score.take(10))
    


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit score bcast')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)


