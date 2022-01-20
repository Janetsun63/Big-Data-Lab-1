from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary

def get_data(comment):
    subreddit = comment["subreddit"]
    score = comment["score"]
    author = comment["author"]
    yield subreddit, score, author




def main(inputs, output):
    text = sc.textFile(inputs).map(json.loads)
    data = text.flatMap(get_data).filter(lambda x:'e' in x[0]).cache()
    positive = data.filter(lambda x: x[1]>0).map(json.dumps).saveAsTextFile(output + '/positive')
    negative = data.filter(lambda x: x[1]<=0).map(json.dumps).saveAsTextFile(output + '/negative')
    


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)


