from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def main(inputs, output, source, destination):
    graph = spark.read.text(inputs)



if __name__ == '__main__':
    conf = SparkConf().setAppName('shortest path rdd')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = sys.argv[3]
    destination = sys.argv[4]
    main(inputs, output, source, destination)
