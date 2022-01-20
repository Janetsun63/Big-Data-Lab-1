from pyspark import SparkConf, SparkContext
import sys
import re, string
import random
import operator
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+



def get_itr(data):
    random.seed()
    data = int(data)
    total_iterations = 0
    for i in range(data):
        sum = 0.0
        while sum < 1.0:
            sum += random.uniform(0, 1)
            total_iterations += 1
    return total_iterations
      


def main(inputs):
    samples = int(inputs)
    partitions = 100
    partition_size = samples/partitions
    data = sc.parallelize([partition_size]*partitions, numSlices=partitions)
    itr = data.map(get_itr)
    total_itr = itr.reduce(operator.add)
    euler = total_itr/(int(inputs))
    print(euler)


if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    main(inputs)
