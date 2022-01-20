import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Row

def get_pairs(line):
    sep = line.split(" ")
    sep[0] = sep[0][:-1]     # remove the colon
    # sep = [int(i) for i in sep]
    source = sep[0]
    dest = sep[1:]
    if dest != []:
        for value in dest:
            yield (source, value)

def main(inputs, output, source_node, destination):
    line = sc.textFile(inputs+'/links-simple-sorted.txt')
    pairs = line.flatMap(get_pairs).filter(lambda x: x is not None)
    pair_schema = types.StructType([
    types.StructField('source', types.StringType(), False),
    types.StructField('edge', types.StringType(), False)
    ])
    pairs_df = spark.createDataFrame(pairs, schema = pair_schema).cache()

    #generate the initial row for known_paths
    known_paths = spark.createDataFrame([Row(node = source_node, source = '-', distance = 0)])

    for i in range(6):
        #only look at distance i during new iteration
        df_temp = known_paths.filter(known_paths.distance == i)
        joined_df = df_temp.join(pairs_df, known_paths.node == pairs_df.source)
        new_df = joined_df.select(pairs_df.edge.alias('node'), pairs_df.source)
        new_df = new_df.withColumn('distance',functions.lit(i+1))
        known_paths = known_paths.unionAll(new_df).cache()
        #only keep shortest distance
        known_paths = known_paths.orderBy('distance').dropDuplicates(subset = ['node']).orderBy(['distance','node'])
        known_paths.rdd.coalesce(1).saveAsTextFile(output + '/iter-' + str(i))
        if known_paths.where(known_paths['node'] == destination).count():
            break
    
    # Check if destination is reachable    
    if known_paths.filter(known_paths['node'] == destination).count():

        # trace back from destination to source in order to get the shortest path
        cur_node = destination
        path_list = [cur_node]

        while cur_node != source_node:
            next_node = known_paths.where(known_paths['node'] == cur_node).head(1)[0][1]
            path_list.insert(0, next_node)
            cur_node = next_node
        finalpath = sc.parallelize(path_list)
        finalpath.coalesce(1).saveAsTextFile(output + '/path')
    else:
        print ("Destination is not reachable!")



if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = sys.argv[3]
    destination = sys.argv[4]
    spark = SparkSession.builder.appName('shortest path df').getOrCreate()
    # assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output, source, destination)