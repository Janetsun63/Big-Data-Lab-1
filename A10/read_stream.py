import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('streaming example').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')


def main():
 
    topic = sys.argv[1]
    messages = spark.readStream.format('kafka') \
            .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
            .option('subscribe', topic).load()

    values = messages.select(messages['value'].cast('string'))
    values = values.select(functions.split(values['value'], " ").alias('split'))
    values = values.withColumn('x', values['split'].getItem(0).cast('float') ).withColumn('y', values['split'].getItem(1).cast('float')).withColumn('n', functions.lit(1))
    values = values.withColumn('xy', values['x']*values['y']).withColumn('x2', values['x']**2)
    sums = values.groupby().sum()
    output = sums.withColumn('slope',(sums['sum(xy)']-sums['sum(x)']*sums['sum(y)']/sums['sum(n)']) / (sums['sum(x2)']-sums['sum(x)']**2/sums['sum(n)']))
    output = output.withColumn('intercept',sums['sum(y)']/sums['sum(n)']-output['slope']*sums['sum(x)']/sums['sum(n)'])
    output = output.select(['slope','intercept'])
    #start streaming and collect data
    stream = output.writeStream.format('console') \
            .outputMode('complete').start()
    
    stream.awaitTermination(600)

    #calculate coefficients
    #slope = (xy_sum - x_sum * y_sum * (n**-1)) / (x_square_sum - x_sum **2 * (n**-1))
    #intercept = y_sum/n - slope * x_sum/n

    #print("Slope is: ",slope)
    #print("Intercept is: ",intercept)

if __name__ == '__main__':
    main()



