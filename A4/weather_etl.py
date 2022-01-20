import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary

def main(inputs, output):
    observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
    ])
    weather = spark.read.csv(inputs, schema=observation_schema)
    filter_qflag = weather.filter(weather.qflag.isNull())
    canadian_data = filter_qflag.filter(weather.station.startswith('CA'))
    max_temp = canadian_data.filter(weather.observation == 'TMAX')
    etl_data = max_temp.select('station', 'date', (max_temp.value/10).alias('tmax'))
    etl_data.write.json(output, compression='gzip', mode='overwrite')
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('weather etl').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)