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
    filter_qflag = weather.filter(weather.qflag.isNull()).cache()
    max_temp = filter_qflag.filter(filter_qflag.observation == 'TMAX').select('date', 'station', filter_qflag.value.alias('tmax'))
    min_temp = filter_qflag.filter(filter_qflag.observation == 'TMIN').select('date', 'station', filter_qflag.value.alias('tmin'))
    joined_temp = max_temp.join(min_temp, ['date','station'])
    range_df = joined_temp.withColumn('range', (joined_temp['tmax'] - joined_temp['tmin'])/10).cache()
    max_range_df = range_df.groupby('date').agg(functions.max('range').alias('max_range'))
    joined_range = range_df.join(functions.broadcast(max_range_df),'date').where(range_df.range == max_range_df.max_range)
    out_data = joined_range.select('date', 'station', joined_range['max_range'].alias('range')).sort('date', 'station')
    out_data.write.csv(output, mode='overwrite')
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range').getOrCreate()
    # assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)