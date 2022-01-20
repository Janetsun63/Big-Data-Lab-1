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
    weather.createOrReplaceTempView("weather")
    max_obs = spark.sql("""SELECT date, station, value AS tmax FROM weather WHERE qflag IS NULL AND observation = 'TMAX'""")
    max_obs.createOrReplaceTempView("max_obs")
    min_obs = spark.sql("""SELECT date, station, value AS tmin FROM weather WHERE qflag IS NULL AND observation = 'TMIN'""")
    min_obs.createOrReplaceTempView("min_obs")
    joined_obs = spark.sql("""SELECT max_obs.date, max_obs.station, tmax, tmin FROM max_obs JOIN min_obs ON min_obs.date=max_obs.date AND  max_obs.station = min_obs.station """)
    joined_obs.createOrReplaceTempView("joined_obs")
    range_df = spark.sql("""SELECT date, station, (tmax - tmin)/10 AS range FROM joined_obs""")
    range_df.createOrReplaceTempView("range_df")
    max_range_df = spark.sql("""SELECT date, MAX(range) AS max_range FROM range_df GROUP BY date ORDER BY date""")
    max_range_df.createOrReplaceTempView("max_range_df")
    joined_data = spark.sql("""SELECT range_df.date, station, max_range FROM max_range_df JOIN range_df ON range_df.date = max_range_df.date AND range_df.range = max_range_df.max_range""")
    joined_data.createOrReplaceTempView("joined_data")
    out_data = spark.sql("""SELECT date, station, max_range AS range FROM joined_data ORDER BY date, station""")
    out_data.write.csv(output, mode='overwrite')
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range sql').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)