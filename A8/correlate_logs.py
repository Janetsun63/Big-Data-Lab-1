import sys,re, math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Row

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def seperate_line(line):
    # val = line_re.match(line)
    # if val:
    sep = line_re.split(line)
    if len(sep) == 6:
        return Row(sep[1], sep[2], sep[3], int(sep[4]))
    

def main(inputs):
    logs = sc.textFile(inputs)
    new_line = logs.map(seperate_line).filter(lambda x: x is not None)
    log_schema = types.StructType([
    types.StructField('host_name', types.StringType()),
    types.StructField('datetime', types.StringType()),
    types.StructField('requested_path', types.StringType()),
    types.StructField('bytes_transferred', types.IntegerType()),
    ])
    log_df = spark.createDataFrame(new_line, schema=log_schema)
    df2 = log_df.groupby('host_name').agg(
        functions.count('requested_path').alias('x'),
        functions.sum('bytes_transferred').alias('y')
        )
    six_values = df2.withColumn('1', functions.lit(1)).withColumn('x^2',df2['x']**2).withColumn('y^2', df2['y']**2).withColumn('xy', df2['x']*df2['y'])
    six_sums = six_values.groupby().sum().collect()
    #six_sums=[Row(sum(x)=1972, sum(y)=36133736, sum(1)=232, sum(x^2)=32560.0, sum(y^2)=25731257461526.0, sum(xy)=662179733)]
    n = six_sums[0]['sum(1)']
    sum_x = six_sums[0]['sum(x)']
    sum_y = six_sums[0]['sum(y)']
    sum_x2 = six_sums[0]['sum(x^2)']
    sum_y2 = six_sums[0]['sum(y^2)']
    sum_xy = six_sums[0]['sum(xy)']
    r = (n*sum_xy - sum_x*sum_y)/(math.sqrt(n*sum_x2-(sum_x)**2) * math.sqrt(n*sum_y2-(sum_y)**2))
    r2 = r**2
    print ('r = ' + str(r))
    print ('r^2 = ' + str(r2))



    
    

    




if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('correlate logs').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)