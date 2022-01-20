import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    split = path.split("/")
    start = len('pagecounts-')
    end = start + len('YYYYMMDD-HH')
    hour = split[-1][start:end]
    return hour


def main(inputs, output):
    page_view_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('views', types.IntegerType()),
    types.StructField('size', types.IntegerType()),
    ])
    views = spark.read.csv(inputs, schema=page_view_schema, sep=" ").withColumn('filename', functions.input_file_name())
    get_hour = views.withColumn('hour', path_to_hour(views['filename']))
    filtered_views = get_hour.filter((views.language == 'en') & (views.title != 'Main_Page') & (~ views.title.startswith('Special:'))).cache()
    max_views = filtered_views.groupby('hour').agg(functions.max(filtered_views.views).alias('most_views'))
    joined_views = filtered_views.join(max_views, 'hour').filter(filtered_views.views == max_views.most_views)
    out_data = joined_views.select('hour', 'title', 'views').sort('hour', 'title')
    out_data.write.json(output, mode='overwrite')
    out_data.explain()
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('wikipedia popular df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)