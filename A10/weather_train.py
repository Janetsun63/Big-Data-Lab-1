import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather train').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '3.0' # make sure we have Spark 3.0+

from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator


def main(inputs, model_file):
    tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
    ])
    data = spark.read.csv(inputs, schema=tmax_schema)

    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    day_of_year = SQLTransformer(statement = """ SELECT *, dayofyear(date) as day from __THIS__ """)
    tmax_yesterday = SQLTransformer(statement = """
    SELECT today.*, dayofyear(today.date) as day, yesterday.tmax AS yesterday_tmax
    FROM __THIS__ as today
    INNER JOIN __THIS__ as yesterday
    ON date_sub(today.date, 1) = yesterday.date
    AND today.station = yesterday.station
    """)
#     tmax_assembler = VectorAssembler(
#         inputCols=['latitude', 'longitude', 'elevation', 'day'], outputCol='features')
    tmax_assembler = VectorAssembler(
        inputCols=['latitude', 'longitude', 'elevation', 'day', 'yesterday_tmax'], outputCol='features')
    regressor = GBTRegressor(
            featuresCol='features', labelCol='tmax', maxDepth=5, stepSize=0.05)
    
#     pipeline = Pipeline(stages=[day_of_year, tmax_assembler, regressor])
    pipeline = Pipeline(stages=[tmax_yesterday, tmax_assembler, regressor])

    model = pipeline.fit(train)
    predictions = model.transform(validation)
    # predictions.show()

    r2_evaluator = RegressionEvaluator(
            predictionCol='prediction', labelCol='tmax',
            metricName='r2')
    rmse_evaluator = RegressionEvaluator(
            predictionCol='prediction', labelCol='tmax',
            metricName='rmse')
    r2 = r2_evaluator.evaluate(predictions)
    rmse = rmse_evaluator.evaluate(predictions)
    print('r2 =', r2)
    print('rmse =', rmse)
    

    model.write().overwrite().save(model_file)


if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]
    main(inputs, model_file)

