import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather train').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '3.0' # make sure we have Spark 3.0+
import datetime
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def test_model(model_file):
    # get the data
    test_data = spark.createDataFrame([('sfu burnaby', datetime.date(2021, 11, 19), 49.2771, -122.9146, 330.0, 12.0),
    ('sfu burnaby', datetime.date(2021, 11, 20), 49.2771, -122.9146, 330.0, 0.0)],tmax_schema)
    # load the model
    model = PipelineModel.load(model_file)

    # use the model to make predictions
    predictions = model.transform(test_data)
    predictions.show()
    result=predictions.select("prediction").collect()

    print('Predicted tmax tomorrow:', result[0]['prediction'])
    # # evaluate the predictions
    # r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
    #         metricName='r2')
    # r2 = r2_evaluator.evaluate(predictions)
    
    # rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
    #         metricName='rmse')
    # rmse = rmse_evaluator.evaluate(predictions)

    # print('r2 =', r2)
    # print('rmse =', rmse)

    # If you used a regressor that gives .featureImportances, maybe have a look...
    print(model.stages[-1].featureImportances)


if __name__ == '__main__':
    model_file = sys.argv[1]
    test_model(model_file)
