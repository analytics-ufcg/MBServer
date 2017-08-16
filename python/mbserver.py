import tornado.ioloop
import tornado.web

import findspark
findspark.init()

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.regression import LinearRegressionModel
from pyspark.mllib.evaluation import RegressionMetrics

from tornado.options import define, options, parse_command_line

import os
import sys

define("port", default=8888, help="run on the given port", type=int)

class IndexHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):

        sconf = SparkConf().setAppName("DurationPredction")
        sc = SparkContext(conf=sconf)  # SparkContext
        sqlContext = SQLContext(sc)

        # df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema","true").load('hdfs://localhost:9000/btr/ctba/data/prediction_data.csv')
        # df = df.withColumn("totalpassengers", df['totalpassengers'].cast('Double'))
        #
        # string_columns = ["route", "week_day", "difference_previous_schedule", "difference_next_schedule"]
        # features = ["route_index", "week_day_index", "group_15_minutes", "difference_next_schedule_index", "difference_previous_schedule_index"]
        #
        # indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(df) for column in string_columns]
    	# pipeline = Pipeline(stages=indexers)
    	# df_r = pipeline.fit(df).transform(df)
        #
    	# assembler = VectorAssembler(
    	# inputCols=features,
    	# outputCol='features')
        #
    	# assembled_df = assembler.transform(df_r)

        model_location = "hdfs://localhost:9000/btr/ctba/models/trip_duration"
        duration_model_loaded = LinearRegressionModel.load(model_location)
        #
        # predictions = duration_model_loaded.transform(assembled_df)
        # predictions_and_labels = predictions.rdd.map(lambda row: (row.prediction, row.duration))
        # trainingSummary = RegressionMetrics(predictions_and_labels)

        # response = duration_model_loaded.transform()

        self.write("Coefficients: %s\n" % str(duration_model_loaded.coefficients))

        sc.stop()
        self.finish()

app = tornado.web.Application([
    (r'/previsao', IndexHandler),
])

if __name__ == '__main__':
    parse_command_line()
    app.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
