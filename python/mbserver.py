# coding: UTF-8
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

    def createDataframeFromParams(self, dictionary, sc):
        sc.parallelize({"bla": 123}).toDF().show()
				
    @tornado.web.asynchronous
    def post(self):

        sconf = SparkConf().setAppName("DurationPredction")
        sc = SparkContext(conf=sconf)  # SparkContext
        sqlContext = SQLContext(sc)

        model_location = "../../../duration_model"
        
        params = self.request.body_arguments
        print(params)
        self.createDataframeFromParams(params, sc)
        
        duration_model_loaded = LinearRegressionModel.load(model_location)
				
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
