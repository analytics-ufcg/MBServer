# coding: UTF-8
import tornado.ioloop
import tornado.web
import tornado.escape

import findspark

findspark.init()

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import Row
from collections import OrderedDict
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
    def predict(self, model, test_data):
        # predicting_json_example = {"periodOrig": "morning", "weekDay": "Mon", "route": "203",
        #                           "shapeLatOrig": -25.487704010585034,
        #                           "shapeLonOrig": -25.487704010585034, "busStopIdOrig": 25739, "busStopIdDest": 25737,
        #                           "shapeLatDest":-25.48449704979423, "shapeLonDest": -49.29378227055224,
        #                           "hourOrig": 10,"isRushOrig": 1, "weekOfYear": 5, "dayOfMonth": 2, "month":2,
        #                           "isHoliday": 0, "isWeekend": 1, "isRegularDay": 0, "distance": 362.501}
        prediction = model.transform(test_data)

        return prediction.toJSON()

    def data_pre_proc(self, df,
                      string_columns=["periodOrig", "weekDay", "route"],
                      features=["shapeLatOrig", "shapeLonOrig",
                                "busStopIdOrig", "busStopIdDest", "shapeLatDest", "shapeLonDest",
                                "hourOrig", "isRushOrig", "weekOfYear", "dayOfMonth",
                                "month", "isHoliday", "isWeekend", "isRegularDay", "distance"]):
        df = df.na.drop(subset=string_columns + features)

        indexers = [StringIndexer(inputCol=column, outputCol=column + "_index").fit(df) for column in string_columns]
        pipeline = Pipeline(stages=indexers)
        df_r = pipeline.fit(df).transform(df)

        assembler = VectorAssembler(
            inputCols=features + map(lambda c: c + "_index", string_columns),
            outputCol='features')

        assembled_df = assembler.transform(df_r)

        return assembled_df

    def createDataframeFromParams(self, dictionary, sc):
        df = sc.parallelize([dictionary])\
            .map(lambda d: Row(**OrderedDict(sorted((d).items()))))\
            .toDF()

        return df

    @tornado.web.asynchronous
    def post(self):
        sconf = SparkConf().setAppName("DurationPredction")
        sc = SparkContext(conf=sconf)  # SparkContext
        sqlContext = SQLContext(sc)

        request_params = tornado.escape.json_decode(self.request.body)

        df = self.createDataframeFromParams(request_params, sc)
        assembled_df = self.data_pre_proc(df)

        # model_location = "/local/orion/bigsea/btr_2.0/duration_model"
        model_location = "hdfs://localhost:9000/btr/ctba/models/trip_duration"
        duration_model = LinearRegressionModel.load(model_location)

        print duration_model.coefficients

        prediction = self.predict(duration_model, assembled_df)

        self.write(prediction.first())

        sc.stop()
        self.finish()


app = tornado.web.Application([
    (r'/previsao', IndexHandler),
])

if __name__ == '__main__':
    parse_command_line()
    app.listen(options.port)
    print "MBServer started!"
    tornado.ioloop.IOLoop.instance().start()
