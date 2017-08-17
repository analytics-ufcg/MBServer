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
    def prediction(self):
        # route,tripNumOrig,shapeId,shapeSequence,shapeLatOrig,shapeLonOrig,distanceTraveledShapeOrig,busCode,gpsPointId,gpsLat,gpsLon,distanceToShapePoint,timestampOrig,busStopIdOrig,problem,date,busStopIdDest,timestampDest,tripNumDest,shapeLatDest,shapeLonDest,distanceTraveledShapeDest,duration,distance,hourOrig,hourDest,isRushOrig,isRushDest,periodOrig,periodDest,weekDay,weekOfYear,dayOfMonth,month,isHoliday,isWeekend,isRegularDay
        # 203,1,3855,4649120,-25.487704010585034,-49.29429710552256,484.602,HE710,,-25.487635,-49.294308,7.7511497,05:20:48,25739,NO_PROBLEM,2017-02-01,25737,05:21:29,1,-25.48449704979423,-49.29378227055224,847.103,41,362.501,5,5,0,0,morning,morning,Wed,5,1,2,0,0,1
        # predicting_json_example = {"periodOrig": "morning", "weekDay": "Mon", "route": "203",
        #                            "tripNumOrig": 1, "shapeId": 3855, "shapeLatOrig": -25.487704010585034,
        #                            "shapeLonOrig": -25.487704010585034, "busStopIdOrig": 25739, "busStopIdDest": 25737,
        #                            "shapeLatDest":-25.48449704979423, "shapeLonDest": -49.29378227055224, "hourOrig": 10,
        #                            "isRushOrig": 1, "weekOfYear": 5, "dayOfMonth": 2, "month":2,
        #                            "isHoliday": 0, "isWeekend": 1, "isRegularDay": 0, "distance": 362.501}
        pass

    def createDataframeFromParams(self, dictionary, sc):
        #dictionary = dict(map(lambda (k, v): (k, v[0]), dictionary.iteritems()))
        df = sc.parallelize([dictionary])\
            .map(lambda d: Row(**OrderedDict(sorted((d).items()))))\
            .toDF()
        df.show()
        df.printSchema()

    @tornado.web.asynchronous
    def post(self):
        sconf = SparkConf().setAppName("DurationPredction")
        sc = SparkContext(conf=sconf)  # SparkContext
        sqlContext = SQLContext(sc)

        model_location = "/local/orion/bigsea/btr_2.0/duration_model"

        params = tornado.escape.json_decode(self.request.body)
        print self.request
        print params
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
    print "MBServer started!"
    tornado.ioloop.IOLoop.instance().start()
