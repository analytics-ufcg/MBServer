# coding: UTF-8
import tornado.ioloop
import tornado.web
import tornado.escape

from spark_handler import SparkHandler

from tornado.options import define, options, parse_command_line

import os
import sys

define("port", default=8888, help="run on the given port", type=int)


class IndexHandler(tornado.web.RequestHandler):

    @tornado.web.asynchronous
    def post(self):

        request_params = tornado.escape.json_decode(self.request.body)
        prediction = sparkHandler.predict(request_params)

        self.write(prediction.first())
        self.finish()


app = tornado.web.Application([
    (r'/previsao', IndexHandler),
])


def startUp():
    global sparkHandler
    modelPath = "hdfs://172.17.0.1:9000/btr/ctba/models/trip_duration"
    appName = "Duration Prediction"
    sparkHandler = SparkHandler(appName, modelPath)
    parse_command_line()
    app.listen(options.port)
    print "MBServer started!"
    tornado.ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    startUp()
