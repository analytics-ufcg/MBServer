# coding: UTF-8
import tornado.ioloop
import tornado.web
import tornado.escape
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornado.options import define, options, parse_command_line

from spark_handler import SparkHandler

import btr_otp_config
import urllib

define("port", default=8888, help="run on the given port", type=int)


class IndexHandler(tornado.web.RequestHandler):

    @tornado.web.asynchronous
    def post(self):

        request_params = tornado.escape.json_decode(self.request.body)
        prediction = sparkHandler.predict(request_params)

        self.write(prediction.first())
        self.finish()


class JoinBTRandOTPHandler(tornado.web.RequestHandler):

    @gen.coroutine
    def post(self):
        # query example: {
        #   "fromPlace": "-25.39211,-49.22613",
        #   "toPlace": "-25.45102,-49.28381",
        #   "mode": "TRANSIT,WALK",
        #   "date": "04/03/2017",
        #   "time": "17:20:00"
        #   }
        request_params = tornado.escape.json_decode(self.request.body)

        query = btr_otp_config.OTP_LINK + urllib.urlencode(request_params)

        http_client = AsyncHTTPClient()
        response = yield http_client.fetch(query)

        self.write(response.body)


app = tornado.web.Application([
    (r'/previsao', IndexHandler),
    (r'/btr_routes_plans', JoinBTRandOTPHandler)
])


def startUp():
    global sparkHandler
    modelPath = btr_otp_config.MODEL_PATH
    appName = "Duration Prediction"
    sparkHandler = SparkHandler(appName, modelPath)
    parse_command_line()
    app.listen(options.port)
    print "MBServer started!"
    tornado.ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    startUp()
