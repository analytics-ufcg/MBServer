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
import ast
from datetime import datetime, date

define("port", default=8888, help="run on the given port", type=int)


class IndexHandler(tornado.web.RequestHandler):

    @tornado.web.asynchronous
    def post(self):

        request_params = tornado.escape.json_decode(self.request.body)
        prediction = sparkHandler.predict(request_params)

        self.write(prediction.first())
        self.finish()


class JoinBTRandOTPHandler(tornado.web.RequestHandler):

    def set_variables(self, request_params):
        self.date_fmt = datetime.strptime(request_params["date"], "%m/%d/%Y")
        self.time = request_params["time"]
        self.hour = self.time.split(":")[0]
        self.period_origin = "morning" if self.hour < 12 else "afternoon" if self.hour < 18 else "night"
        self.is_rush_hour = 1 if self.hour in [6, 7, 11, 12, 17, 18] else 0

        self.weekday = self.date_fmt.weekday()
        wd = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        self.weekday_str = wd[self.weekday]

        self.week_of_year = self.date_fmt.isocalendar()[1]
        self.is_holiday = 1 if self.date_fmt.month in [1, 6, 7, 12] else 0
        self.is_weekend = 1 if self.weekday in [5, 6] else 0
        self.is_regular_day = 1 if self.weekday in [1, 2, 3] else 0

    def feature_extractor(self, leg):
        features = dict()

        features["distance"] = leg["distance"]

        features["hourOrig"] = int(self.hour)
        features["periodOrig"] = self.period_origin
        features["isRushOrig"] = self.is_rush_hour

        features["weekDay"] = self.weekday_str
        features["weekOfYear"] = self.week_of_year
        features["dayOfMonth"] = self.date_fmt.day
        features["month"] = self.date_fmt.month
        features["isHoliday"] = self.is_holiday
        features["isWeekend"] = self.is_weekend
        features["isRegularDay"] = self.is_regular_day

        features["route"] = leg["route"]

        features["shapeLatOrig"] = leg["from"]["lat"]
        features["shapeLonOrig"] = leg["from"]["lon"]
        features["shapeLatDest"] = leg["to"]["lat"]
        features["shapeLonDest"] = leg["to"]["lon"]

        features["busStopIdOrig"] = int(leg["from"]["stopId"].split(":")[1])
        features["busStopIdDest"] = int(leg["to"]["stopId"].split(":")[1])

        return features

    def get_btr_duration(self, legs):
        walk_durations = list()
        bus_legs = list()

        for leg in legs:
            if leg["mode"] == "WALK":
                walk_durations.append(leg["duration"])
            else:
                bus_legs.append(leg)

        bus_legs = map(self.feature_extractor, bus_legs)

        rdd_pred = sparkHandler.predict(bus_legs)

        predictions = map(ast.literal_eval, rdd_pred.collect())

        bus_duration = map(lambda e: e["prediction"], predictions)

        return reduce(lambda l1, l2: l1 + l2, walk_durations + bus_duration)

    def get_btr_prediction(self, otp_data):
        for it in otp_data["plan"]["itineraries"]:
            it["btr-duration"] = self.get_btr_duration(it["legs"])

        return otp_data


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

        otp_data = tornado.escape.json_decode(response.body)

        self.set_variables(request_params)

        otp_data_predicted = self.get_btr_prediction(otp_data)

        self.write(otp_data_predicted)


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
