# coding: UTF-8
from datetime import datetime, time


class FeatureExtractionHandler:
    def __init__(self, request_params):
        self.date_fmt = datetime.strptime(request_params["date"], "%m/%d/%Y")
        self.time = request_params["time"]
        self.hour = self.time.split(":")[0]
        self.period_origin = "morning" if self.hour < 12 else "afternoon" if self.hour < 18 else "night"
        self.is_rush_hour = 1 if self.hour in [6, 7, 11, 12, 17, 18] else 0

        self.weekday = self.date_fmt.weekday()
        wd = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sab", "Dom"]
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
