# coding: UTF-8
from feature_extraction_handler import FeatureExtractionHandler

import ast


class PredictionHandler:

    def __init__(self, spark_handler):
        self.sparkHandler = spark_handler

    def get_btr_duration(self, legs, feature_extraction_handler):
        walk_durations = list()
        bus_legs = list()

        for leg in legs:
            if leg["mode"] == "WALK":
                walk_durations.append(leg["duration"])
            else:
                bus_legs.append(leg)

        bus_legs = map(feature_extraction_handler.feature_extractor, bus_legs)

        rdd_pred = self.sparkHandler.predict(bus_legs)

        predictions = map(ast.literal_eval, rdd_pred.collect())

        bus_duration = map(lambda e: e["prediction"], predictions)

        return reduce(lambda l1, l2: l1 + l2, walk_durations + bus_duration)

    def get_btr_prediction(self, otp_data, request_params):
        feature_extraction_handler = FeatureExtractionHandler(request_params)
        for it in otp_data["plan"]["itineraries"]:
            it["btr-duration"] = self.get_btr_duration(it["legs"], feature_extraction_handler)

        return otp_data
