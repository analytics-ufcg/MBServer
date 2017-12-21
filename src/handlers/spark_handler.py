import findspark
from intermediate_stops_extraction_handler import IntermediateStopsExtractionHandler

findspark.init()

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import PipelineModel
from pyspark.ml.regression import LinearRegressionModel


class SparkHandler:

    def __init__(self, app_name, duration_model_path, crowdedness_model_path, pipeline_path, routes_stops_path):
        self.sc = SparkContext(conf=SparkConf().setAppName(app_name))
        self.sqlContext = SQLContext(self.sc)
        self.duration_model = LinearRegressionModel.load(duration_model_path)
        self.crowdedness_model = LinearRegressionModel.load(crowdedness_model_path)
        self.pipeline = PipelineModel.load(pipeline_path)
        self.intermediate_stops_extraction_handler = IntermediateStopsExtractionHandler(self.sc, self.sqlContext,
                                                                                        routes_stops_path)

    def updateResources(duration_model_path, crowdedness_model_path, pipeline_path, routes_stops_path):
        self.duration_model = LinearRegressionModel.load(duration_model_path)
        self.crowdedness_model = LinearRegressionModel.load(crowdedness_model_path)
        self.pipeline = PipelineModel.load(pipeline_path)
        self.intermediate_stops_extraction_handler = IntermediateStopsExtractionHandler(self.sc, self.sqlContext,
                                                                                        routes_stops_path)


    def predictDuration(self, test_data):
        # predicting_json_example = {"periodOrig": "morning", "weekDay": "Mon", "route": "203",
        #                           "shapeLatOrig": -25.487704010585034,
        #                           "shapeLonOrig": -25.487704010585034, "busStopIdOrig": 25739, "busStopIdDest": 25737,
        #                           "shapeLatDest":-25.48449704979423, "shapeLonDest": -49.29378227055224,
        #                           "hourOrig": 10,"isRushOrig": 1, "weekOfYear": 5, "dayOfMonth": 2, "month":2,
        #                           "isHoliday": 0, "isWeekend": 1, "isRegularDay": 0, "distance": 362.501}

        df = self.intermediate_stops_extraction_handler.extract_intermediate_stops(test_data)

        assembled_df = self.data_pre_proc(df=df,
                                          string_columns=["periodOrig", "weekDay"],#, "route"],
                                          features=["shapeLatOrig", "shapeLonOrig",
                                                    "shapeLatDest", "shapeLonDest",
                                                    "hourOrig", "isRushOrig", "weekOfYear", "dayOfMonth",
                                                    "month", "isHoliday", "isWeekend", "isRegularDay", "distance"])

        print assembled_df.columns

        prediction = self.duration_model.transform(assembled_df)

        return prediction.toJSON()

    def predictCrowdedness(self, test_data):
        # predicting_json_example = {"periodOrig": "morning", "weekDay": "Mon", "route": "203",
        #                           "shapeLatOrig": -25.487704010585034,
        #                           "shapeLonOrig": -25.487704010585034, "busStopIdOrig": 25739, "busStopIdDest": 25737,
        #                           "shapeLatDest":-25.48449704979423, "shapeLonDest": -49.29378227055224,
        #                           "hourOrig": 10,"isRushOrig": 1, "weekOfYear": 5, "dayOfMonth": 2, "month":2,
        #                           "isHoliday": 0, "isWeekend": 1, "isRegularDay": 0, "distance": 362.501}

        df = self.intermediate_stops_extraction_handler.extract_intermediate_stops(test_data)
        assembled_df = self.data_pre_proc(df=df,
                                          string_columns=["periodOrig", "weekDay"],#, "route"],
                                          features=["shapeLatOrig", "shapeLonOrig",
                                                    "shapeLatDest", "shapeLonDest",
                                                    "hourOrig", "isRushOrig", "weekOfYear", "dayOfMonth",
                                                    "month", "isHoliday", "isWeekend", "isRegularDay", "distance"])

        print "Crowdedness" , assembled_df.columns

        prediction = self.crowdedness_model.transform(assembled_df)

        return prediction.toJSON()

    def data_pre_proc(self, df, string_columns, features):

        df = df.na.drop(subset=string_columns + features)

        assembled_df = self.pipeline.transform(df)

        return assembled_df

    def close(self):
        self.sc.stop()
