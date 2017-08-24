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

import btr_otp_config

class SparkHandler:

    def __init__(self, appName, modelPath):
        self.sc = SparkContext(conf=SparkConf().setAppName(appName))
        self.sqlContext = SQLContext(self.sc)
        self.duration_model = LinearRegressionModel.load(modelPath)
        self.pipeline = Pipeline.load(btr_otp_config.PIPELINE_PATH)

    def predict(self, test_data):
        # predicting_json_example = {"periodOrig": "morning", "weekDay": "Mon", "route": "203",
        #                           "shapeLatOrig": -25.487704010585034,
        #                           "shapeLonOrig": -25.487704010585034, "busStopIdOrig": 25739, "busStopIdDest": 25737,
        #                           "shapeLatDest":-25.48449704979423, "shapeLonDest": -49.29378227055224,
        #                           "hourOrig": 10,"isRushOrig": 1, "weekOfYear": 5, "dayOfMonth": 2, "month":2,
        #                           "isHoliday": 0, "isWeekend": 1, "isRegularDay": 0, "distance": 362.501}

        df = self.createDataframeFromParams(test_data)
        assembled_df = self.data_pre_proc(df=df)

        prediction = self.duration_model.transform(assembled_df)

        return prediction.toJSON()

    # def loadModel(self, modelPath):
    #     duration_model = LinearRegressionModel.load(modelPath)
    #
    #     return duration_model

    def data_pre_proc(self, df,
                      string_columns=["periodOrig", "weekDay", "route"],
                      features=["shapeLatOrig", "shapeLonOrig",
                                "busStopIdOrig", "busStopIdDest", "shapeLatDest", "shapeLonDest",
                                "hourOrig", "isRushOrig", "weekOfYear", "dayOfMonth",
                                "month", "isHoliday", "isWeekend", "isRegularDay", "distance"]):
        df = df.na.drop(subset=string_columns + features)

        # indexers = [StringIndexer(inputCol=column, outputCol=column + "_index").fit(df) for column in string_columns]
        # pipeline = Pipeline(stages=indexers)

        # pipeline = Pipeline.load("hdfs://localhost:9000/btr/ctba/train_pipeline")

        df_r = self.pipeline.fit(df).transform(df)

        assembler = VectorAssembler(
            inputCols=features + map(lambda c: c + "_index", string_columns),
            outputCol='features')

        assembled_df = assembler.transform(df_r)

        return assembled_df

    def createDataframeFromParams(self, dictionary):
        df = self.sc.parallelize([dictionary])\
            .map(lambda d: Row(**OrderedDict(sorted((d).items()))))\
            .toDF()

        return df

    def close(self):
        self.sc.stop()
