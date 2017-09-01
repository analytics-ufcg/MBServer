import findspark
from collections import OrderedDict

findspark.init()

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import Row
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegressionModel


class SparkHandler:

    def __init__(self, app_name, model_path, pipeline_path, routes_stops_path):
        self.sc = SparkContext(conf=SparkConf().setAppName(app_name))
        self.sqlContext = SQLContext(self.sc)
        self.duration_model = LinearRegressionModel.load(model_path)
        self.pipeline = Pipeline.load(pipeline_path)
        self.routes_stops = self.get_routes_stops(routes_stops_path)

    def get_routes_stops(self, routes_stops_path):
        df = self.sqlContext.read.format("com.databricks.spark.csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("nullValue", "-") \
            .load(routes_stops_path)

        df_list_of_rows = map(lambda row: row.asDict(), df.collect())

        routes_stops = dict()
        for row in df_list_of_rows:
            route, shape_id, bus_stop, distance = row["route"], row["shapeId"], row["busStopId"], row["distanceTraveledShape"]

            if route not in routes_stops:
                routes_stops[route] = dict()
            if shape_id not in routes_stops[route]:
                routes_stops[route][shape_id] = list()

            routes_stops[route][shape_id].append((bus_stop, distance))

        routes_stops_final = dict()
        for route in routes_stops:
            routes_stops_final[route] = sorted(routes_stops[route].values(), key=len, reverse=True)

        return routes_stops_final

    def predict(self, test_data):
        # predicting_json_example = {"periodOrig": "morning", "weekDay": "Mon", "route": "203",
        #                           "shapeLatOrig": -25.487704010585034,
        #                           "shapeLonOrig": -25.487704010585034, "busStopIdOrig": 25739, "busStopIdDest": 25737,
        #                           "shapeLatDest":-25.48449704979423, "shapeLonDest": -49.29378227055224,
        #                           "hourOrig": 10,"isRushOrig": 1, "weekOfYear": 5, "dayOfMonth": 2, "month":2,
        #                           "isHoliday": 0, "isWeekend": 1, "isRegularDay": 0, "distance": 362.501}

        df = self.create_dataframe_from_params(test_data)
        assembled_df = self.data_pre_proc(df=df,
                                          string_columns=["periodOrig", "weekDay", "route"],
                                          features=["shapeLatOrig", "shapeLonOrig",
                                                    "busStopIdOrig", "busStopIdDest", "shapeLatDest", "shapeLonDest",
                                                    "hourOrig", "isRushOrig", "weekOfYear", "dayOfMonth",
                                                    "month", "isHoliday", "isWeekend", "isRegularDay", "distance"])

        prediction = self.duration_model.transform(assembled_df)

        return prediction.toJSON()

    def data_pre_proc(self, df, string_columns, features):
        df = df.na.drop(subset=string_columns + features)

        df_r = self.pipeline.fit(df).transform(df)

        assembler = VectorAssembler(
            inputCols=features + map(lambda c: c + "_index", string_columns),
            outputCol='features')

        assembled_df = assembler.transform(df_r)

        return assembled_df

    def both_on_list(self, e1, e2, list):
        found_e1 = False
        found_e2 = False
        for e in list:
            if e[0] == e1:
                found_e1 = True
            if e[0] == e2:
                found_e2 = True
        return found_e1 and found_e2

    def create_dataframe_from_params(self, list_of_params):
        df = self.sc.parallelize(list_of_params)\
            .map(lambda d: Row(**OrderedDict(sorted(d.items()))))\
            .toDF()

        df_dict = map(lambda row: row.asDict(), df.collect())

        new_df_list = list()

        for row in df_dict:
            route, bus_stop_orig, bus_stop_dest = row["route"], row["busStopIdOrig"], row["busStopIdDest"]
            found_shape = False
            for shape_stops_list in self.routes_stops[route]:
                if self.both_on_list(bus_stop_orig, bus_stop_dest, shape_stops_list):
                    found_shape = True
                    i = 0
                    found_first = False
                    found_last = False
                    while not (found_first and found_last):
                        idx = i % len(shape_stops_list)
                        if shape_stops_list[idx][0] == bus_stop_orig:
                            found_first = True
                        if found_first and shape_stops_list[idx][0] == bus_stop_dest:
                            found_last = True

                        if found_first:
                            row_copy = row.copy()
                            row_copy["busStopIdOrig"] = shape_stops_list[idx][0]
                            row_copy["busStopIdDest"] = shape_stops_list[(idx + 1) % len(shape_stops_list)][0]
                            row_copy["distance"] = abs(shape_stops_list[(idx + 1) % len(shape_stops_list)][1]
                                                       - shape_stops_list[idx][1])
                            new_df_list.append(row_copy)

                        i += 1

            if not found_shape:
                new_df_list.append(row)

        print new_df_list

        new_df = self.sc.parallelize(new_df_list).toDF()

        return new_df

    def close(self):
        self.sc.stop()
