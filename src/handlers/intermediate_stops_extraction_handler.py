# coding: UTF-8

class IntermediateStopsExtractionHandler:

    def __init__(self, sc, sqlContext, routes_stops_path):
        self.sc = sc
        self.sqlContext = sqlContext
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

    def both_on_list(self, e1, e2, list):
        found_e1 = False
        found_e2 = False
        for e in list:
            if e[0] == e1:
                found_e1 = True
            if e[0] == e2:
                found_e2 = True
        return found_e1 and found_e2

    def extract_intermediate_stops(self, features_list):
        new_features_list = list()

        for feats in features_list:
            route, bus_stop_orig, bus_stop_dest = feats["route"], feats["busStopIdOrig"], feats["busStopIdDest"]
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
                            row_copy = feats.copy()
                            row_copy["busStopIdOrig"] = shape_stops_list[idx][0]
                            row_copy["busStopIdDest"] = shape_stops_list[(idx + 1) % len(shape_stops_list)][0]
                            row_copy["distance"] = abs(shape_stops_list[(idx + 1) % len(shape_stops_list)][1]
                                                       - shape_stops_list[idx][1])
                            new_features_list.append(row_copy)

                        i += 1

            if not found_shape:
                new_features_list.append(feats)

        new_df = self.sc.parallelize(new_features_list).toDF()

        return new_df

    def close(self):
        self.sc.stop()
