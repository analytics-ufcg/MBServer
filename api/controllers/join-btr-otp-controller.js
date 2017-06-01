var rp = require("request-promise");
var qs = require("query-string");
var df = require("dateformat");
var JSONStream = require('JSONStream');

var getBTR = function(data) {
  var leg = data.leg;
  var city = data.city;
  if (leg.mode == "BUS") {
    var datetime = new Date(leg.startTime);
    var request = {
      city: city,
      route: leg.route,
      bus_stop_id: leg.from.stopId.split(":")[1],
      date: df(datetime, "yyyy-mm-dd"),
      time: df(datetime, "HH:MM:ss"),
      closest_trip_type: "single_trip"
    };

    return new Promise(function(resolve, reject){
      resolve(
        rp("http://localhost:3000/get_best_trips?" + qs.stringify(request)).then(function(data){
            var json = JSON.parse(data)[0];
            return {
              passenger_number: json["passengers.number"],
              trip_duration: json["trip.duration"]
            };

          })
      );
    });
  } else {
    return new Promise(function(resolve, reject){
      resolve({
        trip_duration: leg.duration,
        passenger_number: -1
      });
    });
  }
};

exports.get_btr_routes_plans = function(req, res){
  rp("http://localhost:3000/get_routes_plans?" + qs.stringify(req.query))
    .then(function(data){
      var parsedData = JSON.parse(data);
      var promises = [];
      var itineraries = parsedData.plan.itineraries;
      for (var i = 0; i < itineraries.length; i++) {

        var data2 = itineraries[i].legs.map(
          function(leg) {
            return {leg: leg, city: req.query.city};
          }
        );

        promises.push(data2.map(getBTR));
        // console.log(promises);
        // var results = Promise.all(promises);
        // results.push(promises);
      }
      Promise.all(promises).then(function(data3){
        // console.log(data);
        // console.log(itineraries[i]);
        // console.log(JSON.parse(data).plan.itineraries);
        // console.log(data3);
        // var data = JSON.parse(data);
        var result = JSON.parse(data);
        for (var i = 0; i < data3.length; i++) {
          console.log(data3);
          // console.log(result.plan.itineraries[i]);
          result.plan.itineraries[i]["btr_passengers"] = data3[i].reduce(function(d1, d2){
            return d1["passenger_number"] > d2["passenger_number"] ? d1["passenger_number"] : d2["passenger_number"];
          });
          result.plan.itineraries[i]["trip_duration"] = data3[i].reduce(function(d1, d2){
            return d1["trip_duration"] + d2["trip_duration"];
          });
          // console.log(result.plan.itineraries[i]);
        }
      });
    })
    .catch(function(err){
      console.error(err);
    })
};
