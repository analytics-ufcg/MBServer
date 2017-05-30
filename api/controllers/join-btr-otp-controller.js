var request = require("request");
var qs = require("query-string");

exports.get_btr_routes_plans = function(req, res){
  request.get("http://localhost:3000/get_routes_plans?" +
            qs.stringify(req.query), function(error, response, body){
              // for(var i = 0; i < body.itineraries.length; i++) {
              //   console.log(body.itineraries[i]);
              // }
            }).pipe(res);

  // res = plans;
};
