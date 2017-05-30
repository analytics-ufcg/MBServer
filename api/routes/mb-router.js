'use strict';
module.exports = function(app) {
  var btrController = require('../controllers/btr-controller');
  var otpController = require("../controllers/otp-controller");
  var joinBTRandOPTController = require("../controllers/join-btr-otp-controller");

  // todoList Routes
  app.route('/get_best_trips')
    .get(btrController.get_best_trips);

  app.route("/get_routes_plans")
    .get(otpController.get_routes);

  app.route("/get_btr_routes_plans")
    .get(joinBTRandOPTController.get_btr_routes_plans);
};
