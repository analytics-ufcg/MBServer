'use strict';
module.exports = function(app) {
  var btrController = require('../controllers/btr-controller');


  // todoList Routes
  app.route('/get_best_trips')
    .get(btrController.get_best_trips)
};
