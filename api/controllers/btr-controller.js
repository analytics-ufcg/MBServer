'use strict';

var config = require('config');
var request = require('request');

var btrConfig = config.get('BTR');

exports.get_best_trips = function(req, res) {
  var test_url = btrConfig.domain.url + 'get_best_trips?route=0500&time=14:23:00&date=2016-09-01&bus_stop_id=97';
  request.get(test_url).pipe(res);
};
