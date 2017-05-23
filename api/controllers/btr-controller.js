'use strict';

var config = require('config');
var request = require('request');
var qs = require('query-string');

var btrConfig = config.get('BTR');

exports.get_best_trips = function(req, res) {
  var new_url = btrConfig.campinagrande.url + req._parsedUrl.pathname + '?' + qs.stringify(req.query);
  request.get(new_url).pipe(res);
};
