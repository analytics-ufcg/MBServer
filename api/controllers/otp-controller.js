'use strict';

var config = require('config');
var request = require('request');
var qs = require('query-string');

var optConfig = config.get('OTP');

exports.get_routes = function(req, res) {
  if (req.query.city == "cg") {
    var new_url = optConfig.campinagrande.url + '?' + qs.stringify(req.query);
  } else if (req.query.city == "ctba") {
    var new_url = optConfig.curitiba.url + '?' + qs.stringify(req.query);
  }
  request.get(new_url).pipe(res);
};
