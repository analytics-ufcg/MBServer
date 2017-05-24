var express = require('express'),
  app = express(),
  port = process.env.PORT || 3000,
  bodyParser = require('body-parser');

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

var routes = require('./api/routes/mb-router');
routes(app);

app.listen(port);

console.log('Melhor Busão RESTful API server started on: ' + port);

module.exports = app;
