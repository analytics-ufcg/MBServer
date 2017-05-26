var chai = require("chai");
var chaiHttp = require("chai-http");
chai.use(chaiHttp);

var expect = chai.expect;
var server = require("../server");

describe("OTPRequestTest", () => {
  it("The OPT ctba response must succeed, testing valid parameters", (done) => {
    chai.request(server)
      .get("/get_routes_plans?city=ctba&fromPlace=-25.39211,-49.22613&toPlace=-25.45102,-49.28381&mode=TRANSIT,WALK&date=04/03/2017&time=17:20:00")
      .end((err, res) => {
          expect(res).to.have.status(200);
          expect(res.body).to.not.have.property("error");
          expect(res.body).to.be.an("object");
          expect(res.body).to.contain.keys("plan");
          expect(res.body.plan).to.contain.keys("date", "from", "to", "itineraries");
          expect(res.body.plan.itineraries).to.be.not.empty;
          done();
      });
  });

  it("The OTP ctba response must return error, testing wrong parameters", (done) => {
    chai.request(server)
      .get("/get_routes_plans?city=ctba&fromPlace=25.39211,49.22613&toPlace=-25.45102,-49.28381&mode=TRANSIT,WALK&date=04/03/2017&time=17:20:00")
      .end((err, res) => {
        expect(res.body).to.have.property("error");
        done();
      });
  });

  it("The OPT cg response must succeed, testing valid parameters", (done) => {
    chai.request(server)
      .get("/get_routes_plans?city=cg&fromPlace=-7.213447,%20-35.907584&toPlace=-7.220330,%20-35.885446&mode=TRANSIT,WALK&date=04/03/2017&time=17:20:00")
      .end((err, res) => {
          expect(res).to.have.status(200);
          expect(res.body).to.not.have.property("error");
          expect(res.body).to.be.an("object");
          expect(res.body).to.contain.keys("plan");
          expect(res.body.plan).to.contain.keys("date", "from", "to", "itineraries");
          expect(res.body.plan.itineraries).to.be.not.empty;
          done();
      });
  });

  it("The OTP cg response must return error, testing wrong parameters", (done) => {
    chai.request(server)
      .get("/get_routes_plans?city=cg&fromPlace=25.39211,49.22613&toPlace=-25.45102,-49.28381&mode=TRANSIT,WALK&date=04/03/2017&time=17:20:00")
      .end((err, res) => {
        expect(res.body).to.have.property("error");
        done();
      });
  });
});

describe("BTRRequestTest", () => {
  it("The BTR cg response must succeed, testing valid parameters", (done) => {
    chai.request(server)
      .get("/get_best_trips?city=cg&route=0303&time=16:20:55&date=2016-12-20&bus_stop_id=451&closest_trip_type=single_trip")
      .end((err, res) => {
          expect(res).to.have.status(200);
          expect(res.body).to.be.an("array");
          expect(res.body).to.not.be.empty;
          expect(res.body).to.have.lengthOf(1);
          expect(res.body[0]).to.contain.keys("route", "date", "mean.timetable", "passengers.number", "trip.duration");
          done();
      });
  });

  it("The BTR cg response must return error, testing wrong parameters", (done) => {
    chai.request(server)
      .get("/get_best_trips?city=cg&route=9874&time=16:20:55&date=2016-12-20&bus_stop_id=451&closest_trip_type=single_trip")
      .end((err, res) => {
        expect(res.body).to.be.an("array");
        expect(res.body[0]).to.be.an("string");
        done();
      });
  });

  it("The BTR ctba response must succeed, testing valid parameters", (done) => {
    chai.request(server)
      .get("/get_best_trips?city=ctba&route=022&time=10:00:00&date=2016-10-26&bus_stop_id=26276&closest_trip_type=single_trip")
      .end((err, res) => {
          expect(res).to.have.status(200);
          expect(res.body).to.be.an("array");
          expect(res.body).to.not.be.empty;
          expect(res.body).to.have.lengthOf(1);
          expect(res.body[0]).to.contain.keys("route", "date", "mean.timetable", "passengers.number", "trip.duration");
          done();
      });
  });

  it("The BTR ctba response must return error, testing wrong parameters", (done) => {
    chai.request(server)
      .get("/get_best_trips?city=ctba&route=9879&time=10:00:00&date=2016-10-26&bus_stop_id=26276&closest_trip_type=single_trip")
      .end((err, res) => {
        expect(res.body).to.be.an("array");
        expect(res.body[0]).to.be.an("string");
        done();
      });
  });
});
