var chai = require("chai");
var chaiHttp = require("chai-http");
chai.use(chaiHttp);

var expect = chai.expect;
var server = require("../server");

describe("OTPRequestTest", () => {
  it("The OPT response must succeed, testing valid parameters", (done) => {
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

  it("The OTP response must return error, testing wrong parameters", (done) => {
    chai.request(server)
      .get("/get_routes_plans?city=ctba&fromPlace=25.39211,49.22613&toPlace=-25.45102,-49.28381&mode=TRANSIT,WALK&date=04/03/2017&time=17:20:00")
      .end((err, res) => {
        expect(res.body).to.have.property("error");
        done();
      });
  });
});
