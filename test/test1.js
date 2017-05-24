var chai = require("chai");
var chaiHttp = require("chai-http");
chai.use(chaiHttp);

var expect = chai.expect;
var server = require("../server");

describe("OTPRequestTest", function(){
  it("The OPT response must have a plan", function(){
    chai.request(server)
            .get("/get_routes_plans?city=ctba&fromPlace=-25.39211,-49.22613&toPlace=-25.45102,-49.28381&mode=TRANSIT,WALK&date=04/03/2017&time=17:20:00")
            .end((err, res) => {
                expect(res).to.have.status(500);
                expect(res).body.to.be.a("json");
                expect(res).to.have.keys("plan");
            });
  });
});
