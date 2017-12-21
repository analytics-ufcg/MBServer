# BTR API

Melhor Busao Server (MBServer) is a Web Service which exposes the Best Trip Recommender (BTR) features to applications.

## Usage

### Bus Trip Duration Prediction

One of the features of BTR is the prediction of bus trips duration. The input is the user trip origin, destination, date and time. In order to retrieve predictions from the API, applications must send a POST request to the server, in the format specified below:

| Field | Specification |
| -------- | -------- |
| Endpoint  | /btr_routes_plans |
| Method | POST |
| Content Type  | application/json  |
| Returned Value | A JSON formated string containing the suggested itineraries with their predicted duration. |

| Argument | Description |
| -------- | -------- |
| fromPlace  | comma separated string with latitude and longitude |
| toPlace    | comma separated string with latitude and longitude |
| mode       | "TRANSIT,WALK" |
| date       | string using the format “mm/dd/YYYY” |
| time       | string using the format “HH:MM:SS”  |

### Sample Call

```
$ curl -X POST \
  http://<host:port>/btr_routes_plans \
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -d '{
  "fromPlace": "-25.39211,-49.22613",
  "toPlace": "-25.45102,-49.28381",
  "mode": "TRANSIT,WALK",
  "date": "04/06/2017",
  "time": "17:20:00"
}'
```

### Sample response
Click [here](https://jsonblob.com/c6196ad8-9974-11e7-aa97-c720a295cde5).

### Useful information

1. The itineraries are inside the plan attribute. 
2. An itinerary is a list of steps of how you can go from one location to another. Each step is called “leg”.
3. A leg is a description of how you can move between two locations without the need to change the way you’re moving (by bus or walking).
4. An example of one itinerary: “walk 200m from A to B then take the bus 222 from B to C then walk 50m from C to D then take the bus 300 from D to E”
5. The BTR 2.0 predict the duration of all bus legs and in the end aggregates all durations. Meaning that in the response, each itinerary will have an attribute called “btr-duration” representing the full duration of the itinerary (accounting the duration of all legs).
6. Also, each itinerary/leg has information that can be used to plot it on a map.












