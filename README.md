statshub
========

statshub is a hub for statistics from Lantern clients.

It provides a basic API for setting and querying statistics using a RESTful API.

statshub is currently deployed to Heroku at http://pure-journey-3547.herokuapp.com/.

There are two types of supported stats:

Counters - these keep incrementing forever (e.g. an odometer)

Gauges - these track an absolute value that can change over time (a speedometer).  Gauges are stored in 5 minute intervals and the current value is calculated as the average of the last 6 intervals (i.e. the last 30 minutes)

### Example Session

```bash
Macintosh% curl --data-binary '{"countryCode": "es", "counter": { "mystat": 1, "myotherstat": 50 }, "gauge": {"mygauge": 78, "online": 1}}' "https://pure-journey-3547.herokuapp.com/stats/523523"
{"Succeeded":true,"Error":""}%
Macintosh% curl https://pure-journey-3547.herokuapp.com/stats/523523
{"Succeeded":true,"Error":"","user":{"counter":{"myotherstat":1244600,"mystat":24892},"gauge":{"mygauge":39,"online":0}},"rollups":{"global":{"counter":{"myotherstat":1244600,"mystat":24892},"gauge":{"mygauge":39,"online":0}},"perCountry":{"es":{"counter":{"myotherstat":1244600,"mystat":24892},"gauge":{"mygauge":39,"online":0}}}}}%
```

### Running Local Server

```bash
REDIS_ADDR=<host:port> REDIS_PASS=<password> PORT=9000 go run statshub.go
```

### Deploying to Heroku

Need to configure the Redis address and password only once (these are persistent settings in Heroku).

```bash
heroku config:set REDIS_ADDR=<host:port>
heroku config:set REDIS_PASS=mR0bKNfhlxoKIHqnBA53
```

To deploy:

```bash
git commit -a -m"..." && git push
git push heroku master
```