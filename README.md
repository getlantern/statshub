statshub
========

statshub is a hub for statistics from Lantern clients.

It provides a basic API for setting and querying statistics using a RESTful API.

statshub is currently deployed to Heroku at http://pure-journey-3547.herokuapp.com/.

There are two types of supported stats:

Counters - these keep incrementing forever (e.g. an odometer)

Gauges - these track an absolute value that can change over time (a speedometer).  Gauges are stored in 5 minute intervals and the current value is calculated as the average of the last 6 intervals (i.e. the last 30 minutes)

Stats are identified by a string key, which is always normalized to lowercase.

Stats are always submitted for a particular userid and within a specific country code.  Stat submissions can include any number of counters and gauges.

Stats query results always include all counters and gauges for the user, as well as rollups globally and rollups for each country
from which we've received stats in the past.

### Example Session

Here we are submitting and querying stats for the user 523523.

```bash
Macintosh% curl --data-binary '{"countryCode": "es", "counter": { "mystat": 1, "myotherstat": 50 }, "gauge": {"mygauge": 78, "online": 1}}' "https://pure-journey-3547.herokuapp.com/stats/523523"
{"Succeeded":true,"Error":""}%
Macintosh% curl https://pure-journey-3547.herokuapp.com/stats/523523
{"Succeeded":true,"Error":"","user":{"counter":{"myotherstat":1244600,"mystat":24892},"gauge":{"mygauge":39,"online":0}},"rollups":{"global":{"counter":{"myotherstat":1244600,"mystat":24892},"gauge":{"mygauge":39,"online":0}},"perCountry":{"es":{"counter":{"myotherstat":1244600,"mystat":24892},"gauge":{"mygauge":39,"online":0}}}}}%
```

Pretty printed request data:

```json
{
    "counter": {
        "myotherstat": 50,
        "mystat": 1
    },
    "countryCode": "es",
    "gauge": {
        "mygauge": 78,
        "online": 1
    }
}
```

Pretty printed response data:

```json
{
    "Error": "",
    "Succeeded": true,
    "rollups": {
        "global": {
            "counter": {
                "myotherstat": 1244600,
                "mystat": 24892
            },
            "gauge": {
                "mygauge": 39,
                "online": 0
            }
        },
        "perCountry": {
            "es": {
                "counter": {
                    "myotherstat": 1244600,
                    "mystat": 24892
                },
                "gauge": {
                    "mygauge": 39,
                    "online": 0
                }
            }
        }
    },
    "user": {
        "counter": {
            "myotherstat": 1244600,
            "mystat": 24892
        },
        "gauge": {
            "mygauge": 39,
            "online": 0
        }
    }
}
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