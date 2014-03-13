statshub
========

statshub is a hub for statistics from Lantern clients.

It provides a basic API for setting and querying statistics using a RESTful API.

statshub is currently deployed to Heroku at http://pure-journey-3547.herokuapp.com/.

There are two types of supported stats:

Counters - these keep incrementing forever (e.g. an odometer)

Gauges - these track an absolute value that can change over time (a speedometer).  Statshub assumes that gauges are reported every 5 minutes.  It stores them in 6 minute buckets.  For detail-level gauges, the most recent reported value is given.  For aggregate gauges, the reported values reflect the prior 6 minute bucket, thus they can be up to 6 minutes out of date.

Stats are identified by a string key, which is always normalized to lowercase.

Stats are always submitted for a particular id and within a specific country code.  Stat submissions can include any number of counters and gauges.

Stats query results always include all counters and gauges at the detail level, as well as rollups globally and rollups for each country
from which we've received stats in the past.

statshub submits its stats to Google Big Query on an hourly basis.  It authenticates using OAuth and connects to a specific project,
using the environment variables `GOOGLE_PROJECT` and `GOOGLE_TOKEN`.

### Example Session

Here we are submitting and querying stats for the id 523523.

```bash
Macintosh% curl --data-binary '{"countryCode": "es", "counter": { "counter1": 5 }}' "http://localhost:9000/stats/myid"{"Succeeded":true,"Error":""}%                                                                                                                              

Macintosh% curl "http://localhost:9000/stats/myid"                                                                    
{"Succeeded":true,"Error":"","detail":{"counter":{"counter1":5},"gauge":{"everOnline":0}},"rollups":{"global":{"counter":{"counter1":5},"gauge":{"everOnline":0}},"perCountry":{"es":{"counter":{"counter1":5},"gauge":{"everOnline":0}}}}}%                                      

Macintosh% curl --data-binary '{"countryCode": "es", "counter": { "counter1": 7 }}' "http://localhost:9000/stats/myid"{"Succeeded":true,"Error":""}%                                                                                                                              

Macintosh% curl "http://localhost:9000/stats/myid"                                                                    
{"Succeeded":true,"Error":"","detail":{"counter":{"counter1":7},"gauge":{"everOnline":0}},"rollups":{"global":{"counter":{"counter1":5},"gauge":{"everOnline":0}},"perCountry":{"es":{"counter":{"counter1":5},"gauge":{"everOnline":0}}}}}%                                      

Macintosh% curl --data-binary '{"countryCode": "es", "increment": { "counter1": 9 }}' "http://localhost:9000/stats/myid"
{"Succeeded":true,"Error":""}%                                                                                                           

Macintosh% curl "http://localhost:9000/stats/myid"                                                                      
{"Succeeded":true,"Error":"","detail":{"counter":{"counter1":16},"gauge":{"everOnline":0}},"rollups":{"global":{"counter":{"counter1":5},"gauge":{"everOnline":0}},"perCountry":{"es":{"counter":{"counter1":5},"gauge":{"everOnline":0}}}}}%                                     

Macintosh% curl --data-binary '{"countryCode": "de", "increment": { "counter1": 10 }}' "http://localhost:9000/stats/myid"
{"Succeeded":true,"Error":""}%                                                                                                                                                                              
Macintosh% curl "http://localhost:9000/stats/myid"                                                                       
{"Succeeded":true,"Error":"","detail":{"counter":{"counter1":26},"gauge":{"everOnline":0}},"rollups":{"global":{"counter":{"counter1":26},"gauge":{"everOnline":0}},"perCountry":{"de":{"counter":{"counter1":10},"gauge":{"everOnline":0}},"es":{"counter":{"counter1":16},"gauge":{"everOnline":0}}}}}%                                                                                                               
Macintosh% curl --data-binary '{"countryCode": "de", "increment": { "counter2": 15 }}' "http://localhost:9000/stats/myid"
{"Succeeded":true,"Error":""}%                                                                                                                                                                              
Macintosh% curl "http://localhost:9000/stats/myid"                                                                       
{"Succeeded":true,"Error":"","detail":{"counter":{"counter1":26,"counter2":15},"gauge":{"everOnline":0}},"rollups":{"global":{"counter":{"counter1":26},"gauge":{"everOnline":0}},"perCountry":{"de":{"counter":{"counter1":10},"gauge":{"everOnline":0}},"es":{"counter":{"counter1":16},"gauge":{"everOnline":0}}}}}%                                                                                                 
Macintosh% curl --data-binary '{"countryCode": "es", "gauge": { "online": 1 }}' "http://localhost:9000/stats/myid"      
{"Succeeded":true,"Error":""}%                                                                                                                                                                              
Macintosh% date
Thu Mar 13 16:18:16 CDT 2014

Macintosh% curl "http://localhost:9000/stats/myid"
{"Succeeded":true,"Error":"","detail":{"counter":{"counter1":26,"counter2":15},"gauge":{"everOnline":1,"online":1}},"rollups":{"global":{"counter":{"counter1":26,"counter2":15},"gauge":{"everOnline":1,"online":0}},"perCountry":{"de":{"counter":{"counter1":10,"counter2":15},"gauge":{"everOnline":0,"online":0}},"es":{"counter":{"counter1":16,"counter2":0},"gauge":{"everOnline":1,"online":0}}}}}%            

Macintosh% date    
Thu Mar 13 16:23:56 CDT 2014

Macintosh% curl "http://localhost:9000/stats/myid"
{"Succeeded":true,"Error":"","detail":{"counter":{"counter1":26,"counter2":15},"gauge":{"everOnline":1,"online":1}},"rollups":{"global":{"counter":{"counter1":26,"counter2":15},"gauge":{"everOnline":1,"online":1}},"perCountry":{"de":{"counter":{"counter1":10,"counter2":15},"gauge":{"everOnline":0,"online":0}},"es":{"counter":{"counter1":16,"counter2":0},"gauge":{"everOnline":1,"online":1}}}}}%            
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
    "detail": {
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
REDIS_ADDR=<host:port> REDIS_PASS=<password> GOOGLE_PROJECT=<project id> GOOGLE_TOKEN=<json encoded oauth config from oauther> PORT=9000 go run statshub.go
```

### Deploying to Heroku

Need to configure the Redis address and password only once (these are persistent settings in Heroku).

```bash
heroku config:set REDIS_ADDR=<host:port>
heroku config:set REDIS_PASS=mR0bKNfhlxoKIHqnBA53
heroku config:set GOOGLE_PROJECT=<project id>
heroku config:set OAUTH_CONFIG=<json encoded oauth config from oauther>
```

To deploy:

```bash
git commit -a -m"..." && git push
git push heroku master
```