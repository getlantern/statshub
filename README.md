## statshub

statshub is a repository for incrementally calculated statistics stored using a
[dimensional](http://en.wikipedia.org/wiki/Dimensional_modeling) model.

Stats are updated and queried using a RESTful API.

statshub for Lantern is currently deployed to Heroku at
http://pure-journey-3547.herokuapp.com/.

### Stat IDs
Every stat tracked by statshub is associated to a string id.  When stats are
updated, they are updated relative to their original values as tied to that 
string id.

statshub can roll up stats to any number of unrelated dimensions.

### Stat Types
statshub tracks two kinds of stats:

Counters - these keep incrementing forever (e.g. an odometer)

Gauges - these track an absolute value that can change over time (a
speedometer).  Statshub assumes that gauges are reported every 5 minutes.  It 
stores them in 6 minute buckets.  For rolled up gauges, in order to avoid
presenting incomplete or fluctuating information, the reported values reflect
the prior 6 minute bucket. Consequently, they can be up to 6 minutes out of
date.

### Updating Stats
Stats can be updated in one of four ways:

Counters - directly sets the value of a counter.

Increments - increments the existing value of a counter by some delta.

Gauges - directly sets the value of a gauge.

Members - tracks the value's membership in a set of unique values.  The
corresponding gauge value is calculated as the count of unique members.

MultiMembers - like Members, but allows submitting multiple members at once
instead of just one.

### Querying Stats
Stats are queried at the dimension level.  A query can ask for only a single 
dimension, or omit the dimension and receive stats for all dimensions.

For use by Lantern specifically, the statshub REST api caches queries for the
"country" dimension for 1 minute.  It does this as a performance optimization
for this very frequent query.

Query results include a couple of special items:

Totals - for every dimension, statshub returns the total across all dimension
keys.

GaugesCurrent - In addition to returning the usual Gauges (which are up to 6
minutes out of date), the query result also includes GaugesCurrent, which shows
gauges for the current period.  These are not particularly reliable because of
the reasons mentioned above, but they can be handy for testing to make sure
that updates are being recorded.

### Stat Archival
statshub archives its stats to Google Big Query every 10 minutes.  It
authenticates using OAuth and connects to a specific project, using the
environment variables `GOOGLE_PROJECT` and `GOOGLE_TOKEN`.

statshub expects Google Big Query to contain a dataset named "statshub".  It
populates one table per dimension inside this dataset.

### Example curl Session

This example session submits and queries stats for the ids "myid1" and "myid2".

Here we are submitting and querying stats for the id myid1.

#### Update
```bash
curl --data-binary \
'{"dims": {
    "country": "es",
    "user": "bob"
    },
  "counters": { "counterA": 50 },
  "increments": { "counterB": 500 },
  "gauges": { "gaugeA": 5000 },
  "members": { "gaugeB": "item1" },
  "multiMembers": { "gaugeC", ["itemI", "itemII"] }
}' \
"http://localhost:9000/stats/myid1"
```

#### Response
```bash
{"Succeeded":true,"Error":""}    
```

#### Query
```bash
curl "http://localhost:9000/stats/" | python -mjson.tool
```

#### Response
```json
{
    "Error": "",
    "Succeeded": true,
    "dims": {
        "country": {
            "es": {
                "counters": {
                    "counterA": 50,
                    "counterB": 500
                },
                "gauges": {
                    "gaugeB": 1
                },
                "gaugesCurrent": {
                    "gaugeA": 5000
                }
            },
            "total": {
                "counters": {
                    "counterA": 50,
                    "counterB": 500
                },
                "gauges": {
                    "gaugeB": 1,
                    "gaugeC": 2
                },
                "gaugesCurrent": {
                    "gaugeA": 5000
                }
            }
        },
        "user": {
            "bob": {
                "counters": {
                    "counterA": 50,
                    "counterB": 500
                },
                "gauges": {
                    "gaugeB": 1
                },
                "gaugesCurrent": {
                    "gaugeA": 5000
                }
            },
            "total": {
                "counters": {
                    "counterA": 50,
                    "counterB": 500
                },
                "gauges": {
                    "gaugeB": 1,
                    "gaugeC": 2
                },
                "gaugesCurrent": {
                    "gaugeA": 5000
                }
            }
        }
    }
}
```

#### Update
```bash
curl --data-binary \
'{"dims": {
    "country": "es",
    "user": "bob"
    },
  "counters": { "counterA": 60 },
  "increments": { "counterB": 600 },
  "gauges": { "gaugeA": 6000 },
  "members": { "gaugeB": "item2" },
  "multiMembers": { "gaugeC", ["itemI", "itemIII"] }
}' \
"http://localhost:9000/stats/myid2"
```

#### Response
```bash
{"Succeeded":true,"Error":""}  
```

#### Query
```bash
curl "http://localhost:9000/stats/country" | python -mjson.tool
```

#### Response
```json
{
    "Error": "",
    "Succeeded": true,
    "dims": {
        "country": {
            "es": {
                "counters": {
                    "counterA": 60,
                    "counterB": 1100
                },
                "gauges": {
                    "gaugeB": 2,
                    "gaugeC": 3
                },
                "gaugesCurrent": {
                    "gaugeA": 6000
                }
            },
            "total": {
                "counters": {
                    "counterA": 60,
                    "counterB": 1100
                },
                "gauges": {
                    "gaugeB": 2,
                    "gaugeC": 3
                },
                "gaugesCurrent": {
                    "gaugeA": 6000
                }
            }
        }
    }
}
```

### Running a Local Server

```bash
REDIS_ADDR=<host:port> REDIS_PASS=<password> GOOGLE_PROJECT=<project id> GOOGLE_TOKEN=<json encoded oauth config from oauther> PORT=9000 go run statshub.go
```

### Deploying to Heroku

Need to configure the Redis address and password only once (these are persistent settings in Heroku).

```bash
heroku config:set REDIS_ADDR=<host:port>
heroku config:set REDIS_PASS=mR0bKNfhlxoKIHqnBA53
heroku config:set ARCHIVE_TO_BIGQUERY=true
heroku config:set GOOGLE_PROJECT=<project id>
heroku config:set OAUTH_CONFIG=<json encoded oauth config from oauther>
```

You'll need to connect your git repo to Heroku first:

```bash
cd $GOPATH/src/github.com/getlantern/statshub
git remote add heroku git@heroku.com:pure-journey-3547.git
```

To deploy:

```bash
git commit -a -m"..." && git push
git push heroku master
```

### Deleting Keys

Sometimes, it is necessary to delete keys from the database (e.g. to clean out
corrupted/test data). This can be done using a LUA script.

For example, let's say that we need to delete all keys containing
"traversalSucceeded". Running this command inside redis-cli will do that:

```
EVAL "return redis.call('del', unpack(redis.call('keys', ARGV[1])))" 0 "*traversalSucceeded*"
```

See [StackOverflow](http://stackoverflow.com/questions/4006324/how-to-atomically-delete-keys-matching-a-pattern-using-redis)
for more details.
