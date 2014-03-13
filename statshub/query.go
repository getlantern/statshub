package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

// QueryResponse is a Response to a StatsQuery
type QueryResponse struct {
	Detail     *Stats            `json:"id"`         // Detailed stats
	Global     *Stats            `json:"global"`     // Global stats
	PerCountry map[string]*Stats `json:"perCountry"` // Maps country codes to stats for those countries
}

// query runs a query for a given id, optionally including global and
// country rollups depending on the value of includeRollups.
func Query(id string, includeRollups bool) (resp *QueryResponse, err error) {
	var conn redis.Conn
	conn, err = connectToRedis()
	if err != nil {
		err = fmt.Errorf("Unable to connect to redis: %s", err)
		return
	}
	defer conn.Close()

	resp = &QueryResponse{
		Detail:     newStats(),
		Global:     newStats(),
		PerCountry: make(map[string]*Stats),
	}

	var countries []string
	if countries, err = listCountries(conn); err != nil {
		return
	}

	if err = queryCounters(conn, countries, id, resp, includeRollups); err != nil {
		return
	}

	err = queryGauges(conn, countries, id, resp, includeRollups)

	return
}

// queryCounters queries simple counter statistics
func queryCounters(conn redis.Conn, countries []string, id string, resp *QueryResponse, includeRollups bool) (err error) {
	var counterKeys []string
	if counterKeys, err = listStatKeys(conn, "counter"); err != nil {
		return
	}

	for _, key := range counterKeys {
		detailKey := redisKey("counter", fmt.Sprintf("detail:%s", id), key)
		globalKey := redisKey("counter", "global", key)
		err = conn.Send("GET", detailKey)
		if includeRollups {
			err = conn.Send("GET", globalKey)
			for _, countryCode := range countries {
				countryKey := redisKey("counter", fmt.Sprintf("country:%s", countryCode), key)
				err = conn.Send("GET", countryKey)
			}
		}
	}

	err = conn.Flush()

	for _, key := range counterKeys {
		var val int64
		if val, _, err = receive(conn); err != nil {
			return
		}
		resp.Detail.Counter[key] = val

		if includeRollups {
			if val, _, err = receive(conn); err != nil {
				return
			}
			resp.Global.Counter[key] = val

			for _, countryCode := range countries {
				if val, _, err = receive(conn); err != nil {
					return
				}
				countryStats := resp.PerCountry[countryCode]
				if countryStats == nil {
					countryStats = newStats()
					resp.PerCountry[countryCode] = countryStats
				}
				countryStats.Counter[key] = val
			}
		}
	}

	return
}

// queryGauges queries simple gauge statistics
// TODO: this is a lot like queryCounters, might be nice to reduce the repetition
func queryGauges(conn redis.Conn, countries []string, id string, resp *QueryResponse, includeRollups bool) (err error) {
	currentPeriod := time.Now().Truncate(statsPeriod)
	priorPeriod := currentPeriod.Add(-1 * statsPeriod)

	var gaugeKeys []string
	if gaugeKeys, err = listStatKeys(conn, "gauge"); err != nil {
		return
	}

	for _, key := range gaugeKeys {
		detailKey := redisKey("gauge", fmt.Sprintf("detail:%s", id), keyForPeriod(key, currentPeriod))
		detailKeyPrior := redisKey("gauge", fmt.Sprintf("detail:%s", id), keyForPeriod(key, priorPeriod))
		globalKey := redisKey("gauge", "global", keyForPeriod(key, priorPeriod))
		err = conn.Send("GET", detailKey)
		err = conn.Send("GET", detailKeyPrior)

		if includeRollups {
			err = conn.Send("GET", globalKey)
			for _, countryCode := range countries {
				countryKey := redisKey("gauge", fmt.Sprintf("country:%s", countryCode), keyForPeriod(key, priorPeriod))
				err = conn.Send("GET", countryKey)
			}
		}
	}

	// Special treatment for "everOnline" gauge on non-fallbacks
	everOnlineKey := redisKey("gauge", fmt.Sprintf("detail:%s", id), "everOnline")
	globalEverOnlineKey := redisKey("gauge", "global", "everOnline")
	conn.Send("GET", everOnlineKey)
	if includeRollups {
		conn.Send("SCARD", globalEverOnlineKey)
		for _, countryCode := range countries {
			countryEverOnlineKey := redisKey("gauge", fmt.Sprintf("country:%s", countryCode), "everOnline")
			err = conn.Send("SCARD", countryEverOnlineKey)
		}
	}

	err = conn.Flush()

	for _, key := range gaugeKeys {
		var val, currentVal int64
		currentValueFound := false
		if currentVal, currentValueFound, err = receive(conn); err != nil {
			return
		}
		if val, _, err = receive(conn); err != nil {
			return
		}
		if currentValueFound {
			resp.Detail.Gauge[key] = currentVal
		} else {
			resp.Detail.Gauge[key] = val
		}

		if includeRollups {
			if val, _, err = receive(conn); err != nil {
				return
			}
			resp.Global.Gauge[key] = val

			for _, countryCode := range countries {
				if val, _, err = receive(conn); err != nil {
					return
				}
				countryStats := resp.PerCountry[countryCode]
				if countryStats == nil {
					countryStats = newStats()
					resp.PerCountry[countryCode] = countryStats
				}
				countryStats.Gauge[key] = val
			}
		}
	}

	// Special treatment for "everOnline" gauge on non-fallbacks
	var everOnline int64
	if everOnline, _, err = receive(conn); err != nil {
		return
	}
	resp.Detail.Gauge["everOnline"] = everOnline
	if includeRollups {
		if everOnline, _, err = receive(conn); err != nil {
			return
		}
		resp.Global.Gauge["everOnline"] = everOnline
		for _, countryCode := range countries {
			if everOnline, _, err = receive(conn); err != nil {
				return
			}
			resp.PerCountry[countryCode].Gauge["everOnline"] = everOnline
		}
	}

	return
}

// keyForPeriod constructs a redis key from a base key plus a given time period
func keyForPeriod(key string, period time.Time) string {
	return fmt.Sprintf("%s:%d", key, period.Unix())
}
