package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

// QueryResponse is a Response to a StatsQuery
type QueryResponse struct {
	User       *Stats            `json:"user"`       // Stats for the user
	Global     *Stats            `json:"global"`     // Global stats
	PerCountry map[string]*Stats `json:"perCountry"` // Maps country codes to stats for those countries
}

// query runs a query for a given userId, optionally including global and
// country rollups depending on the value of includeRollups.
func query(conn redis.Conn, userId string, includeRollups bool) (resp *QueryResponse, err error) {
	resp = &QueryResponse{
		User:       newStats(),
		Global:     newStats(),
		PerCountry: make(map[string]*Stats),
	}

	var countries []string
	if countries, err = listCountries(conn); err != nil {
		return
	}

	if err = queryCounters(conn, countries, userId, resp, includeRollups); err != nil {
		return
	}

	err = queryGauges(conn, countries, userId, resp, includeRollups)

	return
}

// queryCounters queries simple counter statistics
func queryCounters(conn redis.Conn, countries []string, userId string, resp *QueryResponse, includeRollups bool) (err error) {
	var counterKeys []string
	if counterKeys, err = listStatKeys(conn, "counter"); err != nil {
		return
	}

	for _, key := range counterKeys {
		userKey := redisKey("counter", fmt.Sprintf("user:%s", userId), key)
		globalKey := redisKey("counter", "global", key)
		err = conn.Send("GET", userKey)
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
		resp.User.Counter[key] = val

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
func queryGauges(conn redis.Conn, countries []string, userId string, resp *QueryResponse, includeRollups bool) (err error) {
	currentPeriod := time.Now().Truncate(statsPeriod)
	priorPeriod := currentPeriod.Add(-1 * statsPeriod)

	var gaugeKeys []string
	if gaugeKeys, err = listStatKeys(conn, "gauge"); err != nil {
		return
	}

	for _, key := range gaugeKeys {
		userKey := redisKey("gauge", fmt.Sprintf("user:%s", userId), keyForPeriod(key, currentPeriod))
		userKeyPrior := redisKey("gauge", fmt.Sprintf("user:%s", userId), keyForPeriod(key, priorPeriod))
		globalKey := redisKey("gauge", "global", keyForPeriod(key, priorPeriod))
		err = conn.Send("GET", userKey)
		err = conn.Send("GET", userKeyPrior)

		if includeRollups {
			err = conn.Send("GET", globalKey)
			for _, countryCode := range countries {
				countryKey := redisKey("gauge", fmt.Sprintf("country:%s", countryCode), keyForPeriod(key, priorPeriod))
				err = conn.Send("GET", countryKey)
			}
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
			resp.User.Gauge[key] = currentVal
		} else {
			resp.User.Gauge[key] = val
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

	return
}

// keyForPeriod constructs a redis key from a base key plus a given time period
func keyForPeriod(key string, period time.Time) string {
	return fmt.Sprintf("%s:%d", key, period.Unix())
}
