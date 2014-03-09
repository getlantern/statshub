package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

// QueryResponse is a Response to a StatsQuery
type QueryResponse struct {
	Response
	User       *Stats            `json:"user"`       // Stats for the user
	Global     *Stats            `json:"global"`     // Global stats
	PerCountry map[string]*Stats `json:"perCountry"` // Maps country codes to stats for those countries
}

func query(conn redis.Conn, userId int64) (resp *QueryResponse, err error) {
	resp = &QueryResponse{
		User:       newStats(),
		Global:     newStats(),
		PerCountry: make(map[string]*Stats),
	}

	if err = queryCounters(conn, userId, resp); err != nil {
		return
	}

	err = queryGauges(conn, userId, resp)

	return
}

func queryCounters(conn redis.Conn, userId int64, resp *QueryResponse) (err error) {
	var counterKeys []string
	if counterKeys, err = listStatKeys(conn, "counter"); err != nil {
		return
	}
	for _, key := range counterKeys {
		userKey := redisKey("counter", fmt.Sprintf("user:%d", userId), key)
		globalKey := redisKey("counter", "global", key)
		if err = conn.Send("GET", userKey); err != nil {
			return
		}
		if err = conn.Send("GET", globalKey); err != nil {
			return
		}
		for _, countryCode := range allCountryCodes {
			countryKey := redisKey("counter", fmt.Sprintf("country:%s", countryCode), key)
			if err = conn.Send("GET", countryKey); err != nil {
				return
			}
		}
	}
	conn.Flush()
	for _, key := range counterKeys {
		var val int64
		if val, err = receive(conn); err != nil {
			return
		}
		resp.User.Counter[key] = val

		if val, err = receive(conn); err != nil {
			return
		}
		resp.Global.Counter[key] = val

		for _, countryCode := range allCountryCodes {
			if val, err = receive(conn); err != nil {
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

	return
}

func queryGauges(conn redis.Conn, userId int64, resp *QueryResponse) (err error) {
	periods := make([]int64, lookbackPeriods)
	start := time.Now().Truncate(gaugePeriod).Add(-1 * (lookbackPeriods - 1) * gaugePeriod)
	for i := 0; i < lookbackPeriods; i++ {
		periods[i] = start.Add(time.Duration(i) * gaugePeriod).Unix()
	}

	var gaugeKeys []string
	if gaugeKeys, err = listStatKeys(conn, "gauge"); err != nil {
		return
	}
	for _, key := range gaugeKeys {
		for _, period := range periods {
			userKey := redisKey("gauge", fmt.Sprintf("user:%d", userId), keyForPeriod(key, period))
			log.Printf("User key: %s", userKey)
			globalKey := redisKey("gauge", "global", keyForPeriod(key, period))
			if err = conn.Send("GET", userKey); err != nil {
				return
			}
			if err = conn.Send("GET", globalKey); err != nil {
				return
			}
			for _, countryCode := range allCountryCodes {
				countryKey := redisKey("gauge", fmt.Sprintf("country:%s", countryCode), keyForPeriod(key, period))
				if err = conn.Send("GET", countryKey); err != nil {
					return
				}
			}
		}
	}
	conn.Flush()
	for _, key := range gaugeKeys {
		userTotal := int64(0)
		globalTotal := int64(0)
		countryTotals := make(map[string]int64)

		for i := 0; i < lookbackPeriods; i++ {
			var val int64
			if val, err = receive(conn); err != nil {
				return
			}
			userTotal += val

			if val, err = receive(conn); err != nil {
				return
			}
			globalTotal += val

			for _, countryCode := range allCountryCodes {
				if val, err = receive(conn); err != nil {
					return
				}
				countryTotals[countryCode] += val
			}
		}

		log.Printf("%s: %d", key, userTotal)
		resp.User.Gauge[key] = userTotal / lookbackPeriods
		resp.Global.Gauge[key] = globalTotal / lookbackPeriods
		for _, countryCode := range allCountryCodes {
			countryStats := resp.PerCountry[countryCode]
			if countryStats == nil {
				countryStats = newStats()
				resp.PerCountry[countryCode] = countryStats
			}
			countryStats.Gauge[key] = countryTotals[countryCode] / lookbackPeriods
		}
	}

	return
}

func keyForPeriod(key string, period int64) string {
	return fmt.Sprintf("%s:%d", key, period)
}

func dontCallMe() {
	log.Fatalf("Dammit, you called me!")
}
