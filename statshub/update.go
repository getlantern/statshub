package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strings"
	"time"
)

// StatsUpdate posts stats from within a specific country.  Stats
// include Counter (cumulative) and Gauges (point in time)
// (online/offline).
type StatsUpdate struct {
	CountryCode string `json:"countryCode"`
	Stats
}

// postToRedis posts Counter and Gauge for the given id to redis
// using INCRBY and SET respectively.
func (stats *StatsUpdate) postToRedis(id string) (err error) {
	// Always treat countries as lower case
	stats.CountryCode = strings.ToLower(stats.CountryCode)

	if err = writeToRedis(id, stats.CountryCode, stats.Counter, "counter"); err != nil {
		return
	}
	err = writeToRedis(id, stats.CountryCode, stats.Gauge, "gauge")

	return
}

// writeToRedis does the actual writing to redis.
// statType is either "counter" or "gauge".
// counters and gauges are recorded similarly, but gauges are bucked with expiration
// to account for the fact that their values aren't fully persistent.
func writeToRedis(id string, countryCode string, values map[string]int64, statType string) (err error) {
	var conn redis.Conn
	if conn, err = connectToRedis(); err != nil {
		return
	}
	defer conn.Close()

	now := time.Now()
	now = now.Truncate(statsPeriod)
	expiration := now.Add(3 * statsPeriod)

	keyArgs := make([]interface{}, len(values)+1)
	keyArgs[0] = "key:" + statType
	i := 1

	// For gauges, use expiring logic
	expire := "gauge" == statType

	// keyWithDate adds a date to the key, but only if expire == true
	keyWithDate := func(key string) string {
		if expire {
			return fmt.Sprintf("%s:%d", key, now.Unix())
		} else {
			return key
		}
	}

	// Set detail level
	for key, value := range values {
		keyArgs[i] = key
		i++
		redisKey := redisKey(statType, fmt.Sprintf("detail:%s", id), keyWithDate(key))
		err = conn.Send("GETSET", redisKey, value)
	}

	if expire {
		// The reason we don't do EXPIREAT in the above loop is that the code for
		// country and global rollups needs to read the return values from GETSET,
		// and we don't want to bother with interleaving those with the EXPIREAT
		// return values.
		for key, _ := range values {
			redisKey := redisKey(statType, fmt.Sprintf("detail:%s", id), keyWithDate(key))
			err = conn.Send("EXPIREAT", redisKey, expiration.Unix())
		}
	}

	err = conn.Flush()

	// Roll up to country and global level
	for key, value := range values {
		var oldValue int64
		if oldValue, _, err = receive(conn); err != nil {
			return
		}
		delta := value - oldValue
		countryKey := redisKey(statType, fmt.Sprintf("country:%s", countryCode), keyWithDate(key))
		globalKey := redisKey(statType, "global", keyWithDate(key))
		err = conn.Send("INCRBY", countryKey, delta)
		err = conn.Send("INCRBY", globalKey, delta)
		if expire {
			err = conn.Send("EXPIREAT", countryKey, expiration.Unix())
			err = conn.Send("EXPIREAT", globalKey, expiration.Unix())
		}
	}

	// Remember keys
	err = conn.Send("SADD", keyArgs...)

	// Remember country
	err = conn.Send("SADD", "countries", countryCode)

	err = conn.Flush()
	return
}

// withLowerCaseKeys converts the keys in a map to lower case, returning a new
// map with the lower cased keys.
func withLowerCaseKeys(values map[string]uint64) (lowerCased map[string]uint64) {
	lowerCased = make(map[string]uint64)
	for key, value := range values {
		lowerCased[strings.ToLower(key)] = value
	}
	return
}
