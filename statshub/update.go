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

	if err = writeIncrement(id, stats); err != nil {
		return
	}
	if err = writeToRedis("counter", id, stats.CountryCode, stats.Counter); err != nil {
		return
	}
	err = writeToRedis("gauge", id, stats.CountryCode, stats.Gauge)

	return
}

// writeIncrement increments counters in redis
func writeIncrement(id string, stats *StatsUpdate) (err error) {
	var conn redis.Conn
	if conn, err = connectToRedis(); err != nil {
		return
	}
	defer conn.Close()

	values := stats.Increment
	keyArgs := make([]interface{}, len(values)+1)
	keyArgs[0] = "key:counter"
	i := 1

	for key, value := range values {
		keyArgs[i] = key
		i++
		detailKey := redisKey("counter", fmt.Sprintf("detail:%s", id), key)
		countryKey := redisKey("counter", fmt.Sprintf("country:%s", stats.CountryCode), key)
		globalKey := redisKey("counter", "global", key)
		err = conn.Send("INCRBY", detailKey, value)
		err = conn.Send("INCRBY", countryKey, value)
		err = conn.Send("INCRBY", globalKey, value)
	}

	// Remember counter keys
	err = conn.Send("SADD", keyArgs...)

	// Save country
	err = conn.Send("SADD", "countries", stats.CountryCode)

	err = conn.Flush()
	return
}

// writeToRedis writes values (counters or gauges) to redis
func writeToRedis(
	statType string,
	id string,
	countryCode string,
	values map[string]int64) (err error) {
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

	qualifiedKey := func(key string) string {
		if statType == "gauge" {
			// For gauges, qualify the key with a date
			return fmt.Sprintf("%s:%d", key, now.Unix())
		} else {
			return key
		}
	}

	// Set details
	for key, value := range values {
		keyArgs[i] = key
		i++
		redisKey := redisKey(statType, fmt.Sprintf("detail:%s", id), qualifiedKey(key))
		err = conn.Send("GETSET", redisKey, value)
	}

	// The reason we don't do EXPIREAT in the above loop is that the code for
	// country and global rollups needs to read the return values from GETSET,
	// and we don't want to bother with interleaving those with the EXPIREAT
	// return values.
	for key, _ := range values {
		redisKey := redisKey(statType, fmt.Sprintf("detail:%s", id), qualifiedKey(key))
		err = conn.Send("EXPIREAT", redisKey, expiration.Unix())
	}

	err = conn.Flush()

	// Roll up to country and global level
	for key, value := range values {
		var oldValue int64
		if oldValue, _, err = receive(conn); err != nil {
			return
		}
		delta := value - oldValue
		countryKey := redisKey(statType, fmt.Sprintf("country:%s", countryCode), qualifiedKey(key))
		globalKey := redisKey(statType, "global", qualifiedKey(key))
		err = conn.Send("INCRBY", countryKey, delta)
		err = conn.Send("EXPIREAT", countryKey, expiration.Unix())
		err = conn.Send("INCRBY", globalKey, delta)
		err = conn.Send("EXPIREAT", globalKey, expiration.Unix())

		// Special treatment for "everOnline" gauge on non-fallbacks
		if statType == statType && key == "online" && value == 1 && strings.Index(id, "fp-") != 0 {
			everOnlineKey := redisKey(statType, fmt.Sprintf("detail:%s", id), "everOnline")
			countryEverOnlineKey := redisKey(statType, fmt.Sprintf("country:%s", countryCode), "everOnline")
			globalEverOnlineKey := redisKey(statType, "global", "everOnline")
			conn.Send("SET", everOnlineKey, 1)
			conn.Send("SADD", countryEverOnlineKey, id)
			conn.Send("SADD", globalEverOnlineKey, id)
		}
	}

	// Remember keys
	err = conn.Send("SADD", keyArgs...)

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
