package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"strings"
	"time"
)

const (
	presencePeriod     = 5 * time.Minute
	maxPresencePeriods = 10
)

// StatsSubmission posts stats from within a specific country.  Stats
// include Counter (cumulative), Gauges (point in time) and Presence
// (online/offline).
type StatsSubmission struct {
	CountryCode string `json:"countryCode"`
	Stats
}

// postToRedis posts Counter, Gauge and Presence for the given userId to redis
// using INCRBY and SET respectively.
func (stats *StatsSubmission) postToRedis(userId int64) (err error) {
	// Always treat countries as lower case
	stats.CountryCode = strings.ToLower(stats.CountryCode)

	if err = writeCounters(userId, stats); err != nil {
		return
	}
	err = writeGauges(userId, stats)

	return
}

func writeCounters(userId int64, stats *StatsSubmission) (err error) {
	var conn redis.Conn
	if conn, err = connectToRedis(); err != nil {
		return
	}

	values := stats.Counter
	keyArgs := make([]interface{}, len(values)+1)
	keyArgs[0] = "key:counter"
	i := 1

	for key, value := range values {
		keyArgs[i] = key
		i++
		userKey := redisKey("counter", fmt.Sprintf("user:%d", userId), key)
		countryKey := redisKey("counter", fmt.Sprintf("country:%s", stats.CountryCode), key)
		globalKey := redisKey("counter", "global", key)
		if err = conn.Send("INCRBY", userKey, value); err != nil {
			return
		}
		if err = conn.Send("INCRBY", countryKey, value); err != nil {
			return
		}
		if err = conn.Send("INCRBY", globalKey, value); err != nil {
			return
		}
	}

	if err = conn.Send("SADD", keyArgs...); err != nil {
		return
	}

	err = conn.Flush()
	return
}

func writeGauges(userId int64, stats *StatsSubmission) (err error) {
	var conn redis.Conn
	if conn, err = connectToRedis(); err != nil {
		return
	}

	values := stats.Gauge
	keyArgs := make([]interface{}, len(values)+1)
	keyArgs[0] = "key:gauge"
	i := 1

	// Set gauges at user level
	for key, value := range values {
		key = strings.ToLower(key)
		keyArgs[i] = key
		i++
		redisKey := redisKey("gauge", fmt.Sprintf("user:%d", userId), key)
		if err = conn.Send("GETSET", redisKey, value); err != nil {
			return
		}
	}

	if err = conn.Flush(); err != nil {
		return
	}

	// Roll up gauges to country and global level
	for key, value := range values {
		var oldValue int64
		if oldValue, err = receive(conn); err != nil {
			return
		}
		delta := value - oldValue
		log.Printf("%s value: %d, oldValue: %d, delta: %d", key, value, oldValue, delta)
		countryKey := redisKey("gauge", fmt.Sprintf("country:%s", stats.CountryCode), key)
		globalKey := redisKey("gauge", "global", key)
		if err = conn.Send("INCRBY", countryKey, delta); err != nil {
			return
		}
		if err = conn.Send("INCRBY", globalKey, delta); err != nil {
			return
		}
	}

	// Remember gauge keys
	if err = conn.Send("SADD", keyArgs...); err != nil {
		return
	}

	err = conn.Flush()
	return
}

func withLowerCaseKeys(values map[string]uint64) (lowerCased map[string]uint64) {
	lowerCased = make(map[string]uint64)
	for key, value := range values {
		lowerCased[strings.ToLower(key)] = value
	}
	return
}
