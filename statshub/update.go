package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strings"
	"time"
)

const (
	gaugePeriod       = 5 * time.Minute
	lookbackPeriods   = 6
	expirationPeriods = lookbackPeriods + 2
)

// StatsUpdate posts stats from within a specific country.  Stats
// include Counter (cumulative) and Gauges (point in time)
// (online/offline).
type StatsUpdate struct {
	CountryCode string `json:"countryCode"`
	Stats
}

// postToRedis posts Counter and Gauge for the given userId to redis
// using INCRBY and SET respectively.
func (stats *StatsUpdate) postToRedis(userId string) (err error) {
	// Always treat countries as lower case
	stats.CountryCode = strings.ToLower(stats.CountryCode)

	if err = writeCounters(userId, stats); err != nil {
		return
	}
	err = writeGauges(userId, stats)

	return
}

func writeCounters(userId string, stats *StatsUpdate) (err error) {
	var conn redis.Conn
	if conn, err = connectToRedis(); err != nil {
		return
	}
	defer conn.Close()

	values := stats.Counter
	keyArgs := make([]interface{}, len(values)+1)
	keyArgs[0] = "key:counter"
	i := 1

	for key, value := range values {
		keyArgs[i] = key
		i++
		userKey := redisKey("counter", fmt.Sprintf("user:%s", userId), key)
		countryKey := redisKey("counter", fmt.Sprintf("country:%s", stats.CountryCode), key)
		globalKey := redisKey("counter", "global", key)
		err = conn.Send("INCRBY", userKey, value)
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

func writeGauges(userId string, stats *StatsUpdate) (err error) {
	var conn redis.Conn
	if conn, err = connectToRedis(); err != nil {
		return
	}
	defer conn.Close()

	now := time.Now()
	now = now.Truncate(gaugePeriod)
	expiration := now.Add(expirationPeriods * gaugePeriod)

	values := stats.Gauge
	keyArgs := make([]interface{}, len(values)+1)
	keyArgs[0] = "key:gauge"
	i := 1

	keyWithDate := func(key string) string {
		return fmt.Sprintf("%s:%d", key, now.Unix())
	}

	// Set gauges at user level
	for key, value := range values {
		keyArgs[i] = key
		i++
		redisKey := redisKey("gauge", fmt.Sprintf("user:%s", userId), keyWithDate(key))
		err = conn.Send("GETSET", redisKey, value)
	}

	// The reason we don't do EXPIREAT in the above loop is that the code for
	// country and global rollups needs to read the return values from GETSET,
	// and we don't want to bother with interleaving those with the EXPIREAT
	// return values.
	for key, _ := range values {
		redisKey := redisKey("gauge", fmt.Sprintf("user:%s", userId), keyWithDate(key))
		err = conn.Send("EXPIREAT", redisKey, expiration.Unix())
	}

	err = conn.Flush()

	// Roll up gauges to country and global level
	for key, value := range values {
		var oldValue int64
		if oldValue, err = receive(conn); err != nil {
			return
		}
		delta := value - oldValue
		countryKey := redisKey("gauge", fmt.Sprintf("country:%s", stats.CountryCode), keyWithDate(key))
		globalKey := redisKey("gauge", "global", keyWithDate(key))
		err = conn.Send("INCRBY", countryKey, delta)
		err = conn.Send("EXPIREAT", countryKey, expiration.Unix())
		err = conn.Send("INCRBY", globalKey, delta)
		err = conn.Send("EXPIREAT", globalKey, expiration.Unix())
	}

	// Remember gauge keys
	err = conn.Send("SADD", keyArgs...)

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
