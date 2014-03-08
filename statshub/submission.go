package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strings"
	"time"
)

const (
	presencePeriod     = 5 * time.Minute
	maxPresencePeriods = 10
)

// StatsSubmission posts stats from within a specific country.  Stats
// include Counters (cumulative), Gauges (point in time) and Presence
// (online/offline).
type StatsSubmission struct {
	CountryCode string `json:"countryCode"`
	Stats
}

// postToRedis posts Counters, Gauges and Presence for the given userId to redis
// using INCRBY and SET respectively.
func (stats *StatsSubmission) postToRedis(conn redis.Conn, userId int64) (err error) {

	err = submitStats(conn, userId, stats.CountryCode, stats.Counters, "counters", func(i int, redisKey string, value int64) error {
		redisKey = fmt.Sprintf("counter:%s", redisKey)
		return conn.Send("INCRBY", redisKey, value)
	})
	if err != nil {
		return
	}

	err = submitStats(conn, userId, stats.CountryCode, stats.Gauges, "gauges", func(i int, redisKey string, value int64) error {
		redisKey = fmt.Sprintf("gauge:%s", redisKey)
		return conn.Send("SET", redisKey, value)
	})
	if err != nil {
		return
	}

	now := time.Now()
	now = now.Truncate(presencePeriod)
	expiration := now.Add(maxPresencePeriods * presencePeriod)

	err = submitStats(conn, userId, stats.CountryCode, stats.Presences, "presences", func(i int, redisKey string, value int64) (err error) {
		redisKey = fmt.Sprintf("presence:%s:%d", redisKey, now.Unix())
		// Add the timestamp to the redis key
		if i == 0 {
			// For the first key (user-specific) simply set the presence
			if err = conn.Send("SET", redisKey, value); err != nil {
				return
			}
		} else {
			// For the other keys (rollups), add/remove the user id to/from a set
			var cmd string
			if value == 1 {
				cmd = "SADD"
			} else {
				cmd = "SREM"
			}
			if err = conn.Send(cmd, redisKey, userId); err != nil {
				return
			}
		}
		err = conn.Send("EXPIREAT", redisKey, expiration.Unix())
		return
	})
	if err != nil {
		return
	}

	err = conn.Flush()
	return
}

func submitStats(
	conn redis.Conn,
	userId int64,
	countryCode string,
	statsMap map[string]int64,
	name string,
	submitter func(i int, key string, value int64) error) (err error) {

	redisKeys := func(key string) []string {
		key = strings.ToLower(key)
		return []string{
			fmt.Sprintf("user:%d:%s", userId, key),
			fmt.Sprintf("country:%s:%s", strings.ToLower(countryCode), key),
			fmt.Sprintf("global:%s", key),
		}
	}

	keyArgs := []interface{}{fmt.Sprintf("key:%s", name)}

	for key, value := range statsMap {
		key = strings.ToLower(key)
		keyArgs = append(keyArgs, key)
		i := 0
		for _, redisKey := range redisKeys(key) {
			if err = submitter(i, redisKey, value); err != nil {
				return
			}
			i++
		}
	}

	err = conn.Send("SADD", keyArgs...)

	return
}
