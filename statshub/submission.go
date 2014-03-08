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
	redisKeys := func(key string) []string {
		key = strings.ToLower(key)
		return []string{
			fmt.Sprintf("user:%d:%s", userId, key),
			fmt.Sprintf("country:%s:%s", strings.ToLower(stats.CountryCode), key),
			fmt.Sprintf("global:%s", key),
		}
	}

	for key, value := range stats.Counters {
		key = strings.ToLower(key)
		for _, redisKey := range redisKeys(key) {
			redisKey = fmt.Sprintf("counter:%s", redisKey)
			if err = conn.Send("INCRBY", redisKey, value); err != nil {
				return
			}
		}
	}

	for key, value := range stats.Gauges {
		key = strings.ToLower(key)
		for _, redisKey := range redisKeys(key) {
			redisKey = fmt.Sprintf("gauge:%s", redisKey)
			if err = conn.Send("SET", redisKey, value); err != nil {
				return
			}
		}
	}

	now := time.Now()
	now = now.Truncate(presencePeriod)
	expiration := now.Add(maxPresencePeriods * presencePeriod)

	for key, value := range stats.Presence {
		for i, redisKey := range redisKeys(key) {
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
				if value {
					cmd = "SADD"
				} else {
					cmd = "SREM"
				}
				if err = conn.Send(cmd, redisKey, userId); err != nil {
					return
				}
			}
			if err = conn.Send("EXPIREAT", redisKey, expiration.Unix()); err != nil {
				return
			}
		}
	}

	err = conn.Flush()
	return
}
