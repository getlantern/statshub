package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strings"
	"time"
)

// StatsUpdate posts stats with zero, one or more dimensions.  Stats
// include Counter (cumulative) and Gauges (point in time)
// (online/offline).
type StatsUpdate struct {
	Dims map[string]string `json:"dims"`
	Stats
}

// postToRedis posts Counter and Gauge for the given id to redis
// using INCRBY and SET respectively.
func (stats *StatsUpdate) postToRedis(id string) (err error) {
	// Always treat dimensions as lower case
	lowercasedDims := make(map[string]string)
	for name, key := range stats.Dims {
		dimName := strings.ToLower(name)
		dimKey := strings.ToLower(key)
		lowercasedDims[dimName] = dimKey
	}
	stats.Dims = lowercasedDims

	if err = writeIncrement(id, stats); err != nil {
		return
	}
	if err = writeToRedis("counter", id, stats.Dims, stats.Counters); err != nil {
		return
	}
	if err = writeToRedis("gauge", id, stats.Dims, stats.Gauges); err != nil {
		return
	}
	err = writeMembers(id, stats)

	return
}

// writeIncrement increments counters in redis
func writeIncrement(id string, stats *StatsUpdate) (err error) {
	var conn redis.Conn
	if conn, err = connectToRedis(); err != nil {
		return
	}
	defer conn.Close()

	values := stats.Increments
	keyArgs := make([]interface{}, len(values)+1)
	keyArgs[0] = "key:counter"
	i := 1

	for key, value := range values {
		keyArgs[i] = key
		i++
		detailKey := redisKey("counter", fmt.Sprintf("detail:%s", id), key)
		err = conn.Send("INCRBY", detailKey, value)
		for dimName, dimValue := range stats.Dims {
			dimKey := redisKey("counter", fmt.Sprintf("dim:%s:%s", dimName, dimValue), key)
			err = conn.Send("INCRBY", dimKey, value)
		}
	}

	// Remember counter keys
	err = conn.Send("SADD", keyArgs...)

	// Save dims
	for name, value := range stats.Dims {
		err = conn.Send("SADD", "dim", name)
		err = conn.Send("SADD", "dim:"+name, value)
	}

	err = conn.Flush()
	return
}

// writeToRedis writes values (counters or gauges) to redis
func writeToRedis(
	statType string,
	id string,
	dims map[string]string,
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
	// dimensional rollups needs to read the return values from GETSET,
	// and we don't want to bother with interleaving those with the EXPIREAT
	// return values.
	for key, _ := range values {
		redisKey := redisKey(statType, fmt.Sprintf("detail:%s", id), qualifiedKey(key))
		err = conn.Send("EXPIREAT", redisKey, expiration.Unix())
	}

	err = conn.Flush()

	// Roll up to dimensions
	for key, value := range values {
		var oldValue int64
		if oldValue, _, err = receive(conn); err != nil {
			return
		}
		delta := value - oldValue
		for dimName, dimValue := range dims {
			dimKey := redisKey(statType, fmt.Sprintf("dim:%s:%s", dimName, dimValue), qualifiedKey(key))
			err = conn.Send("INCRBY", dimKey, delta)
			err = conn.Send("EXPIREAT", dimKey, expiration.Unix())
		}
	}

	// Remember keys
	err = conn.Send("SADD", keyArgs...)

	err = conn.Flush()
	return
}

// writeMembers adds members to redis
func writeMembers(id string, stats *StatsUpdate) (err error) {
	var conn redis.Conn
	if conn, err = connectToRedis(); err != nil {
		return
	}
	defer conn.Close()

	values := stats.Members
	keyArgs := make([]interface{}, len(values)+1)
	keyArgs[0] = "key:member"
	i := 1

	for key, value := range values {
		keyArgs[i] = key
		i++
		detailKey := redisKey("member", fmt.Sprintf("detail:%s", id), key)
		err = conn.Send("SADD", detailKey, value)
		for dimName, dimValue := range stats.Dims {
			dimKey := redisKey("member", fmt.Sprintf("dim:%s:%s", dimName, dimValue), key)
			err = conn.Send("SADD", dimKey, value)
		}
	}

	// Remember member keys
	err = conn.Send("SADD", keyArgs...)

	// Save dims //TODO: rather than doing this several times for each type of stat, do it just once
	for name, value := range stats.Dims {
		err = conn.Send("SADD", "dim", name)
		err = conn.Send("SADD", "dim:"+name, value)
	}

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
