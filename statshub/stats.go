package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strconv"
	"strings"
)

// Stats is a bundle of stats
type Stats struct {
	Counters   map[string]int64 `json:"counters,omitempty"`
	Increments map[string]int64 `json:"increments,omitempty"`
	Gauges     map[string]int64 `json:"gauges,omitempty"`
}

// newStats constructs a Stats
func newStats() (stats *Stats) {
	return &Stats{
		Counters: make(map[string]int64),
		Gauges:   make(map[string]int64),
	}
}

// receive receives the next value from the redis.Conn's output buffer.
func receive(conn redis.Conn) (val int64, found bool, err error) {
	var ival interface{}
	if ival, err = conn.Receive(); err != nil {
		return
	}
	val, found, err = fromRedisVal(ival)
	return
}

// fromRedisVale converts a value received from redis into an int64.
// If there was no value found in redis, found will equal false.
func fromRedisVal(redisVal interface{}) (val int64, found bool, err error) {
	if redisVal == nil {
		found = false
	} else {
		found = true
		switch v := redisVal.(type) {
		case []uint8:
			valString := string(v)
			var intVal int
			intVal, err = strconv.Atoi(valString)
			if err != nil {
				return
			} else {
				val = int64(intVal)
			}
		case int64:
			val = v
		default:
			err = fmt.Errorf("Value of unknown type returned from redis: %s", v)
		}
	}
	return
}

// redisKey constructs a key for a stat from its type (e.g. counter),
// group (e.g. country:es) and key (e.g. mystat).  Dashes are replaced
// by underscores.
func redisKey(statType string, group string, key interface{}) string {
	return strings.Replace(
		fmt.Sprintf("%s:%s:%s", statType, group, key),
		"-",
		"_",
		-1)
}

// listStatKeys lists all keys (e.g. mystat) for stats of the given type
// (e.g. counter).
func listStatKeys(conn redis.Conn, statType string) (keys []string, err error) {
	var tmpKeys interface{}
	if tmpKeys, err = conn.Do("SMEMBERS", fmt.Sprintf("key:%s", statType)); err != nil {
		return
	}
	ikeys := tmpKeys.([]interface{})
	keys = make([]string, len(ikeys))
	for i, ikey := range ikeys {
		keys[i] = string(ikey.([]uint8))
	}
	return
}

// listDimKeys lists all keys of the given dimension.
func listDimKeys(conn redis.Conn, name string) (values []string, err error) {
	var ivalues interface{}
	if ivalues, err = conn.Do("SMEMBERS", "dim:"+name); err != nil {
		return
	}
	iavalues := ivalues.([]interface{})
	values = make([]string, len(iavalues))
	for i, value := range iavalues {
		values[i] = string(value.([]uint8))
	}
	return
}
