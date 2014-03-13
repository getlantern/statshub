package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strconv"
	"strings"
)

// Stats is a bundle of stats
type Stats struct {
	Counter   map[string]int64 `json:"counter"`
	Increment map[string]int64 `json:"increment,omitempty"`
	Gauge     map[string]int64 `json:"gauge"`
}

// newStats constructs a Stats
func newStats() (stats *Stats) {
	return &Stats{
		Counter: make(map[string]int64),
		Gauge:   make(map[string]int64),
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

// listCountries lists all country codes for which a stat has been reported at
// some point in the past.
func listCountries(conn redis.Conn) (countries []string, err error) {
	var icountries interface{}
	if icountries, err = conn.Do("SMEMBERS", "countries"); err != nil {
		return
	}
	iacountries := icountries.([]interface{})
	countries = make([]string, len(iacountries))
	for i, country := range iacountries {
		countries[i] = string(country.([]uint8))
	}
	return
}
