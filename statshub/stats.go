package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strconv"
)

// Stats is a bundle of stats
type Stats struct {
	Counter map[string]int64 `json:"counter"`
	Gauge   map[string]int64 `json:"gauge"`
}

func newStats() (stats *Stats) {
	return &Stats{
		Counter: make(map[string]int64),
		Gauge:   make(map[string]int64),
	}
}

func receive(conn redis.Conn) (val int64, err error) {
	var ival interface{}
	if ival, err = conn.Receive(); err != nil {
		return
	}
	val, err = fromRedisVal(ival)
	return
}

func fromRedisVal(redisVal interface{}) (val int64, err error) {
	if redisVal == nil {
		val = 0
	} else {
		valString := string(redisVal.([]uint8))
		var intVal int
		intVal, err = strconv.Atoi(valString)
		if err != nil {
			return
		} else {
			val = int64(intVal)
		}
	}
	return
}

func redisKey(statType string, group string, key interface{}) string {
	return fmt.Sprintf("%s:%s:%s", statType, group, key)
}

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
