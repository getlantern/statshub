package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"strconv"
)

// QueryResponse is a Response to a StatsQuery
type QueryResponse struct {
	Response
	User       *Stats            `json:"user"`       // Stats for the user
	Global     *Stats            `json:"global"`     // Global stats
	PerCountry map[string]*Stats `json:"perCountry"` // Maps country codes to stats for those countries
}

func buildStats(conn redis.Conn, group string) (stats *Stats, err error) {
	var tmpKeys interface{}
	if tmpKeys, err = conn.Do("SMEMBERS", "key:counters"); err != nil {
		return
	}
	log.Printf("temp keys: %s", tmpKeys.([]interface{}))
	stats = newStats()
	ikeys := tmpKeys.([]interface{})
	for _, ikey := range ikeys {
		key := string(ikey.([]uint8))
		fullKey := fmt.Sprintf("counter:%s:%s", group, key)
		if err = conn.Send("GET", fullKey); err != nil {
			return
		}
	}
	conn.Flush()
	for _, ikey := range ikeys {
		key := string(ikey.([]uint8))
		var val interface{}
		val, err = conn.Receive()
		if err != nil {
			return
		}
		if val == nil {
			stats.Counters[key] = 0
		} else {
			valString := string(val.([]uint8))
			var intVal int
			intVal, err = strconv.Atoi(valString)
			if err != nil {
				return
			} else {
				stats.Counters[key] = int64(intVal)
			}
		}
	}
	log.Printf("stats.Counters: %s", stats.Counters)
	return
}

func query(conn redis.Conn, userId int64) (resp *QueryResponse, err error) {
	buildStats(conn, "global")
	resp = &QueryResponse{User: newStats()}
	return
}
