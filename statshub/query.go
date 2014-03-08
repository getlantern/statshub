package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strconv"
	"strings"
)

// StatsQuery is a query for stats of a particular user, including the names
// of the Counters, Gauges and Presence stats that are desired.
type StatsQuery struct {
	Counters []string
	Gauges   []string
	Presence []string
}

// QueryResponse is a Response to a StatsQuery
type QueryResponse struct {
	Response
	User       *Stats            `json:"user"`       // Stats for the user
	Global     *Stats            `json:"global"`     // Global stats
	PerCountry map[string]*Stats `json:"perCountry"` // Maps country codes to stats for those countries
}

func (query *StatsQuery) execute(conn redis.Conn, userId int64) (resp *QueryResponse, err error) {
	for _, key := range query.Counters {
		key = strings.ToLower(key)
		userKey := fmt.Sprintf("counter:user:%d:%s", userId, key)
		if err = conn.Send("GET", userKey); err != nil {
			return
		}
	}
	conn.Flush()
	resp = &QueryResponse{User: newStats()}
	for _, key := range query.Counters {
		var val interface{}
		val, err = conn.Receive()
		if err != nil {
			return
		}
		if val == nil {
			resp.User.Counters[key] = 0
		} else {
			valString := string(val.([]uint8))
			var intVal int
			intVal, err = strconv.Atoi(valString)
			if err != nil {
				return
			} else {
				resp.User.Counters[key] = int64(intVal)
			}
		}
	}

	return
}
