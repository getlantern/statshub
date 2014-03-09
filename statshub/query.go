package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
)

// QueryResponse is a Response to a StatsQuery
type QueryResponse struct {
	Response
	User       *Stats            `json:"user"`       // Stats for the user
	Global     *Stats            `json:"global"`     // Global stats
	PerCountry map[string]*Stats `json:"perCountry"` // Maps country codes to stats for those countries
}

func query(conn redis.Conn, userId int64) (resp *QueryResponse, err error) {
	resp = &QueryResponse{
		User:       newStats(),
		Global:     newStats(),
		PerCountry: make(map[string]*Stats),
	}

	var counterKeys []string
	if counterKeys, err = listStatKeys(conn, "counter"); err != nil {
		return
	}
	for _, key := range counterKeys {
		userKey := redisKey("counter", fmt.Sprintf("user:%d", userId), key)
		globalKey := redisKey("counter", "global", key)
		if err = conn.Send("GET", userKey); err != nil {
			return
		}
		if err = conn.Send("GET", globalKey); err != nil {
			return
		}
		for _, countryCode := range allCountryCodes {
			countryKey := redisKey("counter", fmt.Sprintf("country:%s", countryCode), key)
			if err = conn.Send("GET", countryKey); err != nil {
				return
			}
		}
	}
	conn.Flush()
	for _, key := range counterKeys {
		var val int64
		if val, err = receive(conn); err != nil {
			return
		}
		resp.User.Counter[key] = val
		if val, err = receive(conn); err != nil {
			return
		}
		resp.Global.Counter[key] = val
		for _, countryCode := range allCountryCodes {
			if val, err = receive(conn); err != nil {
				return
			}
			countryStats := resp.PerCountry[countryCode]
			if countryStats == nil {
				countryStats = newStats()
				resp.PerCountry[countryCode] = countryStats
			}
			countryStats.Counter[key] = val
		}
	}

	return
}

func dontCallMe() {
	log.Fatalf("Dammit, you called me!")
}
