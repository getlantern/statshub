package statshub

import (
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
	resp = &QueryResponse{Global: newStats()}

	var allStats []Stat
	allStats, err = buildStats(conn)
	if err != nil {
		return
	}

	log.Printf("All stats: %s", allStats)

	// Prepare reads
	for _, stat := range allStats {
		if err = stat.prepareRead(conn, "global"); err != nil {
			return
		}
	}
	// Execute
	conn.Flush()
	// Save results
	for _, stat := range allStats {
		if err = stat.saveResult(conn, "global", resp.Global); err != nil {
			return
		}
	}

	return
}
