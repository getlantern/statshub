package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

// QueryDims runs a query for values from the requested dimensions
func QueryDims(dimNames []string) (statsByDim map[string]map[string]*Stats, err error) {
	var conn redis.Conn
	conn, err = connectToRedis()
	if err != nil {
		err = fmt.Errorf("Unable to connect to redis: %s", err)
		return
	}
	defer conn.Close()

	statsByDim = make(map[string]map[string]*Stats)
	// statsByDim["auto"] = make(map[string]*Stats)
	// statsByDim["total"] = newStats()
	for _, dimName := range dimNames {
		dimStats := make(map[string]*Stats)
		var dimKeys []string
		if dimKeys, err = listDimKeys(conn, dimName); err != nil {
			return nil, fmt.Errorf("Unable to list keys for dimension: %s", dimName)
		}
		for _, dimKey := range dimKeys {
			dimStats[dimKey] = newStats()
		}

		statsByDim[dimName] = dimStats
	}

	if err = queryCounters(conn, statsByDim); err != nil {
		return
	}

	err = queryGauges(conn, statsByDim)

	return
}

// queryCounters queries simple counter statistics
func queryCounters(conn redis.Conn, statsByDim map[string]map[string]*Stats) (err error) {
	var counterKeys []string
	if counterKeys, err = listStatKeys(conn, "counter"); err != nil {
		return
	}

	// dimNames and dimKeys are needed for consistent iteration order on statsByDim
	dimNames := make([]string, len(statsByDim))
	dimKeys := make(map[string][]string)

	i := 0
	for dimName, dimStats := range statsByDim {
		dimNames[i] = dimName
		keysForDim := make([]string, len(dimStats))
		j := 0
		for dimKey, _ := range dimStats {
			keysForDim[j] = dimKey
			for _, key := range counterKeys {
				fullDimKey := redisKey("counter", fmt.Sprintf("dim:%s:%s", dimName, dimKey), key)
				err = conn.Send("GET", fullDimKey)
			}
			j++
		}
		dimKeys[dimName] = keysForDim
		i++
	}

	err = conn.Flush()

	var val int64
	for _, dimName := range dimNames {
		dimStats := statsByDim[dimName]
		for _, dimKey := range dimKeys[dimName] {
			for _, key := range counterKeys {
				var found bool
				if val, found, err = receive(conn); err != nil {
					return
				}
				if found {
					dimStats[dimKey].Counters[key] = val
				}
			}
		}
	}

	return
}

// queryGauges queries simple gauge statistics
// TODO: this is a lot like queryCounters, might be nice to reduce the repetition
func queryGauges(conn redis.Conn, statsByDim map[string]map[string]*Stats) (err error) {
	currentPeriod := time.Now().Truncate(statsPeriod)
	priorPeriod := currentPeriod.Add(-1 * statsPeriod)

	var gaugeKeys []string
	if gaugeKeys, err = listStatKeys(conn, "gauge"); err != nil {
		return
	}

	// dimNames and dimKeys are needed for consistent iteration order on statsByDim
	dimNames := make([]string, len(statsByDim))
	dimKeys := make(map[string][]string)

	i := 0
	for dimName, dimStats := range statsByDim {
		dimNames[i] = dimName
		keysForDim := make([]string, len(dimStats))
		j := 0
		for dimKey, _ := range dimStats {
			keysForDim[j] = dimKey
			for _, key := range gaugeKeys {
				fullDimKey := redisKey("gauge", fmt.Sprintf("dim:%s:%s", dimName, dimKey), keyForPeriod(key, priorPeriod))
				err = conn.Send("GET", fullDimKey)
			}
			j++
		}
		dimKeys[dimName] = keysForDim
		i++
	}

	err = conn.Flush()

	var val int64
	for _, dimName := range dimNames {
		dimStats := statsByDim[dimName]
		for _, dimKey := range dimKeys[dimName] {
			for _, key := range gaugeKeys {
				var found bool
				if val, found, err = receive(conn); err != nil {
					return
				}
				if found {
					dimStats[dimKey].Gauges[key] = val
				}
			}
		}
	}

	return
}

// keyForPeriod constructs a redis key from a base key plus a given time period
func keyForPeriod(key string, period time.Time) string {
	return fmt.Sprintf("%s:%d", key, period.Unix())
}
