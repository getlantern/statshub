// Copyright 2014 Brave New Software

//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at

//        http://www.apache.org/licenses/LICENSE-2.0

//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

// statReader encapsulates the differences in reading stats between Counters, Gauges and Members
type statReader struct {
	// statType: the type of stat handled by this reader (i.e. "counter" or "gauge")
	statType string

	// prepareRead prepares the read (typically by SENDing a command like GET)
	prepareRead func(redisKey string) error

	// recordVal takes a value that's been read from Redis and sets it on the supplied stats
	recordVal func(stats *Stats, key string, val int64)
}

// QueryDims runs a query for values from the requested dimensions.  If dimNames is empty,
// QueryDims will query all dimensions.
func QueryDims(dimNames []string) (statsByDim map[string]map[string]*Stats, err error) {
	var conn redis.Conn
	conn, err = connectToRedis()
	if err != nil {
		err = fmt.Errorf("Unable to connect to redis: %s", err)
		return
	}
	defer conn.Close()

	if dimNames == nil || len(dimNames) == 0 {
		if dimNames, err = listDimNames(conn); err != nil {
			return
		}
	}

	statsByDim = make(map[string]map[string]*Stats)
	for _, dimName := range dimNames {
		dimStats := make(map[string]*Stats)
		var dimKeys []string
		if dimKeys, err = listDimKeys(conn, dimName); err != nil {
			return nil, fmt.Errorf("Unable to list keys for dimension: %s", dimName)
		}
		for _, dimKey := range dimKeys {
			dimStats[dimKey] = newStats()
		}

		// Synthetic "total" dimKey for calculated totals
		dimStats["total"] = newStats()

		statsByDim[dimName] = dimStats
	}

	if err = queryCounters(conn, statsByDim); err != nil {
		return
	}

	if err = queryGauges(conn, statsByDim); err != nil {
		return
	}

	err = queryMembers(conn, statsByDim)

	return
}

// queryCounters queries simple counter statistics
func queryCounters(conn redis.Conn, statsByDim map[string]map[string]*Stats) (err error) {
	return doQuery(
		conn,
		statsByDim,
		&statReader{
			statType: "counter",
			prepareRead: func(redisKey string) error {
				return conn.Send("GET", redisKey)
			},
			recordVal: func(stats *Stats, key string, val int64) {
				stats.Counters[key] = val
			},
		},
	)
}

// queryGauges queries simple gauge statistics
func queryGauges(conn redis.Conn, statsByDim map[string]map[string]*Stats) (err error) {
	currentPeriod := time.Now().Truncate(statsPeriod)
	priorPeriod := currentPeriod.Add(-1 * statsPeriod)

	// Query gauges from prior period
	err = doQuery(
		conn,
		statsByDim,
		&statReader{
			statType: "gauge",
			prepareRead: func(redisKey string) error {
				return conn.Send("GET", keyForPeriod(redisKey, priorPeriod))
			},
			recordVal: func(stats *Stats, key string, val int64) {
				stats.Gauges[key] = val
			},
		},
	)

	if err != nil {
		return
	}

	// Query gauges for current period
	return doQuery(
		conn,
		statsByDim,
		&statReader{
			statType: "gauge",
			prepareRead: func(redisKey string) error {
				return conn.Send("GET", keyForPeriod(redisKey, currentPeriod))
			},
			recordVal: func(stats *Stats, key string, val int64) {
				stats.GaugesCurrent[key] = val
			},
		},
	)
}

// queryMembers queries member statistics and returns their counts as Gauges
func queryMembers(conn redis.Conn, statsByDim map[string]map[string]*Stats) (err error) {
	return doQuery(
		conn,
		statsByDim,
		&statReader{
			statType: "member",
			prepareRead: func(redisKey string) error {
				return conn.Send("SCARD", redisKey)
			},
			recordVal: func(stats *Stats, key string, val int64) {
				stats.Gauges[key] = val
			},
		},
	)
}

// doQuery implements the basic querying flow, which is:
//
// 1. List all keys for the type of stat
// 2. For each dimension, dimension key and stat key, prepare a query (e.g. issue a GET)
// 3. Flush the connection to execute the query
// 4. Read the responses and populate a Stats object with the key/value pairs for each dimension and dimension key
func doQuery(conn redis.Conn, statsByDim map[string]map[string]*Stats, reader *statReader) (err error) {
	var keys []string
	if keys, err = listStatKeys(conn, reader.statType); err != nil {
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
			for _, key := range keys {
				fullDimKey := redisKey(reader.statType, fmt.Sprintf("dim:%s:%s", dimName, dimKey), key)
				err = reader.prepareRead(fullDimKey)
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
		totalByKey := make(map[string]int64)

		for _, dimKey := range dimKeys[dimName] {
			for _, key := range keys {
				var found bool
				if val, found, err = receive(conn); err != nil {
					return
				}
				if found {
					reader.recordVal(dimStats[dimKey], key, val)
					totalByKey[key] += val
				}
			}
		}

		for key, total := range totalByKey {
			reader.recordVal(dimStats["total"], key, total)
		}
	}

	return
}

// keyForPeriod constructs a redis key from a base key plus a given time period
func keyForPeriod(key string, period time.Time) string {
	return fmt.Sprintf("%s:%d", key, period.Unix())
}
