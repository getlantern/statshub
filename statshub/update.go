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
	"strings"
	"time"
)

// StatsUpdate posts stats with zero, one or more dimensions.  Stats
// include Counters (cumulative) and Gauges (point in time)
// (online/offline).
type StatsUpdate struct {
	Dims map[string]string `json:"dims"`
	Stats
	conn redis.Conn
}

// statWriter encapsulates the differences in writing stats between Counters, Increments, Gauges and Members
type statWriter struct {
	// statType: the type of stat handled by this writer (i.e. "counter" or "gauge")
	statType string

	// needsDelta: whether or not the dimensions to be written by this writer need a delta from the written detail
	needsDelta bool

	// writeDetail: writes the detail entry for the given key
	writeDetail func(redisKey string, val interface{}) error

	// expireDetail: expires the detail value (if necessary)
	expireDetail func(redisKey string) error

	// writeDim: writes the dimension entry for the given key
	writeDim func(redisKey string, val interface{}, delta interface{}) error
}

// write posts Counters, Increments, and Gauges and Members for the given id to redis,
// precalculating rollups for each dimension in stats.Dims.
func (stats *StatsUpdate) write(id string) (err error) {
	// Always treat dimensions as lower case
	lowercasedDims := make(map[string]string)
	for name, key := range stats.Dims {
		dimName := strings.ToLower(name)
		dimKey := strings.ToLower(key)
		if dimKey == "total" {
			return fmt.Errorf("Dimension key 'total' is not allowed because it is a reserved word")
		}
		lowercasedDims[dimName] = dimKey
	}
	stats.Dims = lowercasedDims

	if stats.conn, err = connectToRedis(); err != nil {
		return
	}
	defer stats.conn.Close()

	if err = stats.writeCounters(id); err != nil {
		return
	}
	if err = stats.writeIncrements(id); err != nil {
		return
	}
	if err = stats.writeGauges(id); err != nil {
		return
	}
	if err = stats.writeMembers(id); err != nil {
		return
	}

	// Save dims
	for name, value := range stats.Dims {
		err = stats.conn.Send("SADD", "dim", name)
		err = stats.conn.Send("SADD", "dim:"+name, value)
	}
	err = stats.conn.Flush()

	return
}

// writeIncrements increments counters in redis
func (stats *StatsUpdate) writeIncrements(id string) (err error) {
	return stats.doWriteInt(id, stats.Increments, &statWriter{
		statType: "counter",
		writeDetail: func(redisKey string, val interface{}) error {
			// Detail values are simply incremented
			return stats.conn.Send("INCRBY", redisKey, val)
		},
		expireDetail: func(redisKey string) error {
			return nil
		},
		writeDim: func(redisKey string, val interface{}, delta interface{}) error {
			// Rollups are simply incremented
			return stats.conn.Send("INCRBY", redisKey, val)
		},
	})
}

// writeCounters sets counters in redis
func (stats *StatsUpdate) writeCounters(id string) (err error) {
	return stats.doWriteInt(id, stats.Counters, &statWriter{
		statType:   "counter",
		needsDelta: true,
		writeDetail: func(redisKey string, val interface{}) error {
			// Detail values are set using GETSET so that they return their old value
			return stats.conn.Send("GETSET", redisKey, val)
		},
		expireDetail: func(redisKey string) error {
			return nil
		},
		writeDim: func(redisKey string, val interface{}, delta interface{}) error {
			// Rollups are incremented by the delta, which is the new value - old value of the detail
			return stats.conn.Send("INCRBY", redisKey, delta)
		},
	})
}

// writeGauges sets gauges in redis
func (stats *StatsUpdate) writeGauges(id string) (err error) {
	now := time.Now()
	now = now.Truncate(statsPeriod)
	expiration := now.Add(3 * statsPeriod)

	return stats.doWriteInt(id, stats.Gauges, &statWriter{
		statType:   "gauge",
		needsDelta: true,
		writeDetail: func(redisKey string, val interface{}) error {
			// Gauge keys are qualified by the current period's Unix timestamp
			redisKey = keyForPeriod(redisKey, now)
			// Detail values are set using GETSET so that they return their old value
			return stats.conn.Send("GETSET", redisKey, val)
		},
		expireDetail: func(redisKey string) error {
			// Gauge keys are qualified by the current period's Unix timestamp
			redisKey = keyForPeriod(redisKey, now)
			// Detail values are expired every statsPeriod period
			return stats.conn.Send("EXPIREAT", redisKey, expiration.Unix())
		},
		writeDim: func(redisKey string, val interface{}, delta interface{}) error {
			// Gauge keys are qualified by the current period's Unix timestamp
			redisKey = keyForPeriod(redisKey, now)
			// Rollups are incremented by the delta, which is the new value - old value of the detail
			stats.conn.Send("INCRBY", redisKey, delta)
			// Rollups are expired every statsPeriod period
			return stats.conn.Send("EXPIREAT", redisKey, expiration.Unix())
		},
	})
}

// writeMembers adds members to redis
func (stats *StatsUpdate) writeMembers(id string) (err error) {
	return stats.doWriteString(id, stats.Members, &statWriter{
		statType: "member",
		writeDetail: func(redisKey string, val interface{}) error {
			// Record member in set
			return stats.conn.Send("SADD", redisKey, val)
		},
		expireDetail: func(redisKey string) error {
			return nil
		},
		writeDim: func(redisKey string, val interface{}, oldVal interface{}) error {
			// Record member in set
			return stats.conn.Send("SADD", redisKey, val)
		},
	})
}

func (stats *StatsUpdate) doWriteInt(
	id string,
	values map[string]int64,
	writer *statWriter) (err error) {

	return stats.doWrite(
		id,
		len(values),
		writer,
		func(reportVal func(key string, val interface{}) error) error {
			for key, val := range values {
				reportVal(key, val)
			}
			return nil
		})
}

func (stats *StatsUpdate) doWriteString(
	id string,
	values map[string]string,
	writer *statWriter) (err error) {

	return stats.doWrite(
		id,
		len(values),
		writer,
		func(reportVal func(key string, val interface{}) error) error {
			for key, val := range values {
				reportVal(key, val)
			}
			return nil
		})
}

// doWrite handles the general pattern for writing part of a StatsUpdate (e.g. Counters) to redis
// For each stat key, this means:
//
// 1. Write the detail value
// 2. For each dimension, update the rollup (potentially calculating this based on how the detail value changed relative its prior value)
// 3. Record the stat key so that future queries know which stats to include
func (stats *StatsUpdate) doWrite(
	id string,
	numValues int,
	writer *statWriter,
	iterateValues func(reportVal func(key string, val interface{}) error) error) (err error) {

	// Drain the receive buffer from redis in case previous work hasn't read all its responses
	if err = stats.drainReceiveBuffer(); err != nil {
		return
	}

	keyArgs := make([]interface{}, numValues+1)
	keyArgs[0] = fmt.Sprintf("key:%s", writer.statType)
	i := 1

	// Write detail values
	err = iterateValues(func(key string, val interface{}) error {
		keyArgs[i] = key
		i++
		detailKey := redisKey(writer.statType, fmt.Sprintf("detail:%s", id), key)
		return writer.writeDetail(detailKey, val)
	})
	if err != nil {
		return
	}

	// Expire detail values
	err = iterateValues(func(key string, val interface{}) error {
		detailKey := redisKey(writer.statType, fmt.Sprintf("detail:%s", id), key)
		return writer.expireDetail(detailKey)
	})
	if err != nil {
		return
	}

	// Flush writes to redis
	if err = stats.conn.Flush(); err != nil {
		return
	}

	iterateValues(func(key string, val interface{}) (err error) {
		var delta int64
		if writer.needsDelta {
			var oldVal int64
			if oldVal, _, err = receive(stats.conn); err != nil {
				return
			}
			delta = val.(int64) - oldVal
		}
		for dimName, dimValue := range stats.Dims {
			dimKey := redisKey(writer.statType, fmt.Sprintf("dim:%s:%s", dimName, dimValue), key)
			err = writer.writeDim(dimKey, val, delta)
		}
		return
	})

	if err != nil {
		return
	}

	// Remember keys
	return stats.conn.Send("SADD", keyArgs...)
}

// drainReceiveBuffer drains any responses in the receive buffer that haven't been read
func (stats *StatsUpdate) drainReceiveBuffer() (err error) {
	_, err = stats.conn.Do("")
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
