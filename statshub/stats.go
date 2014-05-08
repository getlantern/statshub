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
	"strconv"
	"strings"
	"time"
)

// Stats is a bundle of stats
type Stats struct {
	Counters      map[string]int64  `json:"counters,omitempty"`
	Increments    map[string]int64  `json:"increments,omitempty"`
	Gauges        map[string]int64  `json:"gauges,omitempty"`
	GaugesCurrent map[string]int64  `json:"gaugesCurrent,omitempty"`
	Members       map[string]string `json:"members,omitempty"`
}

var (
	// reportingPeriod is how frequently clients report stats
	reportingPeriod = 5 * time.Minute

	// statsPeriod controls the buckets in which we store aggregated stats,
	// which are sized slightly larger than the reportingPeriod to accommodate
	// timing differences.
	statsPeriod = reportingPeriod + 1*time.Minute
)

// newStats constructs a Stats
func newStats() (stats *Stats) {
	return &Stats{
		Counters:      make(map[string]int64),
		Gauges:        make(map[string]int64),
		GaugesCurrent: make(map[string]int64),
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

// fromRedisVal converts a value received from redis into an int64.
// If there was no value found in redis, found will equal false.
func fromRedisVal(redisVal interface{}) (val int64, found bool, err error) {
	if redisVal == nil {
		found = false
	} else {
		found = true
		switch v := redisVal.(type) {
		case []uint8:
			valString := string(v)
			val, err = strconv.ParseInt(valString, 10, 64)
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
	return removeDashes(fmt.Sprintf("%s:%s:%s", statType, group, key))
}

// removeDashes removes dashes from a string and replaces them with
// underscores
func removeDashes(val string) string {
	return strings.Replace(val, "-", "_", -1)
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

// listDimNames lists all dimension names.
func listDimNames(conn redis.Conn) (values []string, err error) {
	var ivalues interface{}
	if ivalues, err = conn.Do("SMEMBERS", "dim"); err != nil {
		return
	}
	iavalues := ivalues.([]interface{})
	values = make([]string, len(iavalues))
	for i, value := range iavalues {
		values[i] = string(value.([]uint8))
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
