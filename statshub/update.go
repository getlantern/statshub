package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strings"
	"time"
)

// StatsUpdate posts stats with zero, one or more dimensions.  Stats
// include Counter (cumulative) and Gauges (point in time)
// (online/offline).
type StatsUpdate struct {
	Dims map[string]string `json:"dims"`
	Stats
	conn redis.Conn
}

type statWriter struct {
	// statType: the type of stat handled by this writer (i.e. "counter" or "gauge")
	statType string

	// writeDetail: writes the detail entry for the given key
	writeDetail func(redisKey string, val interface{}) error

	// writeDim: writes the dimension entry for the given key
	writeDim func(redisKey string, val interface{}) error
}

// update posts Counter and Gauge for the given id to redis
// using INCRBY and SET respectively.
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

	if err = stats.writeToRedis("counter", id, stats.Counters); err != nil {
		return
	}
	if err = stats.writeToRedis("gauge", id, stats.Gauges); err != nil {
		return
	}
	if err = stats.writeIncrements(id); err != nil {
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
	return stats.doWrite(id, stats.Increments, &statWriter{
		statType: "counter",
		writeDetail: func(redisKey string, val interface{}) error {
			return stats.conn.Send("INCRBY", redisKey, val)
		},
		writeDim: func(redisKey string, val interface{}) error {
			return stats.conn.Send("INCRBY", redisKey, val)
		},
	})
}

// writeMembers adds members to redis
func (stats *StatsUpdate) writeMembers(id string) (err error) {
	return stats.doWriteStrings(id, stats.Members, &statWriter{
		statType: "member",
		writeDetail: func(redisKey string, val interface{}) error {
			return stats.conn.Send("SADD", redisKey, val)
		},
		writeDim: func(redisKey string, val interface{}) error {
			return stats.conn.Send("SADD", redisKey, val)
		},
	})
}

func (stats *StatsUpdate) doWrite(
	id string,
	values map[string]int64,
	writer *statWriter) (err error) {

	keyArgs := make([]interface{}, len(values)+1)
	keyArgs[0] = fmt.Sprintf("key:%s", writer.statType)
	i := 1

	for key, val := range values {
		keyArgs[i] = key
		i++
		detailKey := redisKey(writer.statType, fmt.Sprintf("detail:%s", id), key)
		err = writer.writeDetail(detailKey, val)
		for dimName, dimValue := range stats.Dims {
			dimKey := redisKey(writer.statType, fmt.Sprintf("dim:%s:%s", dimName, dimValue), key)
			err = writer.writeDim(dimKey, val)
		}
	}

	// Remember keys
	err = stats.conn.Send("SADD", keyArgs...)
	return
}

func (stats *StatsUpdate) doWriteStrings(
	id string,
	values map[string]string,
	writer *statWriter) (err error) {

	keyArgs := make([]interface{}, len(values)+1)
	keyArgs[0] = fmt.Sprintf("key:%s", writer.statType)
	i := 1

	for key, val := range values {
		keyArgs[i] = key
		i++
		detailKey := redisKey(writer.statType, fmt.Sprintf("detail:%s", id), key)
		err = writer.writeDetail(detailKey, val)
		for dimName, dimValue := range stats.Dims {
			dimKey := redisKey(writer.statType, fmt.Sprintf("dim:%s:%s", dimName, dimValue), key)
			err = writer.writeDim(dimKey, val)
		}
	}

	// Remember keys
	err = stats.conn.Send("SADD", keyArgs...)
	return
}

// writeToRedis writes values (counters or gauges) to redis
func (stats *StatsUpdate) writeToRedis(
	statType string,
	id string,
	values map[string]int64) (err error) {

	now := time.Now()
	now = now.Truncate(statsPeriod)
	expiration := now.Add(3 * statsPeriod)

	keyArgs := make([]interface{}, len(values)+1)
	keyArgs[0] = "key:" + statType
	i := 1

	if err = stats.drainReceiveBuffer(); err != nil {
		return
	}

	isGauge := statType == "gauge"

	qualifiedKey := func(key string) string {
		if isGauge {
			// For gauges, qualify the key with a date
			return fmt.Sprintf("%s:%d", key, now.Unix())
		} else {
			return key
		}
	}

	// Set details
	for key, value := range values {
		keyArgs[i] = key
		i++
		redisKey := redisKey(statType, fmt.Sprintf("detail:%s", id), qualifiedKey(key))
		err = stats.conn.Send("GETSET", redisKey, value)
	}

	if isGauge {
		// The reason we don't do EXPIREAT in the above loop is that the code for
		// dimensional rollups needs to read the return values from GETSET,
		// and we don't want to bother with interleaving those with the EXPIREAT
		// return values.
		for key, _ := range values {
			redisKey := redisKey(statType, fmt.Sprintf("detail:%s", id), qualifiedKey(key))
			err = stats.conn.Send("EXPIREAT", redisKey, expiration.Unix())
		}
	}

	err = stats.conn.Flush()

	// Roll up to dimensions
	for key, value := range values {
		var oldValue int64
		if oldValue, _, err = receive(stats.conn); err != nil {
			return
		}
		delta := value - oldValue
		for dimName, dimValue := range stats.Dims {
			dimKey := redisKey(statType, fmt.Sprintf("dim:%s:%s", dimName, dimValue), qualifiedKey(key))
			err = stats.conn.Send("INCRBY", dimKey, delta)
			if isGauge {
				err = stats.conn.Send("EXPIREAT", dimKey, expiration.Unix())
			}
		}
	}

	// Remember keys
	err = stats.conn.Send("SADD", keyArgs...)
	return
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
