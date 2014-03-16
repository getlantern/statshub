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

	// needsExpiration: whether or not the values written by this writer need to be expired
	needsExpiration bool

	// needsDelta: whether or not the dimensions to be written by this writer need a delta from the written detail
	needsDelta bool

	// writeDetail: writes the detail entry for the given key
	writeDetail func(redisKey string, val interface{}, expiration time.Time) error

	// writeDim: writes the dimension entry for the given key
	writeDim func(redisKey string, val interface{}, delta interface{}, expiration time.Time) error
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

	if err = stats.writeCounters(id); err != nil {
		return
	}
	if err = stats.writeGauges(id); err != nil {
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
	return stats.doWriteInt(id, stats.Increments, &statWriter{
		statType: "counter",
		writeDetail: func(redisKey string, val interface{}, expiration time.Time) error {
			return stats.conn.Send("INCRBY", redisKey, val)
		},
		writeDim: func(redisKey string, val interface{}, delta interface{}, expiration time.Time) error {
			return stats.conn.Send("INCRBY", redisKey, val)
		},
	})
}

func (stats *StatsUpdate) writeCounters(id string) (err error) {
	return stats.doWriteInt(id, stats.Counters, &statWriter{
		statType:   "counter",
		needsDelta: true,
		writeDetail: func(redisKey string, val interface{}, expiration time.Time) error {
			return stats.conn.Send("GETSET", redisKey, val)
		},
		writeDim: func(redisKey string, val interface{}, delta interface{}, expiration time.Time) error {
			return stats.conn.Send("INCRBY", redisKey, delta)
		},
	})
}

func (stats *StatsUpdate) writeGauges(id string) (err error) {
	return stats.doWriteInt(id, stats.Gauges, &statWriter{
		statType:        "gauge",
		needsExpiration: true,
		needsDelta:      true,
		writeDetail: func(redisKey string, val interface{}, expiration time.Time) error {
			return stats.conn.Send("GETSET", redisKey, val)
		},
		writeDim: func(redisKey string, val interface{}, delta interface{}, expiration time.Time) error {
			stats.conn.Send("INCRBY", redisKey, delta)
			return stats.conn.Send("EXPIREAT", redisKey, expiration.Unix())
		},
	})
}

// writeMembers adds members to redis
func (stats *StatsUpdate) writeMembers(id string) (err error) {
	return stats.doWriteString(id, stats.Members, &statWriter{
		statType: "member",
		writeDetail: func(redisKey string, val interface{}, expiration time.Time) error {
			return stats.conn.Send("SADD", redisKey, val)
		},
		writeDim: func(redisKey string, val interface{}, oldVal interface{}, expiration time.Time) error {
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

func (stats *StatsUpdate) doWrite(
	id string,
	numValues int,
	writer *statWriter,
	iterateValues func(reportVal func(key string, val interface{}) error) error) (err error) {

	now := time.Now()
	now = now.Truncate(statsPeriod)
	expiration := now.Add(3 * statsPeriod)

	// Drain the receive buffer from redis in case previous work hasn't read all its responses
	if err = stats.drainReceiveBuffer(); err != nil {
		return
	}

	keyArgs := make([]interface{}, numValues+1)
	keyArgs[0] = fmt.Sprintf("key:%s", writer.statType)
	i := 1

	isGauge := writer.statType == "gauge"
	qualifiedKey := func(key string) string {
		if isGauge {
			// For gauges, qualify the key with a date
			return fmt.Sprintf("%s:%d", key, now.Unix())
		} else {
			return key
		}
	}

	err = iterateValues(func(key string, val interface{}) error {
		keyArgs[i] = key
		i++
		detailKey := redisKey(writer.statType, fmt.Sprintf("detail:%s", id), qualifiedKey(key))
		return writer.writeDetail(detailKey, val, expiration)
	})
	if err != nil {
		return
	}

	if writer.needsExpiration {
		// The reason we don't do EXPIREAT in the above loop is that the code for
		// dimensional rollups needs to read the return values from GETSET,
		// and we don't want to bother with interleaving those with the EXPIREAT
		// return values.
		iterateValues(func(key string, val interface{}) error {
			redisKey := redisKey(writer.statType, fmt.Sprintf("detail:%s", id), qualifiedKey(key))
			return stats.conn.Send("EXPIREAT", redisKey, expiration.Unix())
		})
	}
	if err != nil {
		return
	}

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
			dimKey := redisKey(writer.statType, fmt.Sprintf("dim:%s:%s", dimName, dimValue), qualifiedKey(key))
			err = writer.writeDim(dimKey, val, delta, expiration)
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
