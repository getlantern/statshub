package statshub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"strconv"
)

type Stat interface {
	prepareRead(conn redis.Conn, group string) error

	saveResult(conn redis.Conn, group string, stats *Stats) error
}

type Counter string

type Gauge string

func (counter *Counter) prepareRead(conn redis.Conn, group string) error {
	return conn.Send("GET", redisKey("counter", group, string(*counter)))
}

func (counter *Counter) saveResult(conn redis.Conn, group string, stats *Stats) error {
	ival, err := conn.Receive()
	if err != nil {
		return err
	}
	val, err := fromRedisVal(ival)
	if err != nil {
		return err
	}
	stats.Counter[string(*counter)] = val
	return nil
}

func receive(conn redis.Conn) (val int64, err error) {
	var ival interface{}
	if ival, err = conn.Receive(); err != nil {
		return
	}
	log.Printf("Received: %s", ival)
	val, err = fromRedisVal(ival)
	return
}

func fromRedisVal(redisVal interface{}) (val int64, err error) {
	if redisVal == nil {
		val = 0
	} else {
		valString := string(redisVal.([]uint8))
		var intVal int
		intVal, err = strconv.Atoi(valString)
		if err != nil {
			return
		} else {
			val = int64(intVal)
		}
	}
	return
}

func redisKey(statType string, group string, key string) string {
	return fmt.Sprintf("%s:%s:%s", statType, group, key)
}

func buildStats(conn redis.Conn) (allStats []Stat, err error) {
	allStats = make([]Stat, 0)
	var ikeys []interface{}

	if ikeys, err = listStats(conn, "counter"); err != nil {
		return
	}

	for _, ikey := range ikeys {
		counter := Counter(string(ikey.([]uint8)))
		allStats = append(allStats, &counter)
	}

	// if ikeys, err = listStats(conn, "gauge"); err != nil {
	//  return
	// }
	// for _, ikey := range ikeys {
	//  allStats[i] = Counter(string(ikey.([]uint8)))
	//  i+= 1
	// }

	// if ikeys, err = listStats(conn, "presence"); err != nil {
	//  return
	// }
	// for _, ikey := range ikeys {
	//  allStats[i] = Counter(string(ikey.([]uint8)))
	//  i+= 1
	// }

	return
}

func listStats(conn redis.Conn, group string) (ikeys []interface{}, err error) {
	var tmpKeys interface{}
	if tmpKeys, err = conn.Do("SMEMBERS", fmt.Sprintf("key:%s", group)); err != nil {
		return
	}
	return tmpKeys.([]interface{}), nil
}
