package statshub

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

const (
	redisConnectTimeout = 10 * time.Second
	redisReadTimeout    = 10 * time.Second
	redisWriteTimeout   = 10 * time.Second
)

// connectToRedis() connects to our cloud Redis server and authenticates
func connectToRedis() (conn redis.Conn, err error) {
	conn, err = redis.DialTimeout("tcp",
		redisAddr,
		redisConnectTimeout,
		redisReadTimeout,
		redisWriteTimeout,
	)
	if err != nil {
		return
	}
	_, err = conn.Do("AUTH", redisPassword)
	return
}
