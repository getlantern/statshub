package statshub

import (
	"appengine"
	"appengine/socket"
	"github.com/garyburd/redigo/redis"
	"net"
	"time"
)

const (
	redisConnectTimeout = 10 * time.Second
	redisReadTimeout    = 10 * time.Second
	redisWriteTimeout   = 10 * time.Second
)

// connectToRedis() connects to our cloud Redis server and authenticates
func connectToRedis(context appengine.Context) (conn redis.Conn, err error) {
	var nconn net.Conn

	if nconn, err = socket.DialTimeout(context, "tcp", redisAddr, redisConnectTimeout); err != nil {
		return
	}

	conn = redis.NewConn(nconn, redisReadTimeout, redisWriteTimeout)

	_, err = conn.Do("AUTH", redisPassword)
	return
}
