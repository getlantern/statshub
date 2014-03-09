package statshub

import (
	"github.com/garyburd/redigo/redis"
	"net"
	"time"
)

const (
	redisConnectTimeout = 10 * time.Second
	redisReadTimeout    = 10 * time.Second
	redisWriteTimeout   = 10 * time.Second
)

// redisConn is a redis.Conn that stops processing new commands after it
// encounters its first error.  Unlike redis.Conn, it is not safe to use from
// multiple goroutines.
type redisConn struct {
	orig redis.Conn
	err  error
}

type redisDialer func(addr string, connectTimeout time.Duration) (net.Conn, error)

// connectToRedis() connects to our cloud Redis server and authenticates
func connectToRedis(dial redisDialer) (conn redis.Conn, err error) {
	var nconn net.Conn

	if nconn, err = dial(redisAddr, redisConnectTimeout); err != nil {
		return
	}

	conn = &redisConn{orig: redis.NewConn(nconn, redisReadTimeout, redisWriteTimeout)}

	_, err = conn.Do("AUTH", redisPassword)
	return
}

func (conn *redisConn) Close() error {
	return conn.orig.Close()
}

func (conn *redisConn) Err() error {
	return conn.err
}

func (conn *redisConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	if conn.err != nil {
		return nil, conn.err
	} else {
		reply, err = conn.orig.Do(commandName, args...)
		conn.err = err
		return
	}
}

func (conn *redisConn) Send(commandName string, args ...interface{}) (err error) {
	if conn.err != nil {
		return conn.err
	} else {
		err = conn.orig.Send(commandName, args...)
		conn.err = err
		return
	}
}

func (conn *redisConn) Flush() (err error) {
	if conn.err != nil {
		return conn.err
	} else {
		err = conn.orig.Flush()
		conn.err = err
		return
	}
}

func (conn *redisConn) Receive() (reply interface{}, err error) {
	if conn.err != nil {
		return nil, conn.err
	} else {
		reply, err = conn.orig.Receive()
		conn.err = err
		return
	}
}
