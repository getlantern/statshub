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
	"github.com/garyburd/redigo/redis"
	"log"
	"os"
	"time"
)

const (
	redisConnectTimeout = 10 * time.Second
	redisReadTimeout    = 10 * time.Second
	redisWriteTimeout   = 10 * time.Second
)

var (
	pool *redis.Pool
)

// init() initializes our redis environment by setting up a connection pool
func init() {
	pool = &redis.Pool{
		MaxIdle:     100,
		MaxActive:   1000,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialTimeout(
				"tcp",
				os.Getenv("REDIS_ADDR"),
				redisConnectTimeout,
				redisReadTimeout,
				redisWriteTimeout)
			if err != nil {
				log.Fatalf("Unable to dial redis: %s", err)
			}
			if _, err := c.Do("AUTH", os.Getenv("REDIS_PASS")); err != nil {
				c.Close()
				log.Fatalf("Unable to authenticate to redis: %s", err)
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

// redisConn is a wrapper for a redis.Conn that itself implements the
// redis.Conn interface. Unlike a normal redis.Conn, redisConn stops processing
// new commands after it encountersits first error.
type redisConn struct {
	orig redis.Conn
	err  error
}

// connectToRedis() connects to our cloud Redis server and authenticates
func connectToRedis() (conn redis.Conn, err error) {
	return &redisConn{orig: pool.Get()}, nil
}

func (conn *redisConn) Close() (err error) {
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
