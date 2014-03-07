package statshub

import (
	"appengine"
	"appengine/user"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"net/http"
	"time"
)

var (
	redisConnectTimeout = 10 * time.Second
	redisReadTimeout    = 10 * time.Second
	redisWriteTimeout   = 10 * time.Second
)

type Stats struct {
	UserId      uint64
	Hash        string // sha256(real userid + userid)
	CountryCode string
	Counters    map[string]uint64
	Gauges      map[string]uint64
}

type Response struct {
	Succeeded bool
	Error     string
}

func init() {
	http.HandleFunc("/", handler)
}

func handler(w http.ResponseWriter, r *http.Request) {
	statusCode, err := doHandle(r)
	w.Header().Set("Content-Type", "application/json")
	response := Response{Succeeded: true}
	if err != nil {
		response.Succeeded = false
		response.Error = fmt.Sprintf("%s", err)
	}
	w.WriteHeader(statusCode)
	bytes, err := json.Marshal(&response)
	if err == nil {
		w.Write(bytes)
	}
	if err != nil {
		log.Printf("Unable to respond to client: %s", err)
	}
}

func doHandle(r *http.Request) (statusCode int, err error) {
	context := appengine.NewContext(r)
	user, err := user.CurrentOAuth(context, "")
	if err != nil {
		return 401, fmt.Errorf("Unable to authenticate: %s", err)
	}

	decoder := json.NewDecoder(r.Body)
	stats := &Stats{}
	err = decoder.Decode(stats)
	if err != nil {
		return 400, fmt.Errorf("Unable to decode stats: %s", err)
	}

	hasher := sha256.New()
	hasher.Reset()
	hashInput := fmt.Sprintf("%s%d", user.Email, stats.UserId)
	hasher.Write([]byte(hashInput))
	expectedHash := hex.EncodeToString(hasher.Sum(nil))

	if expectedHash != stats.Hash {
		return 403, fmt.Errorf("Hash mismatch, authentication failure")
	}

	conn, err := connectToRedis()
	if err != nil {
		return 500, fmt.Errorf("Unable to connect to redis: %s", err)
	}

	if err = postStats(conn, stats); err != nil {
		return 500, fmt.Errorf("Unable to post stats: %s", err)
	}

	return 200, nil
}

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

func postStats(conn redis.Conn, stats *Stats) (err error) {
	redisKeys := func(key string) []string {
		return []string{
			fmt.Sprintf("%d:%s", stats.UserId, key),
			fmt.Sprintf("%s:%s", stats.CountryCode, key),
			fmt.Sprintf("global:%s", key),
		}
	}

	for key, value := range stats.Counters {
		for _, redisKey := range redisKeys(key) {
			if err = conn.Send("INCRBY", redisKey, value); err != nil {
				return
			}
		}
	}

	for key, value := range stats.Gauges {
		for _, redisKey := range redisKeys(key) {
			if err = conn.Send("SET", redisKey, value); err != nil {
				return
			}
		}
	}

	conn.Flush()
	return
}
