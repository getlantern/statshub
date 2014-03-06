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
)

type Stats struct {
	UserId      int64
	Hash        string // sha256(real userid + userid)
	CountryCode string
	Counters    map[string]int64
	Gauges      map[string]int64
}

func init() {
	http.HandleFunc("/", handler)
}

func handler(w http.ResponseWriter, r *http.Request) {
	context := appengine.NewContext(r)
	user, err := user.CurrentOAuth(context, "")
	if err != nil {
		fmt.Fprintln(w, "Unable to authenticate: %s", err)
		return
	}

	decoder := json.NewDecoder(r.Body)
	stats := &Stats{}
	err = decoder.Decode(stats)
	if err != nil {
		fmt.Fprintln(w, "Unable to decode stats: %s", err)
		return
	}

	hasher := sha256.New()
	hashInput := fmt.Sprintf("%s%d", user.Email, stats.UserId)
	log.Printf("Hash input: %s", hashInput)
	hasher.Write([]byte(user.Email))
	hasher.Write([]byte(fmt.Sprintf("%d", stats.UserId)))
	expectedHash := hex.EncodeToString(hasher.Sum(nil))
	log.Printf("Expected hash: %s", expectedHash)
	log.Printf("Actual hash: %s", stats.Hash)

	conn, err := connectToRedis()
	if err != nil {
		fmt.Fprintln(w, "Unable to connect to redis: %s", err)
		return
	}

	if err = postStats(conn, stats); err != nil {
		fmt.Fprintln(w, "Unable to post stats: %s", err)
	} else {
		fmt.Fprintln(w, "Posted stats!")
	}
}

func connectToRedis() (conn redis.Conn, err error) {
	if conn, err = redis.Dial("tcp", "pub-redis-10905.us-central1-1-1.gce.garantiadata.com:10905"); err != nil {
		return
	}
	_, err = conn.Do("AUTH")
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
