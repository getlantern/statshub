// package statshub implements functionality for submitting and querying stats
// from a centralized stats server.
//
// Stats are always submitted on behalf of a specific user, who is identified by an anonymized integer userid.
//
// To run a local server for testing:
//
//     REDIS_ADDR=<host:port> REDIS_PASS=<password> PORT=9000 go run statshub.go
//
// Example stats updates using curl against the local server:
//
//     curl --data-binary '{"countryCode": "es", "counter": { "mystat": 1, "myotherstat": 50 }, "gauge": {"mygauge": 78, "online": 1}}' "http://localhost:9000/stats/523523"
//     curl --data-binary '{"countryCode": "es", "counter": { "mystat": 2, "myotherstat": 60 }, "gauge": {"mygauge": 55, "online": 1}}' "http://localhost:9000/stats/523524"
//
// Example stats get:
//
//     curl -i "http://localhost:9000/stats/523523"
//
package statshub

import (
	"encoding/json"
	"fmt"
	"github.com/getlantern/statshub/cache"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	rollupExpiration = 1 * time.Minute
)

var (
	rollupCache = cache.NewCache()
)

// ClientQueryResponse is a Response to a StatsQuery
type ClientQueryResponse struct {
	Response
	User    *Stats           `json:"user"` // Stats for the user
	Rollups *json.RawMessage `json:"rollups"`
}

type CachedRollups struct {
	Global     *Stats            `json:"global"`     // Global stats
	PerCountry map[string]*Stats `json:"perCountry"` // Maps country codes to stats for those countries
}

// Response is a response to a stats request (update or query)
type Response struct {
	Succeeded bool
	Error     string
}

func init() {
	http.HandleFunc("/stats/", statsHandler)
}

// statsPostHandler handles requests to /stats
func statsHandler(w http.ResponseWriter, r *http.Request) {
	var userId string
	var err error
	if userId, err = extractUserId(r); err != nil {
		fail(w, 400, err)
	}

	if "POST" == r.Method {
		w.Header().Set("Content-Type", "application/json")

		statusCode, resp, err := postStats(r, userId)
		if err != nil {
			fail(w, statusCode, err)
		} else {
			write(w, 200, resp)
		}
	} else if "GET" == r.Method {
		w.Header().Set("Content-Type", "application/json")

		statusCode, resp, err := getStats(r, userId)
		if err != nil {
			fail(w, statusCode, err)
		} else {
			write(w, 200, resp)
		}
	} else {
		log.Printf("Query: %s", r.URL.Query())
		w.WriteHeader(405)
	}
}

// postStats handles a POST request to /stats
func postStats(r *http.Request, userId string) (statusCode int, resp interface{}, err error) {
	decoder := json.NewDecoder(r.Body)
	stats := &StatsUpdate{}
	err = decoder.Decode(stats)
	if err != nil {
		return 400, nil, fmt.Errorf("Unable to decode request: %s", err)
	}

	if err = stats.postToRedis(userId); err != nil {
		formattedError := fmt.Errorf("Unable to post stats: %s", err)
		log.Println(formattedError)
		return 500, nil, formattedError
	}

	return 200, &Response{Succeeded: true}, nil
}

// getStats handles a GET request to /stats
func getStats(r *http.Request, userId string) (statusCode int, resp interface{}, err error) {
	clientResp := &ClientQueryResponse{
		Response: Response{Succeeded: true},
	}

	conn, err := connectToRedis()
	if err != nil {
		return 500, nil, fmt.Errorf("Unable to connect to redis: %s", err)
	}
	defer conn.Close()

	var calculateRollups = false
	cachedRollups := rollupCache.Get()
	if cachedRollups == nil {
		log.Println("Recomputing rollups")
		calculateRollups = true
	} else {
		raw := json.RawMessage(cachedRollups)
		clientResp.Rollups = &raw
	}

	var queryResp *QueryResponse
	if queryResp, err = query(conn, userId, calculateRollups); err != nil {
		return 500, nil, fmt.Errorf("Unable to query stats: %s", err)
	}
	clientResp.User = queryResp.User
	if calculateRollups {
		rollups := &CachedRollups{
			Global:     queryResp.Global,
			PerCountry: queryResp.PerCountry,
		}
		bytes, _ := json.Marshal(&rollups)
		raw := json.RawMessage(bytes)
		clientResp.Rollups = &raw
		rollupCache.Set(bytes, rollupExpiration)
	}

	return 200, clientResp, nil
}

func extractUserId(r *http.Request) (userId string, err error) {
	// Figure out the UserId
	lastSlash := strings.LastIndex(r.URL.Path, "/")
	if lastSlash == 0 {
		return "", fmt.Errorf("Request URL is missing user id")
	}
	return r.URL.Path[lastSlash+1:], nil
}

func fail(w http.ResponseWriter, statusCode int, err error) {
	response := Response{Succeeded: false, Error: fmt.Sprintf("%s", err)}
	write(w, statusCode, response)
}

func write(w http.ResponseWriter, statusCode int, data interface{}) {
	w.WriteHeader(statusCode)
	bytes, err := json.Marshal(data)
	if err == nil {
		w.Write(bytes)
	}
	if err != nil {
		log.Printf("Unable to respond to client: %s", err)
	}
}
