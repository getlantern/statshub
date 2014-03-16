// package statshub implements functionality for submitting and querying stats
// from a centralized stats server.
//
// Stats are always submitted on behalf of a specific id, which can be anything.
//
// To run a local server for testing:
//
//     REDIS_ADDR=<host:port> REDIS_PASS=<password> PORT=9000 go run statshub.go
//
// Example stats updates using curl against the local server:
//
//     curl --data-binary \
//     '{"dims": {
//         "country": "es",
//         "user": "bob"
//         },
//       "counters": { "counterA": 50 },
//       "increments": { "counterB": 500 },
//       "gauges": { "gaugeA": 5000 },
//       "members": { "gaugeB": "item1" }
//     }' \
//     "http://localhost:9000/stats/myid1"
//
// Example stats get (for the country dimension):
//
//     curl -i "http://localhost:9000/stats/country"
//
// Example stats get (for all dimensions):
//
//     curl -i "http://localhost:9000/stats/"

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
	cacheExpiration = 1 * time.Minute
)

var (
	queryCache = cache.NewCache()
)

// ClientQueryResponse is a Response to a StatsQuery
type ClientQueryResponse struct {
	Response
	Dims map[string]map[string]*Stats `json:"dims"`
}

// Response is a response to a stats request (update or query)
type Response struct {
	Succeeded bool
	Error     string
}

func init() {
	http.HandleFunc("/stats/", statsHandler)
}

// statsHandler handles requests to /stats
func statsHandler(w http.ResponseWriter, r *http.Request) {
	var id string
	var err error
	if id, err = extractid(r); err != nil {
		fail(w, 400, err)
	}

	if "POST" == r.Method {
		if id == "" {
			id = "unknown"
		}

		w.Header().Set("Content-Type", "application/json")

		statusCode, resp, err := postStats(r, id)
		if err != nil {
			fail(w, statusCode, err)
		} else {
			write(w, 200, resp)
		}
	} else if "GET" == r.Method {
		w.Header().Set("Content-Type", "application/json")

		// check cache
		// TODO: should probably cache more than just the 1 url
		path := r.URL.Path
		requestedCountryStats := path == "/stats/country"
		if requestedCountryStats {
			cached := queryCache.Get()
			if cached != nil {
				w.WriteHeader(200)
				w.Write(cached)
				return
			} else {
				log.Printf("Countries not found in cache, querying")
			}
		}
		statusCode, resp, err := getStats(r, id)
		if err != nil {
			fail(w, statusCode, err)
		} else {
			w.WriteHeader(statusCode)
			bytes, err := json.Marshal(resp)
			if err == nil {
				w.Write(bytes)
				if requestedCountryStats {
					queryCache.Set(bytes, cacheExpiration)
				}
			} else {
				log.Printf("Unable to respond to client: %s", err)
			}
		}
	} else {
		log.Printf("Query: %s", r.URL.Query())
		w.WriteHeader(405)
	}
}

// postStats handles a POST request to /stats
func postStats(r *http.Request, id string) (statusCode int, resp interface{}, err error) {
	decoder := json.NewDecoder(r.Body)
	stats := &StatsUpdate{}
	err = decoder.Decode(stats)
	if err != nil {
		return 400, nil, fmt.Errorf("Unable to decode request: %s", err)
	}

	if err = stats.write(id); err != nil {
		formattedError := fmt.Errorf("Unable to post stats: %s", err)
		log.Println(formattedError)
		return 500, nil, formattedError
	}

	return 200, &Response{Succeeded: true}, nil
}

// getStats handles a GET request to /stats
func getStats(r *http.Request, dim string) (statusCode int, resp interface{}, err error) {
	clientResp := &ClientQueryResponse{
		Response: Response{Succeeded: true},
	}

	var dimNames []string = nil
	if dim != "" {
		dimNames = []string{dim}
	}
	if clientResp.Dims, err = QueryDims(dimNames); err != nil {
		return 500, nil, fmt.Errorf("Unable to query stats: %s", err)
	}

	return 200, clientResp, nil
}

// extractid extracts the id from the request url
func extractid(r *http.Request) (id string, err error) {
	// Figure out the id
	lastSlash := strings.LastIndex(r.URL.Path, "/")
	if lastSlash == 0 {
		id = ""
	} else {
		id = r.URL.Path[lastSlash+1:]
	}
	return
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
	} else {
		log.Printf("Unable to respond to client: %s", err)
	}
}
