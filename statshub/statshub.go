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
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"
	"time"
)

const (
	cacheExpiration = 1 * time.Minute
)

var (
	countriesCached     bool
	countryStats        []byte
	countryStatsRequest = make(chan chan []byte, 1000)
)

// ClientQueryResponse is a Response to a StatsQuery
type ClientQueryResponse struct {
	Response
	Dims map[string]map[string]*Stats `json:"dims"`
}

// Response is a response to a stats request (update or query)
type Response struct {
	Succeeded bool   `json:"succeeded"`
	Error     string `json:"error"`
}

func init() {
	http.HandleFunc("/stats/", statsHandler)
	go cacheCountries()
}

// statsHandler handles requests to /stats
func statsHandler(w http.ResponseWriter, r *http.Request) {
	id := path.Base(r.URL.Path)

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
		requestedCountryStats := path == "/stats/country" || path == "/stats/country/"
		if requestedCountryStats {
			countryResponse := make(chan []byte)
			countryStatsRequest <- countryResponse
			cached := <-countryResponse
			if cached == nil {
				fail(w, 500, fmt.Errorf("No countries cached"))
			} else {
				w.WriteHeader(200)
				w.Write(cached)
				return
			}
		} else {
			log.Printf("Not using cache for path: %s", path)
		}
		statusCode, resp, err := getStats(id)
		if err != nil {
			fail(w, statusCode, err)
		} else {
			w.WriteHeader(statusCode)
			bytes, err := json.Marshal(resp)
			if err == nil {
				w.Write(bytes)
			} else {
				log.Printf("Unable to respond to client: %s", err)
			}
		}
	} else {
		log.Printf("Query: %s", r.URL.Query())
		w.WriteHeader(405)
	}
}

func cacheCountries() {
	queryCountries()
	for {
		nextInterval := time.Now().Truncate(cacheExpiration).Add(cacheExpiration)
		waitTime := nextInterval.Sub(time.Now())
		select {
		case req := <-countryStatsRequest:
			if countriesCached {
				req <- countryStats
			} else {
				req <- nil
			}
		case <-time.After(waitTime):
			queryCountries()
		}
	}
}

func queryCountries() {
	log.Printf("Querying countries for cache")
	_, resp, err := getStats("country")
	if err != nil {
		log.Printf("Unable to cache countries: %s", err)
		countriesCached = false
	} else {
		bytes, err := json.Marshal(resp)
		if err == nil {
			countryStats = bytes
			countriesCached = true
		} else {
			countriesCached = false
			log.Printf("Unable to cache countries: %s", err)
		}
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
func getStats(dim string) (statusCode int, resp interface{}, err error) {
	clientResp := &ClientQueryResponse{
		Response: Response{Succeeded: true},
	}

	dimNames := dimNamesFor(dim)

	if clientResp.Dims, err = QueryDims(dimNames); err != nil {
		return 500, nil, fmt.Errorf("Unable to query stats: %s", err)
	}

	return 200, clientResp, nil
}

func dimNamesFor(dim string) []string {
	if dim != "" {
		return []string{dim}
	} else {
		return nil
	}
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
