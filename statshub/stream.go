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
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.google.com/p/go.net/websocket"

	"github.com/getlantern/statshub/bigquery"
)

const (
	ANY                = "*"
	STREAMING_INTERVAL = 30 * time.Second

	INTERVAL_QUERY_TEMPL = `
SELECT
    INTEGER(TIMESTAMP_TO_SEC(_ts) / %d) AS period,
	_dim,
	MAX(%s.%s) AS value
FROM [%s]
WHERE
	_ts <= DATE_ADD(CURRENT_TIMESTAMP(), -%d, "DAY")
    AND _ts > DATE_ADD(CURRENT_TIMESTAMP(), -%d, "DAY")
    %s
GROUP BY period, _dim
ORDER BY period`

	DIM_WHERE_TEMPL = "AND _dim = '%s'"

	ONE_MINUTE = 60
	ONE_HOUR   = 60 * ONE_MINUTE
	ONE_DAY    = 24 * ONE_HOUR
	ONE_WEEK   = 7 * ONE_DAY
	ONE_MONTH  = 30 * ONE_DAY  // Approximation
	ONE_YEAR   = 365 * ONE_DAY // Approximation
)

var (
	nextStreamingClientId = 0
	streamingClients      = make(map[int]*streamingClient)
	newStreamingClient    = make(chan *streamingClient)
	closedStreamingClient = make(chan int)
)

type streamingClient struct {
	ws       *websocket.Conn
	updates  chan *streamingUpdate
	id       chan int
	dimName  string // the name of the dimension that this client is querying (e.g. "fallback")
	dimKey   string // the key of the dimension that this client is querying (e.g. "instance_fp-afisk-at-getlantern-dot-org-50e8-4-2014-2-24" or "total")
	statType string // the type of stat being queried (e.g. "counter" or "gauge")
	statName string // the name of the stat being queried (e.g. "bytesGiven")
}

type streamingUpdate struct {
	asOf time.Time
	dims map[string]map[string]*Stats
}

// ClientQueryResponse is a Response to a StatsQuery
type StreamingQueryResponse struct {
	Response
	Intervals []StreamingQueryResponseInterval `json:"intervals"`
}

type StreamingQueryResponseInterval struct {
	AsOfSeconds int64            `json:"asOfSeconds"`
	Values      map[string]int64 `json:"values"`
}

func init() {
	http.Handle("/stream/", websocket.Handler(streamStats))
	go handleStreamingClients()
}

// handleStreamingClients handles streaming updates to subscribed streaming clients
func handleStreamingClients() {
	for {
		nextInterval := time.Now().Truncate(STREAMING_INTERVAL).Add(STREAMING_INTERVAL)
		waitTime := nextInterval.Sub(time.Now())
		select {
		case client := <-newStreamingClient:
			// Add new client to map
			nextStreamingClientId++
			streamingClients[nextStreamingClientId] = client
			client.id <- nextStreamingClientId
		case closedId := <-closedStreamingClient:
			// Remove disconnected client from map
			delete(streamingClients, closedId)
		case <-time.After(waitTime):
			// Query fallback and country dims
			// TODO: only query for the stuff that clients have asked for
			dims, err := QueryDims([]string{"fallback", "country"})
			if err != nil {
				log.Printf("Unable to query dims: %s", err)
			} else {
				// Publish update to clients
				update := &streamingUpdate{asOf: nextInterval, dims: dims}
				for _, client := range streamingClients {
					client.updates <- update
				}
			}
		}
	}
}

// streamStats streams stats over a websocket
func streamStats(ws *websocket.Conn) {
	singleSlashPath := strings.Replace(ws.Request().URL.Path, "//", "/", -1)
	pathParts := strings.Split(singleSlashPath, "/")

	if len(pathParts) < 6 {
		data, err := json.Marshal(&Response{Succeeded: false, Error: fmt.Sprintf("Wrong path: %s. Expected something like: %s", singleSlashPath, "/stream/country/*/counter/bytesGiven")})
		if err == nil {
			ws.Write(data)
		}
		return
	}

	client := &streamingClient{
		ws:       ws,
		updates:  make(chan *streamingUpdate, 100),
		id:       make(chan int),
		dimName:  pathParts[2],
		dimKey:   pathParts[3],
		statType: pathParts[4],
		statName: pathParts[5],
	}

	client.loadHistory()

	go client.writeUpdates()

	var wg sync.WaitGroup
	wg.Add(1)

	// Read from the client (we don't expect to get anything, but this allows us
	// to check for closed connections)
	go func() {
		id := <-client.id
		msg := make([]byte, 1)
		for {
			_, err := ws.Read(msg)
			if err == io.EOF {
				closedStreamingClient <- id
				ws.Close()
				wg.Done()
				return
			}
		}
	}()

	newStreamingClient <- client
	wg.Wait()
}

// loadHistory loads historical data based on the specified query and sends it
// to the client
func (client *streamingClient) loadHistory() {
	intervals := []StreamingQueryResponseInterval{}
	// Monthly figures for  1 month back to 1 year back
	intervals = client.loadHistoryForRange(intervals, ONE_MONTH, ONE_MONTH, ONE_YEAR)
	// Weekly figures for 1 week back to 1 month back
	intervals = client.loadHistoryForRange(intervals, ONE_WEEK, ONE_WEEK, ONE_MONTH)
	// Daily figures for 1 day back to 1 week back
	intervals = client.loadHistoryForRange(intervals, ONE_DAY, ONE_DAY, ONE_WEEK)
	// Hourly figures for the last 1 day
	intervals = client.loadHistoryForRange(intervals, ONE_HOUR, 0, ONE_DAY)

	resp := &StreamingQueryResponse{
		Response:  Response{Succeeded: true},
		Intervals: intervals,
	}
	client.writeResponse(resp)
}

// loadHistoryForRange loads history for a date range
func (client *streamingClient) loadHistoryForRange(
	intervals []StreamingQueryResponseInterval,
	intervalInSeconds int,
	startOffsetInDays int,
	endOffsetInDays int) []StreamingQueryResponseInterval {

	additionalWhereClause := ""
	if client.dimKey != ANY {
		additionalWhereClause = fmt.Sprintf(DIM_WHERE_TEMPL, client.dimKey)
	}

	// Yup, this allows SQL injection, but the BigQuery database doesn't allow
	// any updates of the database, only queries, so we don't worry about it
	queryString := fmt.Sprintf(
		INTERVAL_QUERY_TEMPL,
		intervalInSeconds,
		client.statType,
		client.statName,
		client.dimName,
		startOffsetInDays,
		endOffsetInDays,
		additionalWhereClause)
	rows, err := bigquery.Query(queryString, math.MaxInt32)
	if err != nil {
		log.Printf("Unable to run query: %s\n%s\n\n", err, queryString)
	}

	if len(rows) > 0 {
		lastCutoff := int64(0) // will cause first row to be seen as a new cutoff
		var interval StreamingQueryResponseInterval
		for _, row := range rows {
			cutoff, err := strconv.ParseInt(row[0].(string), 10, 64)
			if err != nil {
				log.Printf("Unable to read cutoff %s: %s", row[0], err)
				return intervals
			}
			if cutoff != lastCutoff {
				// Start a new interval
				asOf := cutoff * int64(intervalInSeconds)
				interval = StreamingQueryResponseInterval{asOf, make(map[string]int64)}
				intervals = append(intervals, interval)
			}
			lastCutoff = cutoff

			dim := row[1].(string)

			value := int64(0)
			valueIf := row[2]
			if valueIf != nil {
				value, err = strconv.ParseInt(valueIf.(string), 10, 64)
				if err != nil {
					log.Printf("Unable to read value %s: %s", row[2], err)
					return intervals
				}
			}
			interval.Values[dim] = value
		}
	}

	return intervals
}

// writeUpdates grabs streaming updates and sends them to the client
func (client *streamingClient) writeUpdates() {
	for {
		// This gets data for all dims
		update := <-client.updates
		values := make(map[string]int64)
		dim := update.dims[client.dimName]
		queryingSpecificDimKey := client.dimKey != ANY
		if dim != nil {
			for dimKey, stats := range dim {
				if !queryingSpecificDimKey || dimKey == client.dimKey {
					switch client.statType {
					case "counter":
						values[dimKey] = stats.Counters[client.statName]
					case "gauge":
						values[dimKey] = stats.Gauges[client.statName]
					default:
						log.Printf("Client has unknown statType: %s", client.statType)
					}
				}
			}
		}
		resp := &StreamingQueryResponse{
			Response: Response{Succeeded: true},
			Intervals: []StreamingQueryResponseInterval{
				StreamingQueryResponseInterval{update.asOf.Unix(), values},
			},
		}
		client.writeResponse(resp)
	}
}

func (client *streamingClient) writeResponse(resp *StreamingQueryResponse) {
	encoded, err := json.Marshal(resp)
	if err != nil {
		log.Printf("Unable to marshal json: %s", err)
	} else {
		client.ws.Write(encoded)
	}
}
