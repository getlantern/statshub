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

// import (
// 	"code.google.com/p/go.net/websocket"
// 	"encoding/json"
// 	"fmt"
// 	"github.com/oxtoacart/go-ringbuffer/ringbuff"
// 	"io"
// 	"log"
// 	"net/http"
// 	"sync"
// 	"time"
// )

// const (
// 	streamingInterval = 30 * time.Second
// )

// var (
// 	nextStreamingClientId = 0
// 	streamingClients      = make(map[int]*streamingClient)
// 	newStreamingClient    = make(chan *streamingClient)
// 	closedStreamingClient = make(chan int)
// 	updatesCacheSize      = 1 * 60 * 60 / int(streamingInterval.Seconds()) // 1 hour worth of updates
// 	oldUpdates            = ringbuff.New(updatesCacheSize)
// )

// type streamingClient struct {
// 	ws      *websocket.Conn
// 	updates chan *streamingUpdate
// 	id      chan int
// }

// type streamingUpdate struct {
// 	asOf    time.Time
// 	allDims map[string]*json.RawMessage // using pointer because of this bug - https://code.google.com/p/go/issues/detail?id=6528
// }

// // ClientQueryResponse is a Response to a StatsQuery
// type StreamingQueryResponse struct {
// 	Response
// 	AsOfSeconds int64                       `json:"asOfSeconds"`
// 	Dims        map[string]*json.RawMessage `json:"dims"`
// }

// func init() {
// 	http.Handle("/stream/", websocket.Handler(streamStats))
// 	go handleStreamingClients()
// }

// // handleStreamingClients handles streaming updates to subscribed streaming clients
// func handleStreamingClients() {
// 	for {
// 		nextInterval := time.Now().Truncate(streamingInterval).Add(streamingInterval)
// 		waitTime := nextInterval.Sub(time.Now())
// 		select {
// 		case client := <-newStreamingClient:
// 			// Add new client to map
// 			nextStreamingClientId++
// 			streamingClients[nextStreamingClientId] = client
// 			client.id <- nextStreamingClientId

// 			// Send buffered updates to client
// 			oldUpdates.ForEach(func(item interface{}) {
// 				update := item.(*streamingUpdate)
// 				for _, client := range streamingClients {
// 					client.updates <- update
// 				}
// 			})
// 		case closedId := <-closedStreamingClient:
// 			// Remove disconnected client from map
// 			delete(streamingClients, closedId)
// 		case <-time.After(waitTime):
// 			// Query fallback and country dims
// 			dims, err := QueryDims([]string{"fallback", "country"})
// 			if err != nil {
// 				log.Printf("Unable to query dims: %s", err)
// 			} else {
// 				// Encode dims as JSON
// 				jsonDims := make(map[string]*json.RawMessage)
// 				for name, dim := range dims {
// 					encoded, err := json.Marshal(dim)
// 					if err != nil {
// 						log.Printf("Unable to json encode dim %s: %s", name, err)
// 					} else {
// 						raw := json.RawMessage(encoded)
// 						jsonDims[name] = &raw
// 					}
// 				}

// 				// Publish update to clients
// 				update := &streamingUpdate{asOf: nextInterval, allDims: jsonDims}
// 				for _, client := range streamingClients {
// 					client.updates <- update
// 				}

// 				// Remember update
// 				oldUpdates.Add(update)
// 			}
// 		}
// 	}
// }

// // streamStats streams stats over a websocket
// func streamStats(ws *websocket.Conn) {
// 	dim, err := extractid(ws.Request())
// 	if err != nil {
// 		data, err := json.Marshal(&Response{Succeeded: false, Error: fmt.Sprintf("Unable to extract id from request: %s", err)})
// 		if err == nil {
// 			ws.Write(data)
// 		}
// 		return
// 	}

// 	dimNames := dimNamesFor(dim)

// 	client := &streamingClient{ws: ws, updates: make(chan *streamingUpdate, updatesCacheSize*2), id: make(chan int)}

// 	var wg sync.WaitGroup
// 	wg.Add(1)

// 	// Write updates to the client
// 	go func() {
// 		for {
// 			// This gets data for all dims
// 			update := <-client.updates
// 			dims := update.allDims
// 			if dimNames != nil {
// 				// Grab only the dimensions that this client is interested in
// 				dims = make(map[string]*json.RawMessage)
// 				for _, name := range dimNames {
// 					dims[name] = update.allDims[name]
// 				}
// 			}
// 			resp := &StreamingQueryResponse{Response: Response{Succeeded: true}, AsOfSeconds: update.asOf.Unix(), Dims: dims}
// 			encoded, err := json.Marshal(resp)
// 			if err != nil {
// 				log.Printf("Unable to marshal json: %s", err)
// 			} else {
// 				ws.Write(encoded)
// 			}
// 		}
// 	}()

// 	// Read from the client (we don't expect to get anything, but this allows us
// 	// to check for closed connections)
// 	go func() {
// 		id := <-client.id
// 		msg := make([]byte, 1)
// 		for {
// 			_, err := ws.Read(msg)
// 			if err == io.EOF {
// 				closedStreamingClient <- id
// 				ws.Close()
// 				wg.Done()
// 				return
// 			}
// 		}
// 	}()

// 	newStreamingClient <- client
// 	wg.Wait()
// }
