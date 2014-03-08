// package statshub implements functionality for submitting and querying stats
// from a centralized stats server.
//
// Stats are always submitted on behalf of a specific user, who is identified by an anonymized integer userid.
// The user is authenticated using Google OAuth, and the userid is checked against the logged-in user by
// comparing a sha-256 hash of the real userid + anonymized userid.  Stats are only stored for anonymized
// user ids.
//
// Example stats submission using curl against a local appengine dev server:
//
//     curl --data-binary '{"countryCode": "ES", "counters": { "mystat": 1, "myotherstat": 50 }, "gauges": {"mygauge": 78}, "presence": {"online": true}}' "http://localhost:8080/stats/523523?hash=c78c666ec1016b8ed66b40bb46e0883020ff7c9d2f2010c0e2dbfbfc358888a2"
//
package statshub

import (
	"appengine"
	"appengine/user"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
)

// UserInfo captures the UserId and authentication Hash for a request.
// Requests are authenticated using OAuth and confirmed to be for the
// requested user by matching a sha256 hash of real userid + UserId.
type UserInfo struct {
	UserId uint64
	Hash   string // sha256(real userid + userid)
}

// StatsQuery is a query for stats of a particular user, including the names
// of the Counters, Gauges and Presence stats that are desired.
type StatsQuery struct {
	Counters []string
	Gauges   []string
	Presence []string
}

// Response is a response to a stats request (submission or query)
type Response struct {
	Succeeded bool
	Error     string
}

// QueryResponse is a Response to a StatsQuery
type QueryResponse struct {
	Response
	Counters map[string]uint64
	Gauges   map[string]uint64
	Presence map[string]bool
}

func init() {
	http.HandleFunc("/stats/", statsHandler)
}

// statsPostHandler handles requests to /stats
func statsHandler(w http.ResponseWriter, r *http.Request) {
	userInfo, err := getUserInfo(r)
	if err != nil {
		fail(w, 400, err)
		return
	}

	statusCode, err := userInfo.authenticateAgainst(r)
	if err != nil {
		fail(w, statusCode, err)
		return
	}

	if "POST" == r.Method {
		w.Header().Set("Content-Type", "application/json")

		statusCode, err := postStats(r, userInfo)
		if err != nil {
			fail(w, statusCode, err)
		} else {
			succeed(w)
		}
	} else {
		log.Printf("Query: %s", r.URL.Query())
		w.WriteHeader(405)
	}
}

// postStats handles a POST request to /stats
func postStats(r *http.Request, userInfo *UserInfo) (statusCode int, err error) {
	decoder := json.NewDecoder(r.Body)
	stats := &StatsSubmission{}
	err = decoder.Decode(stats)
	if err != nil {
		return 400, fmt.Errorf("Unable to decode request: %s", err)
	}

	conn, err := connectToRedis()
	if err != nil {
		return 500, fmt.Errorf("Unable to connect to redis: %s", err)
	}

	if err = stats.postToRedis(conn, userInfo.UserId); err != nil {
		return 500, fmt.Errorf("Unable to post stats: %s", err)
	}

	return 200, nil
}

func getUserInfo(r *http.Request) (userInfo *UserInfo, err error) {
	userInfo = &UserInfo{}

	// Figure out the UserId
	lastSlash := strings.LastIndex(r.URL.Path, "/")
	if lastSlash == 0 {
		return nil, fmt.Errorf("Request URL is missing user id")
	}
	userIdString := r.URL.Path[lastSlash+1:]
	userIdInt, err := strconv.Atoi(userIdString)
	if err != nil {
		return nil, fmt.Errorf("Unable to convert userId %s to int: %s", userIdString, err)
	}
	userInfo.UserId = uint64(userIdInt)

	// Figure out the Hash
	hashes, ok := r.URL.Query()["hash"]
	if !ok {
		return nil, fmt.Errorf("No hash provided in querystring")
	}
	if len(hashes) != 1 {
		return nil, fmt.Errorf("Wrong number of hashes provided in querystring")
	}
	userInfo.Hash = hashes[0]

	return
}

// authenticateAgainst compares the Hash in the request with the hash
// calculated based on the currently logged in user.
func (userInfo *UserInfo) authenticateAgainst(r *http.Request) (statusCode int, err error) {
	// Get the currently logged in user
	context := appengine.NewContext(r)
	currentUser, err := user.CurrentOAuth(context, "")
	if err != nil {
		return 401, fmt.Errorf("Not authenticated: %s", err)
	}

	hasher := sha256.New()
	hasher.Reset()
	hashInput := fmt.Sprintf("%s%d", currentUser.Email, userInfo.UserId)
	hasher.Write([]byte(hashInput))
	expectedHash := hex.EncodeToString(hasher.Sum(nil))

	if expectedHash != userInfo.Hash {
		return 403, fmt.Errorf("Hash mismatch, authentication failure")
	} else {
		return
	}
}

func succeed(w http.ResponseWriter) {
	response := &Response{Succeeded: true}
	response.write(w, 200)
}

func fail(w http.ResponseWriter, statusCode int, err error) {
	response := Response{Succeeded: false, Error: fmt.Sprintf("%s", err)}
	response.write(w, statusCode)
}

func (response *Response) write(w http.ResponseWriter, statusCode int) {
	w.WriteHeader(statusCode)
	bytes, err := json.Marshal(&response)
	if err == nil {
		w.Write(bytes)
	}
	if err != nil {
		log.Printf("Unable to respond to client: %s", err)
	}
}
