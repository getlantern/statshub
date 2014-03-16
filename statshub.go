// statshub is a repository for incrementally calculated statistics stored using a
// dimensional model.
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
// See the README at https://github.com/getlantern/statshub for more information.
package main

import (
	"github.com/getlantern/statshub/archive"
	_ "github.com/getlantern/statshub/statshub"
	"log"
	"net/http"
	"os"
)

func main() {
	archive.Start()

	port := os.Getenv("PORT")
	log.Printf("About to listen at port: %s", port)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		panic(err)
	}
}
