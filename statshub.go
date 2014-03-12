// package main starts up a local statshub server
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
