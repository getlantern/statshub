package main

import (
	_ "github.com/getlantern/statshub/statshub"
	"log"
	"net/http"
	"os"
)

func main() {
	port := os.Getenv("PORT")
	log.Printf("About to listen at port: %s", port)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		panic(err)
	}
}
