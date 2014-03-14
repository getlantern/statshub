package archive

// import (
// 	"github.com/getlantern/statshub/statshub"
// 	"log"
// 	"os"
// 	"time"
// )

// const (
// 	GOOGLE_PROJECT = "GOOGLE_PROJECT"

// 	datasetId       = "statshub"
// 	tableId         = "archive"
// 	archiveInterval = 10 * time.Minute
// )

// var (
// 	projectId = os.Getenv(GOOGLE_PROJECT)
// )

// // Start starts a goroutine that continuously archives data at regular intervals
// // based on the archiveInterval constant.
// func Start() {
// 	go func() {
// 		for {
// 			nextInterval := time.Now().Truncate(archiveInterval).Add(archiveInterval)
// 			waitTime := nextInterval.Sub(time.Now())
// 			time.Sleep(waitTime)
// 			if err := archiveToBigQuery(); err != nil {
// 				log.Printf("Unable to archive to BigQuery: %s", err)
// 			}
// 		}
// 	}()
// }

// func archiveToBigQuery() error {
// 	if resp, err := statshub.Query("", true); err != nil {
// 		return err
// 	} else if statsTable, err := NewStatsTable(projectId, datasetId, tableId); err != nil {
// 		return err
// 	} else {
// 		return statsTable.WriteStats(resp, time.Now().Truncate(archiveInterval))
// 	}
// }
