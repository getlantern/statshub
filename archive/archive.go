package archive

import (
	"github.com/getlantern/statshub/statshub"
	"log"
	"os"
	"time"
)

const (
	GOOGLE_PROJECT = "GOOGLE_PROJECT"

	datasetId       = "statshub"
	archiveInterval = 10 * time.Minute
)

var (
	projectId = os.Getenv(GOOGLE_PROJECT)

	archivedDimensions = []string{"country", "user", "fallback"}
)

// Start starts a goroutine that continuously archives data at regular intervals
// based on the archiveInterval constant.
func Start() {
	if projectId == "" {
		log.Println("No GOOGLE_PROJECT environment variable set, not archiving to BigQuery")
	} else {
		log.Printf("Archiving to BigQuery at %s", projectId)
		go func() {
			for {
				nextInterval := time.Now().Truncate(archiveInterval).Add(archiveInterval)
				waitTime := nextInterval.Sub(time.Now())
				time.Sleep(waitTime)
				if err := archiveToBigQuery(); err != nil {
					log.Printf("Unable to archive to BigQuery: %s", err)
				}
			}
		}()
	}
}

func archiveToBigQuery() error {
	if statsByDim, err := statshub.QueryDims(archivedDimensions); err != nil {
		return err
	} else {
		for dimName, dimStats := range statsByDim {
			if statsTable, err := NewStatsTable(projectId, datasetId, dimName); err != nil {
				return err
			} else {
				return statsTable.WriteStats(dimStats, time.Now().Truncate(archiveInterval))
			}
		}
		return nil
	}
}
