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

package archive

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/getlantern/statshub/bigquery"
	"github.com/getlantern/statshub/statshub"
)

const (
	ARCHIVED_DIMS = "ARCHIVED_DIMS"
)

var (
	frequentlyArchivedDimensions   = []string{"country", "user", "fallback"}
	infrequentlyArchivedDimensions = []string{"user"}
)

// StartArchiving starts a goroutine that continuously archives data at regular intervals
// based on the archiveInterval constant.
func StartArchiving() {
	// Expects something like "country,10 fallback,10"
	ads := os.Getenv(ARCHIVED_DIMS)
	shouldArchive := ads != ""
	if shouldArchive {
		log.Printf("Archiving to BigQuery at %s", bigquery.ProjectId)
		archivedDims := strings.Split(ads, " ")
		for _, dimSpec := range archivedDims {
			pieces := strings.Split(dimSpec, ",")
			dim := pieces[0]
			minutes, err := strconv.Atoi(pieces[1])
			if err != nil {
				log.Printf("Unable to archive dim '%s', invalid # of minutes '%s': %s", dim, pieces[1], err)
			} else {
				log.Printf("Archiving dim '%s' every %d minutes", dim, minutes)
				archivePeriodically(dim, time.Duration(minutes)*time.Minute)
			}
		}
	} else {
		log.Printf("Not archiving to BigQuery")
	}
}

func archivePeriodically(dim string, interval time.Duration) {
	go func() {
		for {
			nextInterval := time.Now().Truncate(interval).Add(interval)
			waitTime := nextInterval.Sub(time.Now())
			time.Sleep(waitTime)
			if err := archiveToBigQuery(dim, interval); err != nil {
				log.Printf("Unable to archive dimension %s to BigQuery: %s", dim, err)
			}
		}
	}()
}

func archiveToBigQuery(dim string, interval time.Duration) error {
	if statsByDim, err := statshub.QueryDims([]string{dim}); err != nil {
		return err
	} else {
		for dimName, dimStats := range statsByDim {
			if statsTable, err := NewStatsTable(bigquery.ProjectId, bigquery.DATASET_ID, dimName); err != nil {
				return err
			} else {
				return statsTable.WriteStats(dimStats, time.Now().Truncate(interval))
			}
		}
		return nil
	}
}
