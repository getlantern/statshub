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
	"strings"
	"time"

	"github.com/getlantern/statshub/bigquery"
	"github.com/getlantern/statshub/statshub"
)

const (
	ARCHIVE_TO_BIGQUERY = "ARCHIVE_TO_BIGQUERY"
)

var (
	shouldArchive = strings.ToLower(os.Getenv(ARCHIVE_TO_BIGQUERY)) == "true"
)

// StartArchiving starts a goroutine that continuously archives data at regular intervals
// based on the archiveInterval constant.
func StartArchiving() {
	if shouldArchive {
		log.Printf("Archiving to BigQuery at %s", bigquery.ProjectId)
		archivePeriodically("fallback", 10*time.Minute)
		archivePeriodically("flserver", 1*time.Hour)
		archivePeriodically("country", 1*time.Hour)
		//archivePeriodically("host", 1*time.Hour)
		archivePeriodically("user", 24*time.Hour)
		archivePeriodically("destport", 1*time.Hour)
		archivePeriodically("answerercountry", 1*time.Hour)
		archivePeriodically("offereranswerercountries", 1*time.Hour)
		archivePeriodically("operatingsystem", 1*time.Hour)
	} else {
		log.Printf("%s was not \"true\", not archiving to BigQuery", ARCHIVE_TO_BIGQUERY)
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
	log.Printf("Archiving dim %s to BigQuery", dim)
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
