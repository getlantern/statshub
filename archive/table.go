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
	"fmt"
	"log"
	"sort"
	"time"

	bigquery "code.google.com/p/ox-google-api-go-client/bigquery/v2"

	shbq "github.com/getlantern/statshub/bigquery"
	"github.com/getlantern/statshub/statshub"
)

const (
	TIMESTAMP = "TIMESTAMP"
	RECORD    = "RECORD"
	INTEGER   = "INTEGER"
	STRING    = "STRING"
	global    = "global"
	counter   = "counter"
	gauge     = "gauge"
	_ts       = "_ts"
	_dim      = "_dim"

	ROWS_PER_INSERT = 1000
)

// StatsTable is a table that holds statistics from statshub
type StatsTable struct {
	service   *bigquery.Service
	tables    *bigquery.TablesService
	tabledata *bigquery.TabledataService
	dataset   *bigquery.Dataset
	table     *bigquery.Table
}

func NewStatsTable(projectId string, datasetId string, tableId string) (statsTable *StatsTable, err error) {
	statsTable = &StatsTable{
		table: &bigquery.Table{
			TableReference: &bigquery.TableReference{
				ProjectId: projectId,
				DatasetId: datasetId,
				TableId:   tableId,
			},
		},
	}
	if statsTable.service, err = shbq.Connect(); err != nil {
		return
	} else {
		statsTable.tables = bigquery.NewTablesService(statsTable.service)
		statsTable.tabledata = bigquery.NewTabledataService(statsTable.service)
		datasets := bigquery.NewDatasetsService(statsTable.service)
		statsTable.dataset, err = datasets.Get(projectId, datasetId).Do()
		return
	}
}

func (statsTable *StatsTable) WriteStats(dimStats map[string]*statshub.Stats, now time.Time) (err error) {
	if err = statsTable.createOrUpdateSchema(dimStats); err != nil {
		return
	}
	err = statsTable.insertRows(dimStats, now)
	return
}

func (statsTable *StatsTable) createOrUpdateSchema(dimStats map[string]*statshub.Stats) (err error) {
	var originalTable *bigquery.Table
	statsTable.table.Schema = schemaForStats(dimStats)
	if originalTable, err = statsTable.tables.Get(
		statsTable.table.TableReference.ProjectId,
		statsTable.table.TableReference.DatasetId,
		statsTable.table.TableReference.TableId,
	).Do(); err != nil {
		log.Printf("Creating table: %s", statsTable.table.TableReference.TableId)

		if statsTable.table, err = statsTable.tables.Insert(
			statsTable.table.TableReference.ProjectId,
			statsTable.table.TableReference.DatasetId,
			statsTable.table).Do(); err != nil {
			log.Printf("Error creating table: %s", err)
			return
		}
	} else {
		// TODO: the patch should only apply new columns, not remove old ones
		log.Printf("Patching table schema: %s", statsTable.table.TableReference.TableId)
		statsTable.mergeSchema(originalTable.Schema)

		if statsTable.table, err = statsTable.tables.Patch(
			statsTable.table.TableReference.ProjectId,
			statsTable.table.TableReference.DatasetId,
			statsTable.table.TableReference.TableId,
			statsTable.table).Do(); err != nil {
			log.Printf("Error patching table: %s", err)
			return
		}
	}

	return
}

func (statsTable *StatsTable) mergeSchema(schema *bigquery.TableSchema) {
	statsTable.table.Schema.Fields = consolidateFields(statsTable.table.Schema.Fields, schema.Fields)
}

func (statsTable *StatsTable) insertRows(dimStats map[string]*statshub.Stats, now time.Time) error {
	tableId := statsTable.table.TableReference.TableId
	doInsert := func(rows []*bigquery.TableDataInsertAllRequestRows) error {
		insertRequest := &bigquery.TableDataInsertAllRequest{Rows: rows}
		resp, err := statsTable.tabledata.InsertAll(
			statsTable.table.TableReference.ProjectId,
			statsTable.table.TableReference.DatasetId,
			tableId,
			insertRequest).Do()
		if err != nil {
			log.Printf("Unable to insert into %s: %s", tableId, err)
		} else if len(resp.InsertErrors) > 0 {
			for _, ie := range resp.InsertErrors {
				log.Printf("Insert error inserting into %s: %s", tableId, ie.Errors)

			}
		} else {
			log.Printf("Inserted %d rows into: %s", len(rows), tableId)
		}
		return nil
	}

	rows := make([]*bigquery.TableDataInsertAllRequestRows, ROWS_PER_INSERT)
	i := 0

	// Set up
	for dim, stats := range dimStats {
		// Rows are identified by a unique InsertId to prevent duplicates for any given dim + ts
		rows[i] = &bigquery.TableDataInsertAllRequestRows{
			InsertId: fmt.Sprintf("%s|%d", dim, now.Unix()),
			Json:     rowFromStats(dim, stats, now),
		}
		log.Println(rows[i].Json)
		i++
		if i == ROWS_PER_INSERT {
			// To deal with rate limiting, insert every 1000 rows
			if err := doInsert(rows); err != nil {
				return err
			}
			i = 0
		}
	}

	if i != 0 {
		// Insert the remaining rows
		return doInsert(rows[0:i])
	} else {
		return nil
	}
}

func schemaForStats(dimStats map[string]*statshub.Stats) *bigquery.TableSchema {
	fields := make([]*bigquery.TableFieldSchema, 2)
	fields[0] = &bigquery.TableFieldSchema{
		Type: STRING,
		Name: _dim,
	}
	fields[1] = &bigquery.TableFieldSchema{
		Type: TIMESTAMP,
		Name: _ts,
	}
	// Build fields based on stats for total
	dimFields := fieldsForStats(dimStats["total"])
	for _, dimField := range dimFields {
		fields = append(fields, dimField)
	}
	return &bigquery.TableSchema{
		Fields: fields,
	}
}

func fieldsForStats(stats *statshub.Stats) (fields []*bigquery.TableFieldSchema) {
	fields = make([]*bigquery.TableFieldSchema, 0)
	if len(stats.Counters) > 0 {
		fields = append(fields, &bigquery.TableFieldSchema{
			Type:   RECORD,
			Name:   counter,
			Fields: fieldsFor(stats.Counters),
		})
	}
	if len(stats.Gauges) > 0 {
		fields = append(fields, &bigquery.TableFieldSchema{
			Type:   RECORD,
			Name:   gauge,
			Fields: fieldsFor(stats.Gauges),
		})
	}
	return
}

func fieldsFor(m map[string]int64) (fields []*bigquery.TableFieldSchema) {
	keys := make([]string, len(m))
	i := 0
	for key, _ := range m {
		keys[i] = key
		i++
	}
	// Sort keys alphabetically
	sort.Strings(keys)
	fields = make([]*bigquery.TableFieldSchema, len(keys))
	for i, key := range keys {
		fields[i] = &bigquery.TableFieldSchema{
			Type: INTEGER,
			Name: key,
		}
	}
	return
}

// consolidateFields consolidates two lists of TableFieldSchemas into a single list
func consolidateFields(a []*bigquery.TableFieldSchema, b []*bigquery.TableFieldSchema) (consolidated []*bigquery.TableFieldSchema) {
	allFields := make(map[string]*bigquery.TableFieldSchema)

	for _, field := range a {
		allFields[field.Name] = field
	}
	for _, field := range b {
		matching, found := allFields[field.Name]
		if found {
			if matching.Type == RECORD {
				// For RECORD fields, consolidate their lists of fields
				matching.Fields = consolidateFields(field.Fields, matching.Fields)
			}
		} else {
			// No matching field found, add field
			allFields[field.Name] = field
		}
	}

	keys := make([]string, len(allFields))
	i := 0
	for key, _ := range allFields {
		keys[i] = key
		i++
	}

	// Sort keys alphabetically
	sort.Strings(keys)
	consolidated = make([]*bigquery.TableFieldSchema, len(keys))
	for i, key := range keys {
		consolidated[i] = allFields[key]
	}

	return
}

func rowFromStats(dim string, stats *statshub.Stats, now time.Time) (row map[string]interface{}) {
	row = statsAsMap(stats)
	row[_ts] = now.Unix()
	row[_dim] = dim
	return
}

func statsAsMap(stats *statshub.Stats) (m map[string]interface{}) {
	m = make(map[string]interface{})
	m[counter] = stats.Counters
	m[gauge] = stats.Gauges
	return
}
