package archive

import (
	// Note - I'm using a patched version of the google-api-go-client library
	// because of this bug -
	// https://code.google.com/p/google-api-go-client/issues/detail?id=52
	bigquery "code.google.com/p/ox-google-api-go-client/bigquery/v2"
	"github.com/getlantern/statshub/statshub"
	"github.com/oxtoacart/oauther/oauth"
	"log"
	"os"
	"sort"
	"time"
)

const (
	OAUTH_CONFIG = "OAUTH_CONFIG"

	TIMESTAMP = "TIMESTAMP"
	RECORD    = "RECORD"
	INTEGER   = "INTEGER"
	global    = "global"
	counter   = "counter"
	gauge     = "gauge"
	ts        = "_ts"
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
	var oauther *oauth.OAuther
	if oauther, err = oauth.FromJSON([]byte(os.Getenv(OAUTH_CONFIG))); err != nil {
		return
	} else if statsTable.service, err = bigquery.New(oauther.Transport().Client()); err != nil {
		return
	} else {
		statsTable.tables = bigquery.NewTablesService(statsTable.service)
		statsTable.tabledata = bigquery.NewTabledataService(statsTable.service)
		datasets := bigquery.NewDatasetsService(statsTable.service)
		statsTable.dataset, err = datasets.Get(projectId, datasetId).Do()
		return
	}
}

func (statsTable *StatsTable) WriteStats(resp *statshub.QueryResponse, now time.Time) (err error) {
	if err = statsTable.createOrUpdateSchema(resp); err != nil {
		return
	}
	insertRequest := &bigquery.TableDataInsertAllRequest{
		Rows: []*bigquery.TableDataInsertAllRequestRows{
			&bigquery.TableDataInsertAllRequestRows{
				Json: rowFromStats(resp, now),
			},
		},
	}
	_, err = statsTable.tabledata.InsertAll(
		statsTable.table.TableReference.ProjectId,
		statsTable.table.TableReference.DatasetId,
		statsTable.table.TableReference.TableId,
		insertRequest).Do()
	if err == nil {
		log.Printf("Inserted new row into: %s", statsTable.table.TableReference.TableId)
	}
	return
}

func (statsTable *StatsTable) createOrUpdateSchema(resp *statshub.QueryResponse) (err error) {
	var originalTable *bigquery.Table
	statsTable.table.Schema = schemaForStats(resp)
	if originalTable, err = statsTable.tables.Get(projectId, datasetId, tableId).Do(); err != nil {
		log.Printf("Creating table: %s", tableId)

		if statsTable.table, err = statsTable.tables.Insert(
			statsTable.table.TableReference.ProjectId,
			statsTable.table.TableReference.DatasetId,
			statsTable.table).Do(); err != nil {
			log.Printf("Error creating table: %s", err)
			return
		}
	} else {
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

func schemaForStats(resp *statshub.QueryResponse) (schema *bigquery.TableSchema) {
	fields := make([]*bigquery.TableFieldSchema, 1)
	fields[0] = &bigquery.TableFieldSchema{
		Type: TIMESTAMP,
		Name: ts,
	}
	globalFields := fieldsForStats(resp.Global)
	if len(globalFields) > 0 {
		fields = append(fields, &bigquery.TableFieldSchema{
			Type:   RECORD,
			Name:   global,
			Fields: globalFields,
		})
	}
	countryCodes := make([]string, len(resp.PerCountry))
	if len(countryCodes) > 0 {
		i := 0
		for key, _ := range resp.PerCountry {
			countryCodes[i] = key
			i++
		}
		// Sort country codes alphabetically
		sort.Strings(countryCodes)
		for _, countryCode := range countryCodes {
			countryFields := fieldsForStats(resp.PerCountry[countryCode])
			if len(countryFields) > 0 {
				fields = append(fields, &bigquery.TableFieldSchema{
					Type:   RECORD,
					Name:   countryCode,
					Fields: countryFields,
				})
			}
		}
	}

	return &bigquery.TableSchema{
		Fields: fields,
	}
}

func fieldsForStats(stats *statshub.Stats) (fields []*bigquery.TableFieldSchema) {
	fields = make([]*bigquery.TableFieldSchema, 0)
	if len(stats.Counter) > 0 {
		fields = append(fields, &bigquery.TableFieldSchema{
			Type:   RECORD,
			Name:   counter,
			Fields: fieldsFor(stats.Counter),
		})
	}
	if len(stats.Gauge) > 0 {
		fields = append(fields, &bigquery.TableFieldSchema{
			Type:   RECORD,
			Name:   gauge,
			Fields: fieldsFor(stats.Gauge),
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

func rowFromStats(resp *statshub.QueryResponse, now time.Time) (row map[string]interface{}) {
	row = make(map[string]interface{})
	row[ts] = now.Unix()
	row[global] = statsAsMap(resp.Global)
	for countryCode, stats := range resp.PerCountry {
		row[countryCode] = statsAsMap(stats)
	}
	return
}

func statsAsMap(stats *statshub.Stats) (m map[string]map[string]int64) {
	m = make(map[string]map[string]int64)
	m[counter] = stats.Counter
	m[gauge] = stats.Gauge
	return
}
