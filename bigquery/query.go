package bigquery

import (
	bigquery "code.google.com/p/ox-google-api-go-client/bigquery/v2"
)

// Query runs the given queryString and returns the result rows
func Query(queryString string, maxResults int64) (rows [][]interface{}, err error) {
	var service *bigquery.Service
	service, err = Connect()
	if err != nil {
		return
	}

	req := &bigquery.QueryRequest{
		DefaultDataset: &bigquery.DatasetReference{
			DatasetId: DATASET_ID,
			ProjectId: ProjectId,
		},
		Query:      queryString,
		Kind:       "json",
		MaxResults: maxResults,
	}
	jobsService := bigquery.NewJobsService(service)
	var resp *bigquery.QueryResponse
	resp, err = jobsService.Query(ProjectId, req).Do()
	if err != nil {
		return
	}

	numRows := int(resp.TotalRows)
	if numRows > int(maxResults) {
		numRows = int(maxResults)
	}
	rows = make([][]interface{}, numRows)
	for r := 0; r < int(numRows); r++ {
		numColumns := len(resp.Schema.Fields)
		dataRow := resp.Rows[r]
		row := make([]interface{}, numColumns)
		for c := 0; c < numColumns; c++ {
			row[c] = dataRow.F[c].V
		}
		rows[r] = row
	}
	return
}
