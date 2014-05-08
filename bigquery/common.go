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

package bigquery

import (
	"os"

	// Note - I'm using a patched version of the google-api-go-client library
	// because of this bug -
	// https://code.google.com/p/google-api-go-client/issues/detail?id=52
	bigquery "code.google.com/p/ox-google-api-go-client/bigquery/v2"

	"github.com/oxtoacart/oauther/oauth"
)

const (
	GOOGLE_PROJECT = "GOOGLE_PROJECT"
	OAUTH_CONFIG   = "OAUTH_CONFIG"
	DATASET_ID     = "statshub"
)

var (
	ProjectId = os.Getenv(GOOGLE_PROJECT)
)

// Connect authenticates using OAuth and returns a bigquery.Service which can be
// used by the bigquery APIs.
func Connect() (service *bigquery.Service, err error) {
	var oauther *oauth.OAuther
	oauther, err = oauth.FromJSON([]byte(os.Getenv(OAUTH_CONFIG)))
	if err != nil {
		return
	}
	service, err = bigquery.New(oauther.Transport().Client())
	return
}
