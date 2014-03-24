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
//

package statshub

import (
	"strings"
	"testing"
	"time"
)

// TestUpdateAndQuery tests updating and querying the statshub
// This should be run with a REDIS_ADDR and REDIS_PASS corresponding
// to our testing Redis database
func TestUpdateAndQuery(t *testing.T) {
	// Make our statsPeriod short to facilitate faster testing
	reportingPeriod = 1800 * time.Millisecond
	statsPeriod = 2000 * time.Millisecond

	// sleepTillNextBucket sleeps until the start of the next Gauge statsPeriod
	sleepTillNextBucket := func() {
		// Sleep to allow gauges to roll into next bucket
		nextStatsPeriod := time.Now().Truncate(statsPeriod).Add(statsPeriod)
		sleepAmount := nextStatsPeriod.Sub(time.Now())
		time.Sleep(sleepAmount)
	}

	conn, err := connectToRedis()
	if err != nil {
		t.Fatalf("Unable to connect to redis: %s", err)
	}
	defer conn.Close()

	// Clear out the test database before starting
	_, err = conn.Do("FLUSHDB")
	if err != nil {
		t.Fatalf("Unable to flush db: %s", err)
	}

	// Try an update that includes dimension key "total", which shouldn't be allowed
	update := &StatsUpdate{
		Dims: map[string]string{
			"country": "total",
		},
		Stats: Stats{
			Counters: map[string]int64{
				"counterA": 10,
			},
		},
	}

	err = update.write("myid1")
	if err == nil {
		t.Fatalf("Attempting to post a stat with a dimension key of 'total' should not have been allowed")
	}

	update = &StatsUpdate{
		Dims: map[string]string{
			"country": "es",
			"user":    "bob",
		},
		Stats: Stats{
			Counters: map[string]int64{
				"counterA": 50,
			},
			Increments: map[string]int64{
				"counterB": 500,
			},
			Gauges: map[string]int64{
				"gaugeA":  5000,
				"gaugeAA": 0,
			},
			Members: map[string]string{
				"gaugeB": "item1",
			},
		},
	}

	writeStats := func(id string) {
		if err = update.write(id); err != nil {
			t.Fatalf("Unable to post to redis: %s", err)
		}
	}

	sleepTillNextBucket()
	writeStats("myid1")
	sleepTillNextBucket()
	statsByDim, err := QueryDims([]string{"country", "user"})
	if err != nil {
		t.Fatalf("Unable to query: %s", err)
	}

	// A counter updated directly should reflect the specified value
	assertCounterEquals(t, statsByDim, "country:es:counterA", 50)
	// An unitialized counter that is incremented should reflect the increment
	assertCounterEquals(t, statsByDim, "country:es:counterB", 500)
	// A gauge should reflect the value reported in the prior reporting period
	assertGaugeEquals(t, statsByDim, "country:es:gaugeA", 5000)
	// A 2nd gauge should also be set correctly (this tests to make sure we don't have problems interleaving SET and EXPIRE calls)
	assertGaugeEquals(t, statsByDim, "country:es:gaugeAA", 0)
	// A gauge from a membership update should reflect the count of unique members
	assertGaugeEquals(t, statsByDim, "country:es:gaugeB", 1)
	// The same things hold true for the rollups to the user dimension
	assertCounterEquals(t, statsByDim, "user:bob:counterA", 50)
	assertCounterEquals(t, statsByDim, "user:bob:counterB", 500)
	assertGaugeEquals(t, statsByDim, "user:bob:gaugeA", 5000)
	assertGaugeEquals(t, statsByDim, "user:bob:gaugeB", 1)

	// Totals should match individual values at this point
	assertCounterEquals(t, statsByDim, "country:total:counterA", 50)
	assertCounterEquals(t, statsByDim, "country:total:counterB", 500)
	assertGaugeEquals(t, statsByDim, "country:total:gaugeA", 5000)
	assertGaugeEquals(t, statsByDim, "country:total:gaugeAA", 0)
	assertGaugeEquals(t, statsByDim, "country:total:gaugeB", 1)
	assertCounterEquals(t, statsByDim, "user:total:counterA", 50)
	assertCounterEquals(t, statsByDim, "user:total:counterB", 500)
	assertGaugeEquals(t, statsByDim, "user:total:gaugeA", 5000)
	assertGaugeEquals(t, statsByDim, "user:total:gaugeB", 1)

	update = &StatsUpdate{
		Dims: map[string]string{
			"country": "es",
			"user":    "bob",
		},
		Stats: Stats{
			Counters: map[string]int64{
				"counterA": 60,
			},
			Increments: map[string]int64{
				"counterB": 600,
			},
			Gauges: map[string]int64{
				"gaugeA":  0,
				"gaugeAA": 50000,
			},
			Members: map[string]string{
				"gaugeB": "item2",
			},
		},
	}

	sleepTillNextBucket()
	writeStats("myid1")
	sleepTillNextBucket()
	statsByDim, err = QueryDims([]string{"country", "user"})
	if err != nil {
		t.Fatalf("Unable to query: %s", err)
	}

	// Updating a counter should result in the value being replaced
	assertCounterEquals(t, statsByDim, "country:es:counterA", 60)
	// Incrementing a counter should result in the value being incremented
	assertCounterEquals(t, statsByDim, "country:es:counterB", 1100)
	// Updating a gauge should result in the value being replaced
	assertGaugeEquals(t, statsByDim, "country:es:gaugeA", 0)
	assertGaugeEquals(t, statsByDim, "country:es:gaugeAA", 50000)
	// Adding a new unique member should be reflected in the corresonding gauge
	assertGaugeEquals(t, statsByDim, "country:es:gaugeB", 2)
	assertCounterEquals(t, statsByDim, "user:bob:counterA", 60)
	assertCounterEquals(t, statsByDim, "user:bob:counterB", 1100)
	assertGaugeEquals(t, statsByDim, "user:bob:gaugeA", 0)
	assertGaugeEquals(t, statsByDim, "user:bob:gaugeB", 2)

	// Totals should still match individual values at this point
	assertCounterEquals(t, statsByDim, "country:total:counterA", 60)
	assertCounterEquals(t, statsByDim, "country:total:counterB", 1100)
	assertGaugeEquals(t, statsByDim, "country:total:gaugeA", 0)
	assertGaugeEquals(t, statsByDim, "country:total:gaugeAA", 50000)
	assertGaugeEquals(t, statsByDim, "country:total:gaugeB", 2)
	assertCounterEquals(t, statsByDim, "user:total:counterA", 60)
	assertCounterEquals(t, statsByDim, "user:total:counterB", 1100)
	assertGaugeEquals(t, statsByDim, "user:total:gaugeA", 0)
	assertGaugeEquals(t, statsByDim, "user:total:gaugeB", 2)

	update = &StatsUpdate{
		Dims: map[string]string{
			"country": "es",
			"user":    "bob",
		},
		Stats: Stats{
			Members: map[string]string{
				"gaugeB": "item2",
			},
		},
	}

	sleepTillNextBucket()
	writeStats("myid1")
	sleepTillNextBucket()
	statsByDim, err = QueryDims([]string{"country", "user"})
	if err != nil {
		t.Fatalf("Unable to query: %s", err)
	}

	// Adding a member about which we already knew should not increase the gauge's value
	assertGaugeEquals(t, statsByDim, "country:es:gaugeB", 2)
	assertGaugeEquals(t, statsByDim, "user:bob:gaugeB", 2)

	sleepTillNextBucket()
	// Post our spanish gauges again to keep them from rolling over in the next period
	update = &StatsUpdate{
		Dims: map[string]string{
			"country": "es",
			"user":    "bob",
		},
		Stats: Stats{
			Gauges: map[string]int64{
				"gaugeA":  0,
				"gaugeAA": 50000,
			},
		},
	}
	writeStats("myid1")

	// Post to a different id, in a different country
	update = &StatsUpdate{
		Dims: map[string]string{
			"country": "de",
			"user":    "bob",
		},
		Stats: Stats{
			Counters: map[string]int64{
				"counterA": 70,
			},
			Increments: map[string]int64{
				"counterB": 700,
			},
			Gauges: map[string]int64{
				"gaugeA":  7000,
				"gaugeAA": 70000,
			},
			Members: map[string]string{
				"gaugeB": "item3",
			},
		},
	}
	writeStats("myid2")
	sleepTillNextBucket()
	statsByDim, err = QueryDims([]string{"country", "user"})
	if err != nil {
		t.Fatalf("Unable to query: %s", err)
	}

	// The counters and gauges from the other id, in Spain, should be unaffected
	assertCounterEquals(t, statsByDim, "country:es:counterA", 60)
	assertCounterEquals(t, statsByDim, "country:es:counterB", 1100)
	assertGaugeEquals(t, statsByDim, "country:es:gaugeA", 0)
	assertGaugeEquals(t, statsByDim, "country:es:gaugeAA", 50000)
	assertGaugeEquals(t, statsByDim, "country:es:gaugeB", 2)
	// The new country should now reflect its counters and gauges
	assertCounterEquals(t, statsByDim, "country:de:counterA", 70)
	assertCounterEquals(t, statsByDim, "country:de:counterB", 700)
	assertGaugeEquals(t, statsByDim, "country:de:gaugeA", 7000)
	assertGaugeEquals(t, statsByDim, "country:de:gaugeAA", 70000)
	assertGaugeEquals(t, statsByDim, "country:de:gaugeB", 1)
	// The user's counters and gauges should reflect the cumulative values from both id (myid1 and myid2)
	// In this case that also happens to be the total across both countries
	assertCounterEquals(t, statsByDim, "user:bob:counterA", 130)
	assertCounterEquals(t, statsByDim, "user:bob:counterB", 1800)
	assertGaugeEquals(t, statsByDim, "user:bob:gaugeA", 7000)
	assertGaugeEquals(t, statsByDim, "user:bob:gaugeB", 3)

	// Totals should now inclue all dimensions
	assertCounterEquals(t, statsByDim, "country:total:counterA", 130)
	assertCounterEquals(t, statsByDim, "country:total:counterB", 1800)
	assertGaugeEquals(t, statsByDim, "country:total:gaugeA", 7000)
	assertGaugeEquals(t, statsByDim, "country:total:gaugeAA", 120000)
	assertGaugeEquals(t, statsByDim, "country:total:gaugeB", 3)
	assertCounterEquals(t, statsByDim, "user:total:counterA", 130)
	assertCounterEquals(t, statsByDim, "user:total:counterB", 1800)
	assertGaugeEquals(t, statsByDim, "user:total:gaugeA", 7000)
	assertGaugeEquals(t, statsByDim, "user:total:gaugeAA", 120000)
	assertGaugeEquals(t, statsByDim, "user:total:gaugeB", 3)

	sleepTillNextBucket()
	statsByDim, err = QueryDims([]string{"country", "user"})
	if err != nil {
		t.Fatalf("Unable to query: %s", err)
	}

	// Manually set gauges should expire when the statsPeriod bucket rolls over
	assertGaugeEquals(t, statsByDim, "country:es:gaugeA", 0)
	// Membership gauges should never expire
	assertGaugeEquals(t, statsByDim, "country:es:gaugeB", 2)
	assertGaugeEquals(t, statsByDim, "country:de:gaugeA", 0)
	assertGaugeEquals(t, statsByDim, "country:de:gaugeB", 1)
	assertGaugeEquals(t, statsByDim, "user:bob:gaugeA", 0)
	assertGaugeEquals(t, statsByDim, "user:bob:gaugeB", 3)
}

func assertCounterEquals(
	t *testing.T,
	statsByDim map[string]map[string]*Stats,
	path string,
	expected int64,
) {
	pe := strings.Split(path, ":")
	actual := statsByDim[pe[0]][pe[1]].Counters[pe[2]]
	if actual != expected {
		t.Errorf("Counter %s wrong.  Expected %d, got %d", path, expected, actual)
	}
}

func assertGaugeEquals(
	t *testing.T,
	statsByDim map[string]map[string]*Stats,
	path string,
	expected int64,
) {
	pe := strings.Split(path, ":")
	actual := statsByDim[pe[0]][pe[1]].Gauges[pe[2]]
	if actual != expected {
		t.Errorf("Gauge %s wrong.  Expected %d, got %d", path, expected, actual)
	}
}
