package bigquery

import (
	"github.com/getlantern/statshub/statshub"
	"testing"
)

func TestSchema(t *testing.T) {
	statsA := map[string]*statshub.Stats{
		"total": &statshub.Stats{
			Counters: map[string]int64{
				"Aca": 5,
				"Acb": 5,
			},
			Gauges: map[string]int64{
				"Aga": 5,
				"Agb": 5,
			},
		},
	}
	statsB := map[string]*statshub.Stats{
		"total": &statshub.Stats{
			Counters: map[string]int64{
				"Bca": 5,
				"Bcb": 5,
			},
			Gauges: map[string]int64{
				"Bga": 5,
				"Bgb": 5,
			},
		},
	}
	schemaA := schemaForStats(statsA)
	schemaB := schemaForStats(statsB)
	consolidatedFields := consolidateFields(schemaA.Fields, schemaB.Fields)
	if len(consolidatedFields) != 4 {
		t.Errorf("Should have 4 consolidated fields, only got %d", len(consolidatedFields))
	}
	consolidatedCounters := consolidatedFields[2].Fields
	consolidatedGauges := consolidatedFields[3].Fields
	if len(consolidatedCounters) != 4 {
		t.Errorf("Should have 4 consolidated counters, only got %d", len(consolidatedCounters))
	}
	if len(consolidatedGauges) != 4 {
		t.Errorf("Should have 4 consolidated gauges, only got %d", len(consolidatedGauges))
	}
}
