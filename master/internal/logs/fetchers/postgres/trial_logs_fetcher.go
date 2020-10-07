package postgres

import (
	"fmt"
	"strings"

	"github.com/determined-ai/determined/master/internal/db"
	"github.com/determined-ai/determined/master/internal/logs"
	"github.com/determined-ai/determined/master/pkg/model"
	"github.com/determined-ai/determined/proto/pkg/filters"
)

const (
	batchSize = 1000
)

// TrialLogsFetcher is a fetcher for postgres-backed trial logs.
type TrialLogsFetcher struct {
	db      *db.PgDB
	trialID int
	offset  int
	filters []*filters.Filter
}

// NewTrialLogsFetcher returns a new TrialLogsFetcher.
func NewTrialLogsFetcher(
	db *db.PgDB, trialID, offset int, fs []*filters.Filter,
) (*TrialLogsFetcher, error) {
	fetcher, err := validateTrialLogsFilters(fs)
	if err != nil {
		return fetcher, err
	}
	return &TrialLogsFetcher{
		db:      db,
		trialID: trialID,
		offset:  offset,
		filters: fs,
	}, nil
}

// validateTrialLogsFilters tries to construct filters using filterToSQL to ensure validation
// is one to one with functionality, which avoids validations allowing invalid filters and vice versa.
func validateTrialLogsFilters(fs []*filters.Filter) (*TrialLogsFetcher, error) {
	for _, f := range fs {
		if _, _, err := filterToSQL(f, trialLogsFieldTypes, 0); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// Fetch implements logs.Fetcher
func (p *TrialLogsFetcher) Fetch(limit int, unlimited bool) (logs.Batch, error) {
	switch {
	case unlimited || limit > batchSize:
		limit = batchSize
	case limit <= 0:
		return nil, nil
	}

	parameters := []interface{}{p.trialID, p.offset, limit}
	parameterID := len(parameters) + 1
	var queryFilters []string
	for _, f := range p.filters {
		fragment, param, err := filterToSQL(f, trialLogsFieldTypes, parameterID)
		if err != nil {
			return nil, err
		}
		queryFilters = append(queryFilters, fragment)
		parameters = append(parameters, param)
		parameterID += 1
	}
	query := fmt.Sprintf(`
SELECT
    l.id,
    l.trial_id,
    encode(l.message, 'escape') as message,
    l.agent_id,
    l.container_id,
    l.rank_id,
    l.timestamp,
    l.level,
    l.std_type,
    l.source
FROM trial_logs l
WHERE l.trial_id = $1
%s
ORDER BY l.id ASC OFFSET $2 LIMIT $3
`, strings.Join(queryFilters, "\n"))

	var b []*model.TrialLog
	err := p.db.QueryStr(query, &b, parameters...)

	if len(b) != 0 {
		p.offset = b[len(b)-1].ID
	}

	return model.TrialLogBatch(b), err
}

func trialLogsFieldTypes(f *filters.Filter) fieldType {
	switch f.Field {
	case "agent_id", "container_id", "level", "std_type", "source":
		return textFieldType
	case "rank_id":
		return integerFieldType
	case "timestamp":
		return timestampFieldType
	default:
		return unsupportedFieldType
	}
}