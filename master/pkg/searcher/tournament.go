package searcher

import (
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/determined-ai/determined/master/pkg/model"
	"github.com/determined-ai/determined/master/pkg/workload"
)

// tournamentSearch runs multiple search methods in tandem. Callbacks for completed operations
// are sent to the originating search method that created the corresponding operation.
type (
	tournamentSearchState struct {
		subSearchUnitsCompleted map[SearchMethod]float64
		trialTable              map[model.RequestID]SearchMethod
		subSearchStates         [][]byte
	}
	tournamentSearch struct {
		subSearches []SearchMethod
		tournamentSearchState
	}
)

func newTournamentSearch(subSearches ...SearchMethod) *tournamentSearch {
	return &tournamentSearch{
		subSearches: subSearches,
		tournamentSearchState: tournamentSearchState{
			subSearchUnitsCompleted: make(map[SearchMethod]float64),
			trialTable:              make(map[model.RequestID]SearchMethod),
			subSearchStates:         make([][]byte, len(subSearches)),
		},
	}
}

func (s *tournamentSearch) save() ([]byte, error) {
	for i := range s.subSearches {
		b, err := s.subSearches[i].save()
		if err != nil {
			return nil, errors.Wrap(err, "failed to save subsearch")
		}
		s.subSearchStates[i] = b
	}
	return json.Marshal(s.tournamentSearchState)
}

func (s *tournamentSearch) load(state []byte) error {
	if state == nil {
		return nil
	}
	err := json.Unmarshal(state, &s.tournamentSearchState)
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal tournament state")
	}
	for i := range s.subSearches {
		if err := s.subSearches[i].load(s.subSearchStates[i]); err != nil {
			return errors.Wrap(err, "failed to load subsearch")
		}
	}
	return nil
}

func (s *tournamentSearch) initialOperations(ctx context) ([]Operation, error) {
	var operations []Operation
	for _, subSearch := range s.subSearches {
		ops, err := subSearch.initialOperations(ctx)
		if err != nil {
			return nil, err
		}
		s.markCreates(subSearch, ops)
		operations = append(operations, ops...)
	}
	return operations, nil
}

func (s *tournamentSearch) trialCreated(ctx context, requestID model.RequestID) ([]Operation, error) {
	subSearch := s.trialTable[requestID]
	ops, err := subSearch.trialCreated(ctx, requestID)
	return s.markCreates(subSearch, ops), err
}

func (s *tournamentSearch) trainCompleted(
	ctx context, requestID model.RequestID, train Train,
) ([]Operation, error) {
	subSearch := s.trialTable[requestID]
	s.subSearchUnitsCompleted[subSearch] += float64(train.Length.Units)
	ops, err := subSearch.trainCompleted(ctx, requestID, train)
	return s.markCreates(subSearch, ops), err
}

func (s *tournamentSearch) checkpointCompleted(
	ctx context, requestID model.RequestID, checkpoint Checkpoint, metrics workload.CheckpointMetrics,
) ([]Operation, error) {
	subSearch := s.trialTable[requestID]
	ops, err := subSearch.checkpointCompleted(ctx, requestID, checkpoint, metrics)
	return s.markCreates(subSearch, ops), err
}

func (s *tournamentSearch) validationCompleted(
	ctx context, requestID model.RequestID, validate Validate, metrics workload.ValidationMetrics,
) ([]Operation, error) {
	subSearch := s.trialTable[requestID]
	ops, err := subSearch.validationCompleted(ctx, requestID, validate, metrics)
	return s.markCreates(subSearch, ops), err
}

// trialClosed informs the searcher that the trial has been closed as a result of a Close operation.
func (s *tournamentSearch) trialClosed(ctx context, requestID model.RequestID) ([]Operation, error) {
	subSearch := s.trialTable[requestID]
	ops, err := subSearch.trialClosed(ctx, requestID)
	return s.markCreates(subSearch, ops), err
}

func (s *tournamentSearch) trialExitedEarly(
	ctx context, requestID model.RequestID, exitedReason workload.ExitedReason,
) ([]Operation, error) {
	subSearch := s.trialTable[requestID]
	ops, err := subSearch.trialExitedEarly(ctx, requestID, exitedReason)
	return s.markCreates(subSearch, ops), err
}

// progress returns experiment progress as a float between 0.0 and 1.0.
func (s *tournamentSearch) progress(float64) float64 {
	sum := 0.0
	for _, subSearch := range s.subSearches {
		sum += subSearch.progress(s.subSearchUnitsCompleted[subSearch])
	}
	return sum / float64(len(s.subSearches))
}

func (s *tournamentSearch) Unit() model.Unit {
	return s.subSearches[0].Unit()
}

func (s *tournamentSearch) markCreates(subSearch SearchMethod, operations []Operation) []Operation {
	for _, operation := range operations {
		switch operation := operation.(type) {
		case Create:
			s.trialTable[operation.RequestID] = subSearch
		}
	}
	return operations
}
