package db

import (
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/determined-ai/determined/master/pkg/model"
)

// CheckSearcherSnapshotExists checks if there is a searcher snapshot associated with
// the experiment.
func (db *PgDB) CheckSearcherSnapshotExists(experimentID int) (bool, error) {
	var exists bool
	err := db.sql.QueryRow(`
SELECT
EXISTS(
  select id
  FROM experiment_snapshots
  WHERE experiment_id = $1
)`, experimentID).Scan(&exists)
	return exists, err
}

// CheckTrialSnapshotExists checks if there is a snapshot associated with the trial.
func (db *PgDB) CheckTrialSnapshotExists(trialID int) (bool, error) {
	var exists bool
	err := db.sql.QueryRow(`
SELECT
EXISTS(
  select id
  FROM trial_snapshots
  WHERE trial_id = $1
)`, trialID).Scan(&exists)
	return exists, err
}

// ExperimentSnapshot returns the snapshot for the specified experiment.
func (db *PgDB) ExperimentSnapshot(experimentID int) ([]byte, error) {
	if b, err := db.rawQuery(`
SELECT content
FROM experiment_snapshots
WHERE experiment_id = $1
ORDER BY id DESC
LIMIT 1`, experimentID); errors.Cause(err) == ErrNotFound {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	} else {
		return b, nil
	}
}

// TrialSnapshot returns the snapshot for the specified trial.
func (db *PgDB) TrialSnapshot(requestID model.RequestID) ([]byte, error) {
	if b, err := db.rawQuery(`
SELECT content
FROM trial_snapshots
WHERE request_id = $1
ORDER BY id DESC
LIMIT 1`, requestID); err != nil {
		return nil, err
	} else {
		return b, nil
	}
}

// SaveSnapshot saves a searcher and trial snapshot together.
func (db *PgDB) SaveSnapshot(
	experimentID int, trialID int, requestID model.RequestID, searcher []byte, trial []byte,
) error {
	tx, err := db.sql.Begin()
	if err != nil {
		return errors.Wrap(err, "starting transaction")
	}
	defer func() {
		if tx == nil {
			return
		}

		if rErr := tx.Rollback(); rErr != nil {
			log.Errorf("during rollback: %v", rErr)
		}
	}()

	if _, err = tx.Exec(`
INSERT INTO experiment_snapshots (experiment_id, content)
VALUES ($1, $2)`, experimentID, searcher); err != nil {
		return errors.Wrap(err, "snapshotting searcher")
	}

	if _, err = tx.Exec(`
INSERT INTO trial_snapshots (trial_id, request_id, content)
VALUES ($1, $2, $3)`, trialID, requestID, trial); err != nil {
		return errors.Wrap(err, "failed to snapshot trial")
	}

	if err = tx.Commit(); err != nil {
		return errors.Wrapf(err, "committing snapshot of experiment %v", experimentID)
	}

	tx = nil
	return nil
}
