package db

import (
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// SaveSnapshot saves a searcher and trial snapshot together.
func (db *PgDB) SaveSnapshot(
	experimentID int, trialID int, searcher []byte, trial []byte,
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
INSERT INTO searcher_snapshots (experiment_id, snapshot)
VALUES ($1, $2)`, experimentID, searcher); err != nil {
		return errors.Wrap(err, "snapshotting searcher")
	}

	if _, err = tx.Exec(`
INSERT INTO trial_snapshots (trial_id, snapshot)
VALUES ($1, $2)`, experimentID, searcher); err != nil {
		return errors.Wrap(err, "snapshotting trial")
	}

	if err = tx.Commit(); err != nil {
		return errors.Wrapf(err, "committing snapshot of experiment %v", experimentID)
	}

	tx = nil
	return nil
}
