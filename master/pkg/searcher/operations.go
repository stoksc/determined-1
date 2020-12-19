package searcher

import (
	"fmt"

	"github.com/determined-ai/determined/master/pkg/model"
	"github.com/determined-ai/determined/master/pkg/nprand"
)

// Operation represents the base interface for possible operations that a search method can return.
type Operation interface{}

// Requested is a convenience interface for operations that were requested by a searcher method
// for a specific trial.
type Requested interface {
	GetRequestID() model.RequestID
}

// Runnable represents any runnable operation. It acts as a sum type for Train, Validate,
// Checkpoints and any future operations that the harness may run.
type Runnable interface {
	Requested
	Runnable()
}

// Create a new trial for the search method.
type Create struct {
	RequestID model.RequestID `json:"request_id"`
	// TrialSeed must be a value between 0 and 2**31 - 1.
	TrialSeed             uint32                      `json:"trial_seed"`
	Hparams               hparamSample                `json:"hparams"`
	Checkpoint            *Checkpoint                 `json:"checkpoint"`
	WorkloadSequencerType model.WorkloadSequencerType `json:"workload_sequencer_type"`
}

// NewCreate initializes a new Create operation with a new request ID and the given hyperparameters.
func NewCreate(
	rand *nprand.State, s hparamSample, sequencerType model.WorkloadSequencerType) Create {
	return Create{
		RequestID:             model.NewRequestID(rand),
		TrialSeed:             uint32(rand.Int64n(1 << 31)),
		Hparams:               s,
		WorkloadSequencerType: sequencerType,
	}
}

// NewCreateFromCheckpoint initializes a new Create operation with a new request ID and the given
// hyperparameters and checkpoint to initially load from.
func NewCreateFromCheckpoint(
	rand *nprand.State, s hparamSample, checkpoint Checkpoint,
	sequencerType model.WorkloadSequencerType,
) Create {
	create := NewCreate(rand, s, sequencerType)
	create.Checkpoint = &checkpoint
	return create
}

func (create Create) String() string {
	if create.Checkpoint == nil {
		return fmt.Sprintf("{Create %s, seed %d}", create.RequestID, create.TrialSeed)
	}
	return fmt.Sprintf(
		"{Create %s, seed %d, checkpoint %v}", create.RequestID, create.TrialSeed, create.Checkpoint,
	)
}

// GetRequestID implemented Requested.
func (create Create) GetRequestID() model.RequestID { return create.RequestID }

// Train is an operation emitted by search methods to signal the trial train for a specified length.
type Train struct {
	RequestID model.RequestID
	Length    model.Length
}

// NewTrain returns a new train operation.
func NewTrain(requestID model.RequestID, length model.Length) Train {
	return Train{requestID, length}
}

func (t Train) String() string {
	return fmt.Sprintf("{Train %s, %s}", t.RequestID, t.Length)
}

// Runnable implements Runnable.
func (t Train) Runnable() {}

// GetRequestID implemented Requested.
func (t Train) GetRequestID() model.RequestID { return t.RequestID }

// Validate is an operation emitted by search methods to signal the trial to validate.
type Validate struct {
	RequestID model.RequestID
}

// NewValidate returns a new validate operation.
func NewValidate(requestID model.RequestID) Validate {
	return Validate{requestID}
}

func (v Validate) String() string {
	return fmt.Sprintf("{Validate %s}", v.RequestID)
}

// Runnable implements Runnable.
func (v Validate) Runnable() {}

// GetRequestID implemented Requested.
func (v Validate) GetRequestID() model.RequestID { return v.RequestID }

// Checkpoint is an operation emitted by search methods to signal the trial to checkpoint.
type Checkpoint struct {
	RequestID model.RequestID
}

// NewCheckpoint returns a new checkpoint operation.
func NewCheckpoint(requestID model.RequestID) Checkpoint {
	return Checkpoint{requestID}
}

func (c Checkpoint) String() string {
	return fmt.Sprintf("{Checkpoint %s}", c.RequestID)
}

// Runnable implements Runnable.
func (c Checkpoint) Runnable() {}

// GetRequestID implemented Requested.
func (c Checkpoint) GetRequestID() model.RequestID { return c.RequestID }

// Close the trial with the given trial id.
type Close struct {
	RequestID model.RequestID `json:"request_id"`
}

// NewClose initializes a new Close operation for the request ID.
func NewClose(requestID model.RequestID) Close {
	return Close{
		RequestID: requestID,
	}
}

func (close Close) String() string {
	return fmt.Sprintf("{Close %s}", close.RequestID)
}

// GetRequestID implemented Requested.
func (close Close) GetRequestID() model.RequestID { return close.RequestID }

// Shutdown marks the searcher as completed.
type Shutdown struct {
	Failure bool
}

// NewShutdown initializes a Shutdown operation for the searcher.
func NewShutdown() Shutdown {
	return Shutdown{}
}

func (shutdown Shutdown) String() string {
	return "{Shutdown}"
}
