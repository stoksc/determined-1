package searcher

import (
	"fmt"
	"math"

	"github.com/determined-ai/determined/master/pkg/model"
)

// OperationPlanner performs a one-to-many mapping of search method operations to workload
// operations and the reverse mapping to turn completed workloads back into their corresponding
// search method operation to inform the search method about. Thie purpose is so the tbe search
// method agonostic of scheduling and fault-tolerance concerns.
type OperationPlanner struct {
	// configuration
	unit                 model.Unit
	targetBatchesPerStep int
	recordsPerEpoch      int
	minValidationPeriod  model.Length
	minCheckpointPeriod  model.Length

	// trial state
	trialGlobalBatchSizes map[RequestID]int
	stepCounts            map[RequestID]int
	trialOpsToPlannedOps  map[RequestID][]opToPlannedOps
	unitsSinceValidation  map[RequestID]model.Length
	unitsSinceCheckpoint  map[RequestID]model.Length

	// searcher state
	totalUnitsCompleted model.Length
}

type opToPlannedOps struct {
	op         Operation
	plannedOps []Operation
}

// NewOperationPlanner creates an new operation planner with the  given configurations.
func NewOperationPlanner(
	batchesPerStep, recordsPerEpoch int,
	minValidationPeriod, minCheckpointPeriod model.Length,
) OperationPlanner {
	return OperationPlanner{
		unit:                  minCheckpointPeriod.Unit,
		targetBatchesPerStep:  batchesPerStep,
		recordsPerEpoch:       recordsPerEpoch,
		minValidationPeriod:   minValidationPeriod,
		minCheckpointPeriod:   minCheckpointPeriod,
		trialGlobalBatchSizes: make(map[RequestID]int),
		stepCounts:            make(map[RequestID]int),
		trialOpsToPlannedOps:  make(map[RequestID][]opToPlannedOps),
		unitsSinceValidation:  make(map[RequestID]model.Length),
		unitsSinceCheckpoint:  make(map[RequestID]model.Length),
		totalUnitsCompleted:   model.NewLength(minCheckpointPeriod.Unit, 0),
	}
}

// Plan plans the given operations.
func (p *OperationPlanner) Plan(ops []Operation) (plannedOps []Operation) {
	for _, op := range ops {
		switch tOp := op.(type) {
		case Create:
			p.trialGlobalBatchSizes[tOp.RequestID] = tOp.Hparams.GlobalBatchSize()
			p.stepCounts[tOp.RequestID] = 0
			p.unitsSinceValidation[tOp.RequestID] = model.NewLength(p.unit, 0)
			p.unitsSinceCheckpoint[tOp.RequestID] = model.NewLength(p.unit, 0)
			plannedOps = append(plannedOps, tOp)
		case Train:
			workloads := p.train(tOp.RequestID, tOp.Length)
			p.trialOpsToPlannedOps[tOp.RequestID] = append(p.trialOpsToPlannedOps[tOp.RequestID],
				opToPlannedOps{op: tOp, plannedOps: workloads})
			plannedOps = append(plannedOps, workloads...)
		case Validate:
			workloads := p.validate(tOp.RequestID)
			p.trialOpsToPlannedOps[tOp.RequestID] = append(p.trialOpsToPlannedOps[tOp.RequestID],
				opToPlannedOps{op: tOp, plannedOps: workloads})
			plannedOps = append(plannedOps, workloads...)
		case Checkpoint:
			workloads := p.checkpoint(tOp.RequestID)
			p.trialOpsToPlannedOps[tOp.RequestID] = append(p.trialOpsToPlannedOps[tOp.RequestID],
				opToPlannedOps{op: tOp, plannedOps: workloads})
			plannedOps = append(plannedOps, workloads...)
		default:
			plannedOps = append(plannedOps, tOp)
		}
	}
	return plannedOps
}

// WorkloadCompleted collates workloads back into search method operations, through multiple calls.
func (p *OperationPlanner) WorkloadCompleted(
	requestID RequestID, workload Workload,
) (op Operation, err error) {
	opsToPlannedOps := p.trialOpsToPlannedOps[requestID]
	if len(opsToPlannedOps) == 0 {
		return nil, fmt.Errorf("received completed workload %s when none were expected", workload)
	}

	expectedWorkload, ok := opsToPlannedOps[0].plannedOps[0].(WorkloadOperation)
	if !ok {
		return nil, fmt.Errorf("planned ops should only be workloads %s", expectedWorkload)
	}

	if expectedWorkload.StepID != workload.StepID {
		return nil, fmt.Errorf("received %s but expected operation %s", workload, expectedWorkload)
	}

	if expectedWorkload.Kind != workload.Kind {
		return nil, fmt.Errorf("received %s but expected operation %s", workload, expectedWorkload)
	}

	if expectedWorkload.NumBatches != workload.NumBatches {
		return nil, fmt.Errorf("received %s but expected operation %s", workload, expectedWorkload)
	}

	unitsThisWorkload := p.unitsFromBatches(requestID, workload.NumBatches)
	p.totalUnitsCompleted = p.totalUnitsCompleted.Add(unitsThisWorkload)

	opsToPlannedOps[0].plannedOps = opsToPlannedOps[0].plannedOps[1:]
	if len(opsToPlannedOps[0].plannedOps) == 0 {
		completedOp := opsToPlannedOps[0]
		p.trialOpsToPlannedOps[requestID] = p.trialOpsToPlannedOps[requestID][1:]
		return completedOp.op, nil
	}
	p.trialOpsToPlannedOps[requestID] = opsToPlannedOps
	return nil, nil
}

func (p *OperationPlanner) train(
	requestID RequestID,
	unitsNeeded model.Length,
) (ops []Operation) {
	batchesNeeded, trunc := p.unitsToBatches(requestID, unitsNeeded)
	p.totalUnitsCompleted = p.totalUnitsCompleted.Add(trunc)
	p.unitsSinceValidation[requestID] = p.unitsSinceValidation[requestID].Add(trunc)
	p.unitsSinceCheckpoint[requestID] = p.unitsSinceCheckpoint[requestID].Add(trunc)
	for curBatches := 0; curBatches < batchesNeeded; curBatches += p.targetBatchesPerStep {
		batchesLeft := batchesNeeded - curBatches
		batchesThisStep := min(batchesLeft, p.targetBatchesPerStep)
		if p.minValidationNeeded(requestID, batchesThisStep) {
			ops = append(ops, p.validate(requestID))
		}
		if p.minCheckpointNeeded(requestID, batchesThisStep) {
			ops = append(ops, p.checkpoint(requestID))
		}
		p.stepCounts[requestID]++
		ops = append(ops, p.trainStep(requestID, batchesThisStep))
	}
	return ops
}

func (p *OperationPlanner) trainStep(requestID RequestID, numBatches int) Operation {
	unitsThisStep := p.unitsFromBatches(requestID, numBatches)
	p.unitsSinceValidation[requestID] = p.unitsSinceValidation[requestID].Add(unitsThisStep)
	p.unitsSinceCheckpoint[requestID] = p.unitsSinceCheckpoint[requestID].Add(unitsThisStep)
	return NewTrainWorkload(requestID, p.stepCounts[requestID], numBatches)
}

func (p *OperationPlanner) validate(requestID RequestID) (ops []Operation) {
	ops = append(ops, NewValidateWorkload(requestID, p.stepCounts[requestID]))
	return ops
}

func (p *OperationPlanner) checkpoint(requestID RequestID) (ops []Operation) {
	ops = append(ops, NewCheckpointWorkload(requestID, p.stepCounts[requestID]))
	return ops
}

func (p *OperationPlanner) minValidationNeeded(requestID RequestID, batchesThisStep int) bool {
	if p.minValidationPeriod.Units == 0 {
		return false
	}
	unitsThisStep := p.unitsFromBatches(requestID, batchesThisStep)
	unitsAfterStep := p.unitsSinceValidation[requestID].Add(unitsThisStep)
	return unitsAfterStep.Units >= p.minValidationPeriod.Units
}

func (p *OperationPlanner) minCheckpointNeeded(requestID RequestID, batchesThisStep int) bool {
	if p.minCheckpointPeriod.Units == 0 {
		return false
	}
	unitsThisStep := p.unitsFromBatches(requestID, batchesThisStep)
	unitsAfterStep := p.unitsSinceCheckpoint[requestID].Add(unitsThisStep)
	return unitsAfterStep.Units >= p.minCheckpointPeriod.Units
}

// unitsFromBatches determines the number of units completed during a given workload.
func (p OperationPlanner) unitsFromBatches(requestID RequestID, batches int) model.Length {
	switch p.unit {
	case model.Records:
		return model.NewLengthInRecords(batches * p.trialGlobalBatchSizes[requestID])
	case model.Batches:
		return model.NewLengthInBatches(batches)
	case model.Epochs:
		// Round up because if we ran a partial epoch, we always _meant_ to run a full one and
		// truncated on the nearest batch.
		numRecords := batches * p.trialGlobalBatchSizes[requestID]
		numEpochs := math.Ceil(float64(numRecords) / float64(p.recordsPerEpoch))
		return model.NewLengthInEpochs(int(numEpochs))
	default:
		panic(fmt.Sprintf("invalid in OperationPlanner: %s", p.unit))
	}
}

// unitsToBatches converts a training length to the nearest batch, potentially truncating some units
// if they are provided as records or epochs.
func (p OperationPlanner) unitsToBatches(
	requestID RequestID, l model.Length,
) (batches int, truncated model.Length) {
	globalBatchSize := p.trialGlobalBatchSizes[requestID]
	switch l.Unit {
	case model.Records:
		return l.Units / globalBatchSize, model.NewLengthInRecords(l.Units % globalBatchSize)
	case model.Batches:
		return l.Units, model.NewLengthInBatches(0)
	case model.Epochs:
		return (l.Units * p.recordsPerEpoch) / globalBatchSize, model.NewLengthInEpochs(0)
	default:
		panic(fmt.Sprintf("invalid Unit passed to unitsToBatches %s", l.Unit))
	}
}
