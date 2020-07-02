package searcher

import (
	"fmt"
	"math"

	"github.com/determined-ai/determined/master/pkg/model"
)

// TrialWorkloadPlanner manages all the context around how requested "units" actually turn into
// workloads that the trial runs. probably can be merged with trial_workload_sequencer.go once
// that code is pulled up to the experiment level.
// TODO(brad): describe that it is essentially a pipe.
type TrialWorkloadPlanner struct {
	kind                  model.Kind
	targetBatchesPerStep  int
	recordsPerEpoch       int
	trialGlobalBatchSizes map[RequestID]int
	stepCounts            map[RequestID]int
	trialOpsToPlannedOps  map[RequestID][]opToPlannedOps
	totalUnitsCompleted   model.Length
}

type opToPlannedOps struct {
	op         Operation
	plannedOps []Operation
}

// NewTrialWorkloadPlanner creates a workload planner to plan searcher ops into workload ops.
func NewTrialWorkloadPlanner(
	kind model.Kind, targetBatchesPerStep, recordsPerEpoch int,
) TrialWorkloadPlanner {
	return TrialWorkloadPlanner{
		kind:                  kind,
		targetBatchesPerStep:  targetBatchesPerStep,
		recordsPerEpoch:       recordsPerEpoch,
		trialGlobalBatchSizes: make(map[RequestID]int),
		stepCounts:            make(map[RequestID]int),
		trialOpsToPlannedOps:  make(map[RequestID][]opToPlannedOps),
		totalUnitsCompleted:   model.NewLength(kind, 0),
	}
}

// Plan plans the given operations.
func (p *TrialWorkloadPlanner) Plan(ops []Operation) (plannedOps []Operation) {
	for _, op := range ops {
		switch tOp := op.(type) {
		case Create:
			p.trialGlobalBatchSizes[tOp.RequestID] = tOp.Hparams.GlobalBatchSize()
			p.stepCounts[tOp.RequestID] = 0
			plannedOps = append(plannedOps, tOp)
		case Train:
			workloads := p.train(tOp.RequestID, tOp.Length)
			p.trialOpsToPlannedOps[tOp.RequestID] = append(p.trialOpsToPlannedOps[tOp.RequestID],
				opToPlannedOps{op: tOp, plannedOps: workloads})
			plannedOps = append(plannedOps, workloads...)
		case Validate:
			workload := p.validate(tOp.RequestID)
			p.trialOpsToPlannedOps[tOp.RequestID] = append(p.trialOpsToPlannedOps[tOp.RequestID],
				opToPlannedOps{op: tOp, plannedOps: []Operation{workload}})
			plannedOps = append(plannedOps, workload)
		case Checkpoint:
			workload := p.checkpoint(tOp.RequestID)
			p.trialOpsToPlannedOps[tOp.RequestID] = append(p.trialOpsToPlannedOps[tOp.RequestID],
				opToPlannedOps{op: tOp, plannedOps: []Operation{workload}})
			plannedOps = append(plannedOps, workload)
		default:
			plannedOps = append(plannedOps, tOp)
		}
	}
	return plannedOps
}

// WorkloadCompleted collates the given workload back into searcher ops.
func (p *TrialWorkloadPlanner) WorkloadCompleted(
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

	if expectedWorkload.Kind != workload.Kind {
		return nil, fmt.Errorf("received %s but expected operation %s", workload, expectedWorkload)
	}

	if expectedWorkload.NumBatches != workload.NumBatches {
		return nil, fmt.Errorf("received %s but expected operation %s", workload, expectedWorkload)
	}

	unitsThisWorkload := p.unitsFromWorkload(p.kind, workload, requestID)
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

func (p *TrialWorkloadPlanner) train(
	requestID RequestID,
	unitsNeeded model.Length,
) (ops []Operation) {
	// TODO(brad): figure out how to account for truncated records
	batchesNeeded, trunc := p.unitsToBatches(unitsNeeded, requestID)
	p.totalUnitsCompleted = p.totalUnitsCompleted.Add(trunc)
	for curBatches := 0; curBatches < batchesNeeded; curBatches += p.targetBatchesPerStep {
		batchesLeft := batchesNeeded - curBatches
		batchesThisStep := min(batchesLeft, p.targetBatchesPerStep)
		p.stepCounts[requestID]++
		ops = append(ops, NewTrainWorkload(requestID, p.stepCounts[requestID], batchesThisStep))
	}
	return ops
}

func (p *TrialWorkloadPlanner) validate(requestID RequestID) Operation {
	return NewValidateWorkload(requestID, p.stepCounts[requestID])
}

func (p *TrialWorkloadPlanner) checkpoint(requestID RequestID) Operation {
	return NewCheckpointWorkload(requestID, p.stepCounts[requestID])
}

func (p *TrialWorkloadPlanner) close(requestID RequestID) Close {
	return NewClose(requestID)
}

// unitsFromWorkload determines the number of units completed during a given workload.
func (p TrialWorkloadPlanner) unitsFromWorkload(
	kind model.Kind, workload Workload, requestID RequestID,
) model.Length {
	switch kind {
	case model.Records:
		return model.NewLengthInRecords(workload.NumBatches * p.trialGlobalBatchSizes[requestID])
	case model.Batches:
		return model.NewLengthInBatches(workload.NumBatches)
	case model.Epochs:
		// Round up because if we ran a partial epoch, we always _meant_ to run a full one and
		// truncated on the nearest batch.
		numRecords := workload.NumBatches * p.trialGlobalBatchSizes[requestID]
		numEpochs := math.Ceil(float64(numRecords) / float64(p.recordsPerEpoch))
		return model.NewLengthInEpochs(int(numEpochs))
	default:
		panic(fmt.Sprintf("invalid Kind passed to unitsFromStep %s", kind))
	}
}

// unitsToBatches converts a training length to the nearest batch. This function is necessary
// because the harness expects RUN_STEP's to contain the number of batches to train for, so searcher
// training length must be rounded to the nearest batch before they are sent and partial batches are
// hard.
func (p TrialWorkloadPlanner) unitsToBatches(
	l model.Length, requestID RequestID,
) (batches int, truncated model.Length) {
	globalBatchSize := p.trialGlobalBatchSizes[requestID]
	switch l.Kind {
	case model.Records:
		return l.Units / globalBatchSize, model.NewLengthInRecords(l.Units % globalBatchSize)
	case model.Batches:
		return l.Units, model.NewLengthInBatches(0)
	case model.Epochs:
		return (l.Units * p.recordsPerEpoch) / globalBatchSize, model.NewLengthInEpochs(0)
	default:
		panic(fmt.Sprintf("invalid Kind passed to unitsToBatches %s", l.Kind))
	}
}
