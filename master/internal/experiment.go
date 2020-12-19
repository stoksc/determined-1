package internal

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/determined-ai/determined/master/internal/db"
	"github.com/determined-ai/determined/master/internal/sproto"
	"github.com/determined-ai/determined/master/internal/telemetry"
	"github.com/determined-ai/determined/master/pkg/actor"
	"github.com/determined-ai/determined/master/pkg/archive"
	"github.com/determined-ai/determined/master/pkg/model"
	"github.com/determined-ai/determined/master/pkg/searcher"
	"github.com/determined-ai/determined/master/pkg/tasks"
	"github.com/determined-ai/determined/master/pkg/workload"
	"github.com/determined-ai/determined/proto/pkg/apiv1"
)

// Experiment-specific actor messages.
type (
	trialCreated struct {
		create  searcher.Create
		trialID int
	}
	trialCompletedOperation struct {
		trialID int
		op      searcher.Runnable
		metrics interface{}
	}
	trialCompletedWorkload struct {
		trialID          int
		completedMessage workload.CompletedMessage
		// unitsCompleted is passed as a float because while the searcher will only request integral
		// units, a trial may complete partial units (especially in the case of epochs).
		unitsCompleted float64
	}
	trialExitedEarly struct {
		trialID      int
		exitedReason *workload.ExitedReason
	}
	trialSnapshot struct {
		trialID  int
		snapshot []byte
	}
	getProgress    struct{}
	getTrial       struct{ trialID int }
	restore        struct{}
	killExperiment struct{}
)

type (
	experimentState struct {
		SearcherState  []byte
		BestValidation *float64
	}

	experiment struct {
		experimentState

		*model.Experiment
		modelDefinition     archive.Archive
		rm                  *actor.Ref
		trialLogger         *actor.Ref
		db                  *db.PgDB
		searcher            *searcher.Searcher
		warmStartCheckpoint *model.Checkpoint
		restoring           bool

		agentUserGroup *model.AgentUserGroup
		taskSpec       *tasks.TaskSpec
	}
)

// Create a new experiment object from the given model experiment object, along with its searcher
// and log. If the input object has no ID set, also create a new experiment in the database and set
// the returned object's ID appropriately.
func newExperiment(master *Master, expModel *model.Experiment) (*experiment, error) {
	conf := expModel.Config

	if err := sproto.ValidateRP(master.system, conf.Resources.ResourcePool); err != nil {
		return nil, err
	}

	method := searcher.NewSearchMethod(conf.Searcher)
	search := searcher.NewSearcher(conf.Reproducibility.ExperimentSeed, method, conf.Hyperparameters)

	// Retrieve the warm start checkpoint, if provided.
	checkpoint, err := checkpointFromTrialIDOrUUID(
		master.db, conf.Searcher.SourceTrialID, conf.Searcher.SourceCheckpointUUID)
	if err != nil {
		return nil, err
	}

	// Decompress the model definition from .tar.gz into an Archive.
	modelDefinition, err := archive.FromTarGz(expModel.ModelDefinitionBytes)
	if err != nil {
		return nil, err
	}

	if expModel.ID == 0 {
		if err = master.db.AddExperiment(expModel); err != nil {
			return nil, err
		}
	}

	agentUserGroup, err := master.db.AgentUserGroup(*expModel.OwnerID)
	if err != nil {
		return nil, err
	}

	if agentUserGroup == nil {
		agentUserGroup = &master.config.Security.DefaultTask
	}

	return &experiment{
		Experiment:          expModel,
		modelDefinition:     modelDefinition,
		rm:                  master.rm,
		trialLogger:         master.trialLogger,
		db:                  master.db,
		searcher:            search,
		warmStartCheckpoint: checkpoint,

		agentUserGroup: agentUserGroup,
		taskSpec:       master.taskSpec,
	}, nil
}

func restoreExperiment(master *Master, expModel *model.Experiment) error {
	// Experiments which were trying to stop need to be marked as terminal in the database.
	if terminal, ok := model.StoppingToTerminalStates[expModel.State]; ok {
		if err := master.db.TerminateExperimentInRestart(expModel.ID, terminal); err != nil {
			return errors.Wrapf(err, "terminating experiment %d", expModel.ID)
		}
		expModel.State = terminal
		telemetry.ReportExperimentStateChanged(master.system, master.db, *expModel)
		return nil
	} else if _, ok := model.RunningStates[expModel.State]; !ok {
		return errors.Errorf(
			"cannot restore experiment %d from state %v", expModel.ID, expModel.State,
		)
	}

	e, err := newExperiment(master, expModel)
	if err != nil {
		return errors.Wrapf(err, "failed to create experiment %d from model", expModel.ID)
	}
	log.WithField("experiment", e.ID).Info("restoring experiment")
	e.restoring = true

	ref, _ := master.system.ActorOf(actor.Addr("experiments", e.ID), e)
	if err := master.system.Ask(ref, restore{}).Error(); err != nil {
		return errors.Wrapf(err, "failed to restore experiment %d", e.ID)
	}
	return nil
}

func (e *experiment) Receive(ctx *actor.Context) error {
	switch msg := ctx.Message().(type) {
	// Searcher-related messages.
	case actor.PreStart:
		telemetry.ReportExperimentCreated(ctx.Self().System(), *e.Experiment)

		ctx.Tell(e.rm, sproto.SetGroupMaxSlots{
			MaxSlots: e.Config.Resources.MaxSlots,
			Handler:  ctx.Self(),
		})
		ctx.Tell(e.rm, sproto.SetGroupWeight{Weight: e.Config.Resources.Weight, Handler: ctx.Self()})
		ctx.Tell(e.rm, sproto.SetGroupPriority{
			Priority: e.Config.Resources.Priority,
			Handler:  ctx.Self(),
		})

		ops, err := e.searcher.InitialOperations()
		e.processOperations(ctx, ops, err)
	case trialCreated:
		ops, err := e.searcher.TrialCreated(msg.create, msg.trialID)
		e.processOperations(ctx, ops, err)
	case trialCompletedOperation:
		ops, err := e.searcher.OperationCompleted(msg.trialID, msg.op, msg.metrics)
		e.processOperations(ctx, ops, err)
	case trialCompletedWorkload:
		e.searcher.WorkloadCompleted(msg.unitsCompleted)
		if msg.completedMessage.Workload.Kind == workload.ComputeValidationMetrics &&
			// Messages indicating trial failures won't have metrics (or need their status).
			msg.completedMessage.ExitedReason == nil {
			ctx.Respond(e.isBestValidation(*msg.completedMessage.ValidationMetrics))
		}
		progress := e.searcher.Progress()
		if err := e.db.SaveExperimentProgress(e.ID, &progress); err != nil {
			ctx.Log().WithError(err).Error("failed to save experiment progress")
		}
	case trialExitedEarly:
		ops, err := e.searcher.TrialExitedEarly(msg.trialID, msg.exitedReason)
		e.processOperations(ctx, ops, err)
	case trialSnapshot:
		if b, err := e.save(); err != nil {
			return err
		} else if requestID, ok := e.searcher.RequestIDs[msg.trialID]; !ok {
			return err // Impossible error
		} else if err := e.db.SaveSnapshot(e.ID, msg.trialID, requestID, b, msg.snapshot); err != nil {
			return err
		}
	case sendNextWorkload:
		// Pass this back to the trial; this message is just used to allow the trial to synchronize
		// with the searcher.
		ctx.Tell(ctx.Sender(), msg)
	case actor.ChildFailed:
		ctx.Log().WithError(msg.Error).Error("trial failed unexpectedly")
		requestID := model.MustParse(msg.Child.Address().Local())
		ops, err := e.searcher.TrialClosed(requestID)
		e.processOperations(ctx, ops, err)
		if e.canTerminate(ctx) {
			ctx.Self().Stop()
		}
	case actor.ChildStopped:
		requestID := model.MustParse(msg.Child.Address().Local())
		ops, err := e.searcher.TrialClosed(requestID)
		e.processOperations(ctx, ops, err)
		if e.canTerminate(ctx) {
			ctx.Self().Stop()
		}
	case getProgress:
		progress := e.searcher.Progress()
		ctx.Respond(&progress)

	case getTrial:
		requestID, ok := e.searcher.RequestID(msg.trialID)
		ref := ctx.Child(requestID)
		if ok && ref != nil {
			ctx.Respond(ref)
		}

	case restore:
		if err := e.restore(ctx); err != nil {
			e.updateState(ctx, model.StoppingErrorState)
			return err
		}

	// Patch experiment messages.
	case model.State:
		e.updateState(ctx, msg)
	case sproto.SetGroupMaxSlots:
		e.Config.Resources.MaxSlots = msg.MaxSlots
		msg.Handler = ctx.Self()
		ctx.Tell(e.rm, msg)
	case sproto.SetGroupWeight:
		e.Config.Resources.Weight = msg.Weight
		msg.Handler = ctx.Self()
		ctx.Tell(e.rm, msg)

	case killExperiment:
		if _, running := model.RunningStates[e.State]; running {
			e.updateState(ctx, model.StoppingCanceledState)
		}

		for _, child := range ctx.Children() {
			ctx.Tell(child, killTrial{})
		}

	// Experiment shutdown logic.
	case actor.PostStop:
		if err := e.db.SaveExperimentProgress(e.ID, nil); err != nil {
			ctx.Log().Error(err)
		}

		state := model.StoppingToTerminalStates[e.State]
		if wasPatched, err := e.Transition(state); err != nil {
			return err
		} else if !wasPatched {
			return errors.New("experiment is already in a terminal state")
		}
		telemetry.ReportExperimentStateChanged(ctx.Self().System(), e.db, *e.Experiment)

		if err := e.db.SaveExperimentState(e.Experiment); err != nil {
			return err
		}
		ctx.Log().Infof("experiment state changed to %s", e.State)
		addr := actor.Addr(fmt.Sprintf("experiment-%d-checkpoint-gc", e.ID))
		ctx.Self().System().ActorOf(addr, &checkpointGCTask{
			agentUserGroup: e.agentUserGroup,
			taskSpec:       e.taskSpec,
			rm:             e.rm,
			db:             e.db,
			experiment:     e.Experiment,
		})

		// Discard searcher events for all terminal experiments (even failed ones).
		// This is safe because we never try to restore the state of the searcher for
		// terminated experiments.
		if err := e.db.DeleteSearcherEvents(e.Experiment.ID); err != nil {
			ctx.Log().WithError(err).Errorf(
				"failure to delete searcher events for experiment: %d", e.Experiment.ID)
		}

		ctx.Log().Info("experiment shut down successfully")

	case *apiv1.ActivateExperimentRequest:
		switch ok := e.updateState(ctx, model.ActiveState); ok {
		case true:
			ctx.Respond(&apiv1.ActivateExperimentResponse{})
		default:
			ctx.Respond(status.Errorf(codes.FailedPrecondition,
				"experiment in incompatible state %s", e.State))
		}

	case *apiv1.PauseExperimentRequest:
		switch ok := e.updateState(ctx, model.PausedState); ok {
		case true:
			ctx.Respond(&apiv1.PauseExperimentResponse{})
		default:
			ctx.Respond(status.Errorf(codes.FailedPrecondition,
				"experiment in incompatible state %s", e.State))
		}

	case *apiv1.CancelExperimentRequest:
		switch {
		case model.StoppingStates[e.State] || model.TerminalStates[e.State]:
			ctx.Respond(&apiv1.CancelExperimentResponse{})
		default:
			switch ok := e.updateState(ctx, model.StoppingCanceledState); ok {
			case true:
				ctx.Respond(&apiv1.CancelExperimentResponse{})
				for _, child := range ctx.Children() {
					ctx.Tell(child, killTrial{})
				}
			default:
				ctx.Respond(status.Errorf(codes.FailedPrecondition,
					"experiment in incompatible state %s", e.State))
			}
		}

	case *apiv1.KillExperimentRequest:
		switch {
		case model.StoppingStates[e.State] || model.TerminalStates[e.State]:
			ctx.Respond(&apiv1.KillExperimentResponse{})
		default:
			switch ok := e.updateState(ctx, model.StoppingCanceledState); ok {
			case true:
				ctx.Respond(&apiv1.KillExperimentResponse{})
				for _, child := range ctx.Children() {
					ctx.Tell(child, killTrial{})
				}
			default:
				ctx.Respond(status.Errorf(codes.FailedPrecondition,
					"experiment in incompatible state %s", e.State))
			}
		}
	}

	return nil
}

func (e *experiment) restore(ctx *actor.Context) error {
	defer func() { e.restoring = false }()
	switch b, err := e.db.ExperimentSnapshot(e.ID); {
	case err == db.ErrNotFound:
	case err != nil:
		return errors.Wrap(err, "failed to retrieve searcher snapshot")
	default:
		if err := e.load(b); err != nil {
			return errors.Wrap(err, "failed to load experiment snapshot")
		}
	}
	e.processOperations(ctx, e.searcher.TrialOperations, nil)
	ctx.AskAll(ctx.AskAll(restoreTrial{}, ctx.Children()...))
	return nil
}

func (e *experiment) processOperations(
	ctx *actor.Context, ops []searcher.Operation, err error) {
	if _, ok := model.StoppingStates[e.State]; ok {
		return
	}
	if err != nil {
		ctx.Log().Error(err)
		e.updateState(ctx, model.StoppingErrorState)
		return
	}

	trialOperations := make(map[model.RequestID][]searcher.Operation)
	for _, operation := range ops {
		ctx.Log().Debugf("handling searcher op: %v", operation)
		switch op := operation.(type) {
		case searcher.Create:
			checkpoint := e.warmStartCheckpoint
			// If the Create specifies a checkpoint, ignore the experiment-wide one.
			if op.Checkpoint != nil {
				trialID, ok := e.searcher.TrialID(op.Checkpoint.RequestID)
				if !ok {
					ctx.Log().Error(errors.Errorf(
						"invalid request ID in Create operation: %d", op.Checkpoint.RequestID))
					e.updateState(ctx, model.StoppingErrorState)
					return
				}
				checkpointModel, err := checkpointFromTrialIDOrUUID(e.db, &trialID, nil)
				if err != nil {
					ctx.Log().Error(errors.Wrap(err, "checkpoint not found"))
					e.updateState(ctx, model.StoppingErrorState)
					return
				}
				checkpoint = checkpointModel
			}
			ctx.ActorOf(op.RequestID, newTrial(e, op, checkpoint))
		case searcher.Requested:
			trialOperations[op.GetRequestID()] = append(trialOperations[op.GetRequestID()], op)
		case searcher.Shutdown:
			if op.Failure {
				e.updateState(ctx, model.StoppingErrorState)
			} else {
				e.updateState(ctx, model.StoppingCompletedState)
			}
		default:
			panic(fmt.Sprintf("unexpected operation: %v", op))
		}
	}
	for requestID, ops := range trialOperations {
		ctx.Tell(ctx.Child(requestID), ops)
	}
}

func (e *experiment) isBestValidation(metrics workload.ValidationMetrics) bool {
	metricName := e.Config.Searcher.Metric
	validation, err := metrics.Metric(metricName)
	if err != nil {
		// TODO: Better error handling here.
		return false
	}
	smallerIsBetter := e.Config.Searcher.SmallerIsBetter
	isBest := (e.BestValidation == nil) ||
		(smallerIsBetter && validation < *e.BestValidation) ||
		(!smallerIsBetter && validation > *e.BestValidation)
	if isBest {
		e.BestValidation = &validation
	}
	return isBest
}

func (e *experiment) updateState(ctx *actor.Context, state model.State) bool {
	if wasPatched, err := e.Transition(state); err != nil {
		ctx.Log().Errorf("error transitioning experiment state: %s", err)
		return false
	} else if !wasPatched {
		return true
	}
	telemetry.ReportExperimentStateChanged(ctx.Self().System(), e.db, *e.Experiment)

	ctx.Log().Infof("experiment state changed to %s", state)
	for _, child := range ctx.Children() {
		ctx.Tell(child, state)
	}
	if err := e.db.SaveExperimentState(e.Experiment); err != nil {
		ctx.Log().Errorf("error saving experiment state: %s", err)
	}
	if e.canTerminate(ctx) {
		ctx.Self().Stop()
	}
	// The database error is explicitly ignored.
	return true
}

func (e *experiment) canTerminate(ctx *actor.Context) bool {
	return model.StoppingStates[e.State] && len(ctx.Children()) == 0
}

func (e *experiment) save() ([]byte, error) {
	searcherSnapshot, err := e.searcher.Save()
	if err != nil {
		return nil, errors.Wrap(err, "failed to snapshot searcher")
	}
	log.Info(string(searcherSnapshot))
	e.SearcherState = searcherSnapshot
	experimentSnapshot, err := json.Marshal(e.experimentState)
	return experimentSnapshot, errors.Wrap(err, "failed to snapshot experiment")
}

func (e *experiment) load(experimentSnapshot []byte) error {
	if err := json.Unmarshal(experimentSnapshot, &e.experimentState); err != nil {
		return errors.Wrap(err, "failed to unmarshal experiment snapshot")
	}
	log.Info(string(e.experimentState.SearcherState))
	if err := e.searcher.Load(e.SearcherState, *e.Experiment); err != nil {
		return errors.Wrap(err, "failed to load searcher snapshot")
	}
	return nil
}
