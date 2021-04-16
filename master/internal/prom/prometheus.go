package prom

import (
	"strconv"

	"github.com/labstack/echo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/determined-ai/determined/master/internal/api"
	"github.com/determined-ai/determined/master/pkg/actor"
	"github.com/determined-ai/determined/master/pkg/container"
	"github.com/determined-ai/determined/master/pkg/device"
	"github.com/determined-ai/determined/master/pkg/model"
)

// TODO: export these separately under /debug/prom/det-state-metrics.
var (
	containerTrials = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "det",
		Name:      "container_trial",
		Help:      "Exposes mapping of containers to trials (useful to join with cAdvisor metrics).",
	}, []string{"container_id", "trial_id"})
	// TODO: k8s pod to trial mappings.
	//podTrials = promauto.NewGaugeVec(prometheus.GaugeOpts{
	//	Subsystem: "det",
	//	Name:      "pod_trial",
	//	Help:      "Exposes mapping of pods to trials (useful to join with Kubernetes metrics).",
	//}, []string{"pod_name", "trial_id"})
	gpuTrials = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "det",
		Name:      "gpu_uuid_trial",
		Help:      "Exposes mapping of GPU UUIDs to trials (useful to join with dcgm-exporter metrics).",
	}, []string{"gpu_uuid", "trial_id"})
)

// TrialContainerStarted records a metric indicating the trial with the container ID has started.
func TrialContainerStarted(trialID int, dockerContainerID string, c container.Container) {
	containerTrials.WithLabelValues(dockerContainerID, strconv.Itoa(trialID)).Inc()
	for _, d := range c.Devices {
		if d.Type == device.GPU {
			gpuTrials.WithLabelValues(d.UUID, strconv.Itoa(trialID)).Inc()
		}
	}
}

// TrialContainerStopped records a metric indicating the trial with the container ID has stopped.
func TrialContainerStopped(trialID int, dockerContainerID string, c container.Container) {
	containerTrials.WithLabelValues(dockerContainerID, strconv.Itoa(trialID)).Dec()
	for _, d := range c.Devices {
		if d.Type == device.GPU {
			gpuTrials.WithLabelValues(d.UUID, strconv.Itoa(trialID)).Dec()
		}
	}
}

const (
	// Locations of expected exporters.
	// TODO: These should either be pre-installed on agent AMIs, or the
	// API should just return agent URLs instead and leave the conversion
	// to a file_sd_config to the user.
	cAdvisorExporter = ":8080"
	dcgmExporter     = ":9400"

	// The are extra labels added to metrics on scrape.
	detAgentIDLabel      = "det_agent_id"
	detResourcePoolLabel = "det_resource_pool"
)

// GetFileSDConfig returns a handle that on request returns a JSON blob in the format specified by
// https://prometheus.io/docs/prometheus/latest/configuration/configuration/#file_sd_config
func GetFileSDConfig(system *actor.System) echo.HandlerFunc {
	return api.Route(func(c echo.Context) (interface{}, error) {
		type fileSDConfigEntry struct {
			Targets []string          `json:"targets"`
			Labels  map[string]string `json:"labels"`
		}
		summary := system.AskAt(actor.Addr("agents"), model.AgentsSummary{})
		var fileSDConfig []fileSDConfigEntry
		for _, a := range summary.Get().(model.AgentsSummary) {
			fileSDConfig = append(fileSDConfig, fileSDConfigEntry{
				// TODO: Maybe just expose what ports to scrape on agents as a config
				// (or maybe instead this really should just be a script that manipulates
				// the outputs of /api/v1/agents).
				Targets: []string{
					a.RemoteAddr + cAdvisorExporter,
					a.RemoteAddr + dcgmExporter,
				},
				Labels: map[string]string{
					detAgentIDLabel:      a.ID,
					detResourcePoolLabel: a.ResourcePool,
				},
			})
		}
		return fileSDConfig, nil
	})
}
