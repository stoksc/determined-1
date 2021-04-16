package model

import (
	"time"

	"github.com/determined-ai/determined/master/pkg/container"
	"github.com/determined-ai/determined/master/pkg/device"
)

// AgentSummary summarizes the state on an agent.
type AgentSummary struct {
	ID             string       `json:"id"`
	RegisteredTime time.Time    `json:"registered_time"`
	Slots          SlotsSummary `json:"slots"`
	NumContainers  int          `json:"num_containers"`
	ResourcePool   string       `json:"resource_pool"`
	Label          string       `json:"label"`
	RemoteAddr     string       `json:"remote_addr"`
}

// AgentsSummary is a map of agent IDs to a summary of the agent.
type AgentsSummary map[string]AgentSummary

// SlotsSummary contains a summary for a number of slots.
type SlotsSummary map[string]SlotSummary

// SlotSummary summarizes the state of a slot.
type SlotSummary struct {
	ID        string               `json:"id"`
	Device    device.Device        `json:"device"`
	Enabled   bool                 `json:"enabled"`
	Container *container.Container `json:"container"`
}
