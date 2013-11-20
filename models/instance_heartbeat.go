package models

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type InstanceState string

const (
	InstanceStateInvalid    InstanceState = ""
	InstanceStateStarting   InstanceState = "STARTING"
	InstanceStateRunning    InstanceState = "RUNNING"
	InstanceStateCrashed    InstanceState = "CRASHED"
	InstanceStateEvacuating InstanceState = "EVACUATING"
)

type InstanceHeartbeat struct {
	CCPartition    string        `json:"cc_partition"`
	AppGuid        string        `json:"droplet"`
	AppVersion     string        `json:"version"`
	InstanceGuid   string        `json:"instance"`
	InstanceIndex  int           `json:"index"`
	State          InstanceState `json:"state"`
	StateTimestamp float64       `json:"state_timestamp"`
	DeaGuid        string        `json:"dea_guid"`
}

func NewInstanceHeartbeatFromJSON(encoded []byte) (InstanceHeartbeat, error) {
	var instance InstanceHeartbeat
	err := json.Unmarshal(encoded, &instance)
	if err != nil {
		return InstanceHeartbeat{}, err
	}
	return instance, nil
}

func (instance InstanceHeartbeat) ToJSON() []byte {
	encoded, _ := json.Marshal(instance)
	return encoded
}

func NewInstanceHeartbeatFromStoreFormat(appGuid string, appVersion string, instanceGuid string, encoded []byte) (InstanceHeartbeat, error) {
	components := strings.Split(string(encoded), "|")
	instanceIndex, err := strconv.Atoi(components[0])
	if err != nil {
		return InstanceHeartbeat{}, err
	}
	stateTimestamp, err := strconv.ParseFloat(components[2], 64)
	if err != nil {
		return InstanceHeartbeat{}, err
	}

	return InstanceHeartbeat{
		CCPartition:    "default",
		AppGuid:        appGuid,
		AppVersion:     appVersion,
		InstanceGuid:   instanceGuid,
		InstanceIndex:  instanceIndex,
		State:          InstanceState(components[1]),
		StateTimestamp: stateTimestamp,
		DeaGuid:        components[3],
	}, err
}

func (instance InstanceHeartbeat) ToStoreFormat() []byte {
	return []byte(fmt.Sprintf("%d|%s|%.0f|%s", instance.InstanceIndex, instance.State, instance.StateTimestamp, instance.DeaGuid))
}

func (instance InstanceHeartbeat) StoreKey() string {
	return instance.InstanceGuid
}

func (instance InstanceHeartbeat) IsStartingOrRunning() bool {
	return instance.IsStarting() || instance.IsRunning()
}

func (instance InstanceHeartbeat) IsStarting() bool {
	return instance.State == InstanceStateStarting
}

func (instance InstanceHeartbeat) IsRunning() bool {
	return instance.State == InstanceStateRunning
}

func (instance InstanceHeartbeat) IsCrashed() bool {
	return instance.State == InstanceStateCrashed
}

func (instance InstanceHeartbeat) IsEvacuating() bool {
	return instance.State == InstanceStateEvacuating
}

func (instance InstanceHeartbeat) LogDescription() map[string]string {
	return map[string]string{
		"AppGuid":        instance.AppGuid,
		"AppVersion":     instance.AppVersion,
		"InstanceGuid":   instance.InstanceGuid,
		"InstanceIndex":  strconv.Itoa(instance.InstanceIndex),
		"State":          string(instance.State),
		"StateTimestamp": strconv.Itoa(int(instance.StateTimestamp)),
		"DeaGuid":        instance.DeaGuid,
	}
}
