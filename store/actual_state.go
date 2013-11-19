package store

import (
	"fmt"
	"github.com/cloudfoundry/hm9000/models"
	"github.com/cloudfoundry/hm9000/storeadapter"
	"time"
)

func (store *RealStore) SyncHeartbeat(incomingHeartbeat models.Heartbeat) error {
	t := time.Now()

	store.logger.Debug("Syncing heartbeat")

	tGet := time.Now()
	existingInstanceHeartbeats, err := store.GetInstanceHeartbeats()
	if err != nil {
		store.logger.Error("Failed to load heartbeats", err)
		return err
	}
	store.logger.Debug(fmt.Sprintf("Read (%d) heartbeats took: %s", len(existingInstanceHeartbeats), time.Since(tGet)))

	filteredExistingInstanceHeartbeats := map[string]models.InstanceHeartbeat{}

	for _, existingInstanceHeartbeat := range existingInstanceHeartbeats {
		if existingInstanceHeartbeat.DeaGuid == incomingHeartbeat.DeaGuid {
			filteredExistingInstanceHeartbeats[existingInstanceHeartbeat.InstanceGuid] = existingInstanceHeartbeat
		}
	}

	incomingInstanceGuids := map[string]bool{}
	nodesToSave := []storeadapter.StoreNode{
		store.deaPresenceNode(incomingHeartbeat.DeaGuid),
	}

	for _, incomingInstanceHeartbeat := range incomingHeartbeat.InstanceHeartbeats {
		incomingInstanceGuids[incomingInstanceHeartbeat.InstanceGuid] = true
		existingInstanceHeartbeat, found := filteredExistingInstanceHeartbeats[incomingInstanceHeartbeat.InstanceGuid]

		if found && existingInstanceHeartbeat.State == incomingInstanceHeartbeat.State {
			continue
		}

		nodesToSave = append(nodesToSave, store.storeNodeForInstanceHeartbeat(incomingInstanceHeartbeat))
	}

	tSave := time.Now()
	err = store.adapter.Set(nodesToSave)
	dtSave := time.Since(tSave).Seconds()
	store.logger.Debug(fmt.Sprintf("Saved (%d) things took: %s", len(nodesToSave), time.Since(tSave)))

	if err != nil {
		store.logger.Error("Failed to save", err)
		return err
	}

	keysToDelete := []string{}

	for _, existingInstanceHeartbeat := range filteredExistingInstanceHeartbeats {
		if incomingInstanceGuids[existingInstanceHeartbeat.InstanceGuid] {
			continue
		}

		key := store.instanceHeartbeatStoreKey(existingInstanceHeartbeat.AppGuid, existingInstanceHeartbeat.AppVersion, existingInstanceHeartbeat.InstanceGuid)
		keysToDelete = append(keysToDelete, key)
	}

	tDelete := time.Now()
	err = store.adapter.Delete(keysToDelete...)
	dtDelete := time.Since(tDelete).Seconds()
	store.logger.Debug(fmt.Sprintf("Deleted (%d) things took: %s: %v", len(keysToDelete), time.Since(tDelete), keysToDelete))

	if err != nil {
		store.logger.Error("Failed to delete", err)
		return err
	}

	store.logger.Debug(fmt.Sprintf("Save Duration Actual"), map[string]string{
		"Number of Items":         fmt.Sprintf("%d", len(incomingHeartbeat.InstanceHeartbeats)),
		"Number of Items Saved":   fmt.Sprintf("%d", len(nodesToSave)),
		"Number of Items Deleted": fmt.Sprintf("%d", len(keysToDelete)),
		"Duration":                fmt.Sprintf("%.4f seconds", time.Since(t).Seconds()),
		"Save Duration":           fmt.Sprintf("%.4f seconds", dtSave),
		"Delete Duration":         fmt.Sprintf("%.4f seconds", dtDelete),
	})

	return nil
}

func (store *RealStore) GetInstanceHeartbeats() (results []models.InstanceHeartbeat, err error) {
	tList := time.Now()
	node, err := store.adapter.ListRecursively(store.SchemaRoot() + "/apps/actual")
	store.logger.Debug(fmt.Sprintf("GETINSTANCEHEARTBEATS List recursively took: %s", time.Since(tList)))

	if err == storeadapter.ErrorKeyNotFound {
		return results, nil
	} else if err != nil {
		store.logger.Error("GETINSTANCEHEARTBEATS Failed to list recurisvely", err)
		return results, err
	}

	tDeas := time.Now()
	unexpiredDeas, err := store.unexpiredDeas()
	store.logger.Debug(fmt.Sprintf("GETINSTANCEHEARTBEATS Unexpired Deas: %s", time.Since(tDeas)))
	if err != nil {
		store.logger.Error("GETINSTANCEHEARTBEATS Failed Unexpired Deas", err)
		return results, err
	}

	expiredKeys := []string{}
	tParse := time.Now()
	for _, actualNode := range node.ChildNodes {
		heartbeats, toDelete, err := store.heartbeatsForNode(actualNode, unexpiredDeas)
		if err != nil {
			store.logger.Error("GETINSTANCEHEARTBEATS Failed To Parse", err)
			return []models.InstanceHeartbeat{}, nil
		}
		results = append(results, heartbeats...)
		expiredKeys = append(expiredKeys, toDelete...)
	}
	store.logger.Debug(fmt.Sprintf("GETINSTANCEHEARTBEATS Parsing: %s", time.Since(tParse)))

	tDel := time.Now()
	err = store.adapter.Delete(expiredKeys...)
	store.logger.Debug(fmt.Sprintf("GETINSTANCEHEARTBEATS Deleting keys (%d): %s", len(expiredKeys), time.Since(tDel)))
	if err != nil {
		store.logger.Error("GETINSTANCEHEARTBEATS Failed To delete keys", err)
		return results, err
	}
	return results, nil
}

func (store *RealStore) GetInstanceHeartbeatsForApp(appGuid string, appVersion string) (results []models.InstanceHeartbeat, err error) {
	node, err := store.adapter.ListRecursively(store.SchemaRoot() + "/apps/actual/" + store.AppKey(appGuid, appVersion))
	if err == storeadapter.ErrorKeyNotFound {
		return []models.InstanceHeartbeat{}, nil
	} else if err != nil {
		return []models.InstanceHeartbeat{}, err
	}

	unexpiredDeas, err := store.unexpiredDeas()
	if err != nil {
		return results, err
	}

	results, expiredKeys, err := store.heartbeatsForNode(node, unexpiredDeas)
	if err != nil {
		return []models.InstanceHeartbeat{}, err
	}

	err = store.adapter.Delete(expiredKeys...)
	return results, err
}

func (store *RealStore) heartbeatsForNode(node storeadapter.StoreNode, unexpiredDeas map[string]bool) (results []models.InstanceHeartbeat, toDelete []string, err error) {
	for _, heartbeatNode := range node.ChildNodes {
		heartbeat, err := models.NewInstanceHeartbeatFromJSON(heartbeatNode.Value)
		if err != nil {
			return []models.InstanceHeartbeat{}, []string{}, err
		}

		_, deaIsPresent := unexpiredDeas[heartbeat.DeaGuid]

		if deaIsPresent {
			results = append(results, heartbeat)
		} else {
			toDelete = append(toDelete, node.Key)
		}
	}
	return results, toDelete, nil
}

func (store *RealStore) unexpiredDeas() (results map[string]bool, err error) {
	results = map[string]bool{}

	summaryNodes, err := store.adapter.ListRecursively(store.SchemaRoot() + "/dea-presence")
	if err == storeadapter.ErrorKeyNotFound {
		return results, nil
	} else if err != nil {
		return results, err
	}

	for _, deaPresenceNode := range summaryNodes.ChildNodes {
		results[string(deaPresenceNode.Value)] = true
	}

	return results, nil
}

func (store *RealStore) instanceHeartbeatStoreKey(appGuid string, appVersion string, instanceGuid string) string {
	return store.SchemaRoot() + "/apps/actual/" + store.AppKey(appGuid, appVersion) + "/" + instanceGuid
}

func (store *RealStore) deaPresenceNode(deaGuid string) storeadapter.StoreNode {
	return storeadapter.StoreNode{
		Key:   store.SchemaRoot() + "/dea-presence/" + deaGuid,
		Value: []byte(deaGuid),
		TTL:   store.config.HeartbeatTTL(),
	}
}

func (store *RealStore) storeNodeForInstanceHeartbeat(instanceHeartbeat models.InstanceHeartbeat) storeadapter.StoreNode {
	return storeadapter.StoreNode{
		Key:   store.instanceHeartbeatStoreKey(instanceHeartbeat.AppGuid, instanceHeartbeat.AppVersion, instanceHeartbeat.InstanceGuid),
		Value: instanceHeartbeat.ToJSON(),
	}
}
