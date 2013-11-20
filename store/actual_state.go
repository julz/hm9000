package store

import (
	"fmt"
	"github.com/cloudfoundry/hm9000/models"
	"github.com/cloudfoundry/hm9000/storeadapter"
	"strings"
	"sync"
	"time"
)

const format = "store" //"json"/"store"
const enableReadCache = true

func init() {
	fmt.Printf("STORE FORMAT: %s\n", format)
	fmt.Printf("ENABLE READ CACHE: %t\n", enableReadCache)
}

var readTime = 0.0
var saveTime = 0.0
var deleteTime = 0.0
var nHeartbeats = 0
var nRead = 0
var nSaved = 0
var nDeleted = 0
var statLock = &sync.Mutex{}

func (store *RealStore) fetchHeartbeatCache() (map[string]models.InstanceHeartbeat, error) {
	if enableReadCache {
		store.heartbeatCacheLock.Lock()
		defer store.heartbeatCacheLock.Unlock()

		//TODO: make this configurable
		if time.Since(store.lastHeartbeatCacheLookup).Seconds() > 20.0 {
			store.logger.Debug("Busting Heartbeat Cache")
			statT := time.Now()
			instanceHeartbeats, err := store.GetInstanceHeartbeats()
			statLock.Lock()
			readTime += time.Since(statT).Seconds()
			nRead += len(instanceHeartbeats)
			statLock.Unlock()

			if err != nil {
				return map[string]models.InstanceHeartbeat{}, err
			}

			store.heartbeatCache = map[string]models.InstanceHeartbeat{}
			for _, instanceHeartbeat := range instanceHeartbeats {
				store.heartbeatCache[instanceHeartbeat.InstanceGuid] = instanceHeartbeat
			}
			store.lastHeartbeatCacheLookup = time.Now()
		}

		return store.heartbeatCache, nil
	} else {
		statT := time.Now()
		instanceHeartbeats, err := store.GetInstanceHeartbeats()
		statLock.Lock()
		readTime += time.Since(statT).Seconds()
		nRead += len(instanceHeartbeats)
		statLock.Unlock()

		if err != nil {
			return map[string]models.InstanceHeartbeat{}, err
		}
		cache := map[string]models.InstanceHeartbeat{}
		for _, instanceHeartbeat := range instanceHeartbeats {
			cache[instanceHeartbeat.InstanceGuid] = instanceHeartbeat
		}
		return cache, nil
	}
}

func (store *RealStore) SyncHeartbeats(incomingHeartbeats ...models.Heartbeat) error {
	t := time.Now()

	store.logger.Debug("Syncing heartbeat")

	tFetch := time.Now()
	cache, err := store.fetchHeartbeatCache()
	dtFetch := time.Since(tFetch).Seconds()
	if err != nil {
		store.logger.Error("Failed to fetch heartbeat cache when syncinc heartbeat", err, map[string]string{
			"Duration": fmt.Sprintf("%.4f", dtFetch),
		})
		return err
	}

	nodesToSave := []storeadapter.StoreNode{}
	keysToDelete := []string{}
	cacheKeysToDelete := []string{}
	incomingInstanceGuids := map[string]bool{}
	numberOfItems := 0

	tBookkeeping := time.Now()
	if enableReadCache {
		store.heartbeatCacheLock.Lock()
	}

	for _, incomingHeartbeat := range incomingHeartbeats {
		numberOfItems += len(incomingHeartbeat.InstanceHeartbeats)
		nodesToSave = append(nodesToSave, store.deaPresenceNode(incomingHeartbeat.DeaGuid))
		for _, incomingInstanceHeartbeat := range incomingHeartbeat.InstanceHeartbeats {
			incomingInstanceGuids[incomingInstanceHeartbeat.InstanceGuid] = true
			existingInstanceHeartbeat, found := cache[incomingInstanceHeartbeat.InstanceGuid]

			if found && existingInstanceHeartbeat.State == incomingInstanceHeartbeat.State {
				continue
			}

			cache[incomingInstanceHeartbeat.InstanceGuid] = incomingInstanceHeartbeat
			nodesToSave = append(nodesToSave, store.storeNodeForInstanceHeartbeat(incomingInstanceHeartbeat))
		}

		for _, existingInstanceHeartbeat := range cache {
			if existingInstanceHeartbeat.DeaGuid != incomingHeartbeat.DeaGuid {
				continue
			}
			if incomingInstanceGuids[existingInstanceHeartbeat.InstanceGuid] {
				continue
			}

			key := store.instanceHeartbeatStoreKey(existingInstanceHeartbeat.AppGuid, existingInstanceHeartbeat.AppVersion, existingInstanceHeartbeat.InstanceGuid)
			keysToDelete = append(keysToDelete, key)
			cacheKeysToDelete = append(cacheKeysToDelete, existingInstanceHeartbeat.InstanceGuid)
		}

		for _, cacheKeyToDelete := range cacheKeysToDelete {
			delete(cache, cacheKeyToDelete)
		}
	}

	if enableReadCache {
		store.heartbeatCacheLock.Unlock()
	}
	dtBookkeeping := time.Since(tBookkeeping).Seconds()

	tSave := time.Now()
	err = store.adapter.Set(nodesToSave)
	dtSave := time.Since(tSave).Seconds()
	statLock.Lock()
	saveTime += dtSave
	nHeartbeats += len(incomingHeartbeats)
	nSaved += len(nodesToSave)
	statLock.Unlock()

	if err != nil {
		store.logger.Error("Failed to save when syncing heartbeat", err, map[string]string{
			"Sync Duration": fmt.Sprintf("%.4f", time.Since(t).Seconds()),
			"Save Duration": fmt.Sprintf("%.4f", dtSave),
		})
		return err
	}

	tDelete := time.Now()
	err = store.adapter.Delete(keysToDelete...)
	dtDelete := time.Since(tDelete).Seconds()
	statLock.Lock()
	deleteTime += dtDelete
	nDeleted += len(keysToDelete)
	statLock.Unlock()

	if err == storeadapter.ErrorKeyNotFound {
		store.logger.Error("Tried to delete a missing key when syncing heartbeat: soldiering on", err)
	} else if err != nil {
		store.logger.Error("Failed to delete when syncing heartbeat: bailing out", err, map[string]string{
			"Sync Duration":   fmt.Sprintf("%.4f", time.Since(t).Seconds()),
			"Save Duration":   fmt.Sprintf("%.4f", dtSave),
			"Delete Duration": fmt.Sprintf("%.4f", dtDelete),
		})
		return err
	}

	statLock.Lock()
	fmt.Printf(`
~~~~~~~~~~~~~~~~~~~~~~~~ Stats:
~~~~~~~~~~~~~~~~~~~~~~~~ Read:%.4f (%d)
~~~~~~~~~~~~~~~~~~~~~~~~ Write:%.4f (%d)
~~~~~~~~~~~~~~~~~~~~~~~~ Delete:%.4f (%d)
~~~~~~~~~~~~~~~~~~~~~~~~ Heartbeats:%d
`, readTime, nRead, saveTime, nSaved, deleteTime, nDeleted, nHeartbeats)
	statLock.Unlock()

	store.logger.Debug(fmt.Sprintf("Store.SyncHeartbeat"), map[string]string{
		"Number of Heartbeats":    fmt.Sprintf("%d", len(incomingHeartbeats)),
		"Number of Items":         fmt.Sprintf("%d", numberOfItems),
		"Number of Items Saved":   fmt.Sprintf("%d", len(nodesToSave)),
		"Number of Items Deleted": fmt.Sprintf("%d", len(keysToDelete)),
		"Sync Duration":           fmt.Sprintf("%.4f seconds", time.Since(t).Seconds()),
		"Bookkeeping Duration":    fmt.Sprintf("%.4f seconds", dtBookkeeping),
		"Fetch Duration":          fmt.Sprintf("%.4f seconds", dtFetch),
		"Save Duration":           fmt.Sprintf("%.4f seconds", dtSave),
		"Delete Duration":         fmt.Sprintf("%.4f seconds", dtDelete),
	})

	return nil
}

func (store *RealStore) SyncHeartbeat(incomingHeartbeat models.Heartbeat) error {
	t := time.Now()

	store.logger.Debug("Syncing heartbeat")

	tFetch := time.Now()
	cache, err := store.fetchHeartbeatCache()
	dtFetch := time.Since(tFetch).Seconds()
	if err != nil {
		store.logger.Error("Failed to fetch heartbeat cache when syncinc heartbeat", err, map[string]string{
			"Duration": fmt.Sprintf("%.4f", dtFetch),
		})
		return err
	}

	nodesToSave := []storeadapter.StoreNode{
		store.deaPresenceNode(incomingHeartbeat.DeaGuid),
	}
	keysToDelete := []string{}
	cacheKeysToDelete := []string{}
	incomingInstanceGuids := map[string]bool{}

	tBookkeeping := time.Now()
	if enableReadCache {
		store.heartbeatCacheLock.Lock()
	}

	for _, incomingInstanceHeartbeat := range incomingHeartbeat.InstanceHeartbeats {
		incomingInstanceGuids[incomingInstanceHeartbeat.InstanceGuid] = true
		existingInstanceHeartbeat, found := cache[incomingInstanceHeartbeat.InstanceGuid]

		if found && existingInstanceHeartbeat.State == incomingInstanceHeartbeat.State {
			continue
		}

		cache[incomingInstanceHeartbeat.InstanceGuid] = incomingInstanceHeartbeat
		nodesToSave = append(nodesToSave, store.storeNodeForInstanceHeartbeat(incomingInstanceHeartbeat))
	}

	for _, existingInstanceHeartbeat := range cache {
		if existingInstanceHeartbeat.DeaGuid != incomingHeartbeat.DeaGuid {
			continue
		}
		if incomingInstanceGuids[existingInstanceHeartbeat.InstanceGuid] {
			continue
		}

		key := store.instanceHeartbeatStoreKey(existingInstanceHeartbeat.AppGuid, existingInstanceHeartbeat.AppVersion, existingInstanceHeartbeat.InstanceGuid)
		keysToDelete = append(keysToDelete, key)
		cacheKeysToDelete = append(cacheKeysToDelete, existingInstanceHeartbeat.InstanceGuid)
	}

	for _, cacheKeyToDelete := range cacheKeysToDelete {
		delete(cache, cacheKeyToDelete)
	}

	if enableReadCache {
		store.heartbeatCacheLock.Unlock()
	}
	dtBookkeeping := time.Since(tBookkeeping).Seconds()

	tSave := time.Now()
	err = store.adapter.Set(nodesToSave)
	dtSave := time.Since(tSave).Seconds()
	statLock.Lock()
	saveTime += dtSave
	nHeartbeats += 1
	nSaved += len(nodesToSave)
	statLock.Unlock()

	if err != nil {
		store.logger.Error("Failed to save when syncing heartbeat", err, map[string]string{
			"Sync Duration": fmt.Sprintf("%.4f", time.Since(t).Seconds()),
			"Save Duration": fmt.Sprintf("%.4f", dtSave),
		})
		return err
	}

	tDelete := time.Now()
	err = store.adapter.Delete(keysToDelete...)
	dtDelete := time.Since(tDelete).Seconds()
	statLock.Lock()
	deleteTime += dtDelete
	nDeleted += len(keysToDelete)
	statLock.Unlock()

	if err == storeadapter.ErrorKeyNotFound {
		store.logger.Error("Tried to delete a missing key when syncing heartbeat: soldiering on", err)
	} else if err != nil {
		store.logger.Error("Failed to delete when syncing heartbeat: bailing out", err, map[string]string{
			"Sync Duration":   fmt.Sprintf("%.4f", time.Since(t).Seconds()),
			"Save Duration":   fmt.Sprintf("%.4f", dtSave),
			"Delete Duration": fmt.Sprintf("%.4f", dtDelete),
		})
		return err
	}

	statLock.Lock()
	fmt.Printf(`
~~~~~~~~~~~~~~~~~~~~~~~~ Stats:
~~~~~~~~~~~~~~~~~~~~~~~~ Read:%.4f (%d)
~~~~~~~~~~~~~~~~~~~~~~~~ Write:%.4f (%d)
~~~~~~~~~~~~~~~~~~~~~~~~ Delete:%.4f (%d)
~~~~~~~~~~~~~~~~~~~~~~~~ Heartbeats:%d
`, readTime, nRead, saveTime, nSaved, deleteTime, nDeleted, nHeartbeats)
	statLock.Unlock()

	store.logger.Debug(fmt.Sprintf("Store.SyncHeartbeat"), map[string]string{
		"Number of Items":         fmt.Sprintf("%d", len(incomingHeartbeat.InstanceHeartbeats)),
		"Number of Items Saved":   fmt.Sprintf("%d", len(nodesToSave)),
		"Number of Items Deleted": fmt.Sprintf("%d", len(keysToDelete)),
		"Sync Duration":           fmt.Sprintf("%.4f seconds", time.Since(t).Seconds()),
		"Bookkeeping Duration":    fmt.Sprintf("%.4f seconds", dtBookkeeping),
		"Fetch Duration":          fmt.Sprintf("%.4f seconds", dtFetch),
		"Save Duration":           fmt.Sprintf("%.4f seconds", dtSave),
		"Delete Duration":         fmt.Sprintf("%.4f seconds", dtDelete),
	})

	return nil
}

func (store *RealStore) GetInstanceHeartbeats() (results []models.InstanceHeartbeat, err error) {
	node, err := store.adapter.ListRecursively(store.SchemaRoot() + "/apps/actual")
	if err == storeadapter.ErrorKeyNotFound {
		return results, nil
	} else if err != nil {
		store.logger.Error("Store.GetInstanceHeartbeats() failed to list apps", err)
		return results, err
	}

	unexpiredDeas, err := store.unexpiredDeas()
	if err != nil {
		store.logger.Error("Store.GetInstanceHeartbeats() failed to fetch unexpired DEAs", err)
		return results, err
	}

	expiredKeys := []string{}
	for _, actualNode := range node.ChildNodes {
		heartbeats, toDelete, err := store.heartbeatsForNode(actualNode, unexpiredDeas)
		if err != nil {
			store.logger.Error("Store.GetInstanceHeartbeats() Failed To Parse", err)
			return []models.InstanceHeartbeat{}, nil
		}
		results = append(results, heartbeats...)
		expiredKeys = append(expiredKeys, toDelete...)
	}

	err = store.adapter.Delete(expiredKeys...)
	if err == storeadapter.ErrorKeyNotFound {
		store.logger.Error("Store.GetInstanceHeartbeats() Failed To Delete a Missing Key: soldiering on", err)
	} else if err != nil {
		store.logger.Error("Store.GetInstanceHeartbeats() Failed To Delete Keys: bailing out", err)
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
		var err error
		var heartbeat models.InstanceHeartbeat

		if format == "store" {
			keyComponents := strings.Split(heartbeatNode.Key, "/")
			instanceGuid := keyComponents[len(keyComponents)-1]
			appGuidVersion := strings.Split(keyComponents[len(keyComponents)-2], "|")
			heartbeat, err = models.NewInstanceHeartbeatFromStoreFormat(appGuidVersion[0], appGuidVersion[1], instanceGuid, heartbeatNode.Value)
		} else {
			heartbeat, err = models.NewInstanceHeartbeatFromJSON(heartbeatNode.Value)
		}

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
	if format == "store" {
		return storeadapter.StoreNode{
			Key:   store.instanceHeartbeatStoreKey(instanceHeartbeat.AppGuid, instanceHeartbeat.AppVersion, instanceHeartbeat.InstanceGuid),
			Value: instanceHeartbeat.ToStoreFormat(),
		}
	} else {
		return storeadapter.StoreNode{
			Key:   store.instanceHeartbeatStoreKey(instanceHeartbeat.AppGuid, instanceHeartbeat.AppVersion, instanceHeartbeat.InstanceGuid),
			Value: instanceHeartbeat.ToJSON(),
		}
	}
}
