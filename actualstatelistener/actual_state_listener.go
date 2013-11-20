package actualstatelistener

import (
	"fmt"
	"github.com/cloudfoundry/hm9000/config"
	"github.com/cloudfoundry/hm9000/helpers/logger"
	"github.com/cloudfoundry/hm9000/helpers/metricsaccountant"
	"github.com/cloudfoundry/hm9000/helpers/timeprovider"
	"github.com/cloudfoundry/hm9000/models"
	"github.com/cloudfoundry/hm9000/store"
	"sync"
	"time"

	"github.com/cloudfoundry/yagnats"
)

const batchedSaves = true

func init() {
	fmt.Printf("BATCHED SAVES: %t\n", batchedSaves)
}

type ActualStateListener struct {
	logger            logger.Logger
	config            config.Config
	messageBus        yagnats.NATSClient
	store             store.Store
	timeProvider      timeprovider.TimeProvider
	storeUsageTracker metricsaccountant.UsageTracker
	metricsAccountant metricsaccountant.MetricsAccountant
	heartbeatsToSave  []models.Heartbeat
	syncMutex         *sync.Mutex
	dt                time.Duration
	n                 int
}

func New(config config.Config,
	messageBus yagnats.NATSClient,
	store store.Store,
	storeUsageTracker metricsaccountant.UsageTracker,
	metricsAccountant metricsaccountant.MetricsAccountant,
	timeProvider timeprovider.TimeProvider,
	logger logger.Logger) *ActualStateListener {

	return &ActualStateListener{
		logger:            logger,
		config:            config,
		messageBus:        messageBus,
		store:             store,
		storeUsageTracker: storeUsageTracker,
		metricsAccountant: metricsAccountant,
		timeProvider:      timeProvider,
		heartbeatsToSave:  []models.Heartbeat{},
		syncMutex:         &sync.Mutex{},
	}
}

func (listener *ActualStateListener) Start() {
	listener.messageBus.Subscribe("dea.advertise", func(message *yagnats.Message) {
		listener.bumpFreshness()
		listener.logger.Debug("Received dea.advertise")
	})

	if batchedSaves {
		listener.messageBus.Subscribe("dea.heartbeat", func(message *yagnats.Message) {
			listener.logger.Debug("Got a heartbeat")
			heartbeat, err := models.NewHeartbeatFromJSON([]byte(message.Payload))
			if err != nil {
				listener.logger.Error("Could not unmarshal heartbeat", err,
					map[string]string{
						"MessageBody": message.Payload,
					})
				return
			}

			listener.syncMutex.Lock()
			listener.heartbeatsToSave = append(listener.heartbeatsToSave, heartbeat)
			listener.syncMutex.Unlock()
		})

		listener.syncHeartbeats()
	} else {
		dt := time.Duration(0)
		n := 0
		tmutex := &sync.Mutex{}

		listener.messageBus.Subscribe("dea.heartbeat", func(message *yagnats.Message) {
			t := time.Now()
			listener.logger.Debug("Got a heartbeat")
			heartbeat, err := models.NewHeartbeatFromJSON([]byte(message.Payload))
			if err != nil {
				listener.logger.Error("Could not unmarshal heartbeat", err,
					map[string]string{
						"MessageBody": message.Payload,
					})
				return
			}

			err = listener.store.SyncHeartbeat(heartbeat)
			if err != nil {
				listener.logger.Error("Could not put heartbeat in store:", err, heartbeat.LogDescription())
				return
			}

			listener.logger.Info("Synced a Heartbeat", heartbeat.LogDescription(), map[string]string{"Duration": time.Since(t).String()})
			listener.bumpFreshness()
			listener.logger.Debug("Received dea.heartbeat") //Leave this here: the integration test uses this to ensure the heartbeat has been processed
			tmutex.Lock()
			dt += time.Since(t)
			n += 1
			fmt.Printf("~~~~~~~~~~~~~~~~~~~~~~~~ TOTAL TIME %s (%d)\n", dt, n)
			tmutex.Unlock()
		})
	}

	if listener.storeUsageTracker != nil {
		listener.storeUsageTracker.StartTrackingUsage()
		listener.measureStoreUsage()
	}
}

func (listener *ActualStateListener) syncHeartbeats() {
	t := time.Now()
	listener.syncMutex.Lock()
	heartbeatsToSave := listener.heartbeatsToSave
	listener.heartbeatsToSave = []models.Heartbeat{}
	listener.syncMutex.Unlock()

	if len(heartbeatsToSave) > 0 {
		err := listener.store.SyncHeartbeats(heartbeatsToSave...)
		if err != nil {
			listener.logger.Error("Could not put heartbeats in store:", err)
			return
		}

		listener.logger.Info("Synced Heartbeats", map[string]string{"Duration": time.Since(t).String()})
		listener.bumpFreshness()
		listener.logger.Debug("Received dea.heartbeat") //Leave this here: the integration test uses this to ensure the heartbeat has been processed
		listener.dt += time.Since(t)
		listener.n += len(heartbeatsToSave)
		fmt.Printf("~~~~~~~~~~~~~~~~~~~~~~~~ TOTAL TIME %s (%d)\n", listener.dt, listener.n)
	}

	time.AfterFunc(1*time.Second, func() {
		listener.syncHeartbeats()
	})
}

func (listener *ActualStateListener) measureStoreUsage() {
	usage, _ := listener.storeUsageTracker.MeasureUsage()
	listener.metricsAccountant.TrackActualStateListenerStoreUsageFraction(usage)
	listener.logger.Info("Usage", map[string]string{"Usage": fmt.Sprintf("%f%%", usage*100.0)})

	time.AfterFunc(3*time.Duration(listener.config.HeartbeatPeriod)*time.Second, func() {
		listener.measureStoreUsage()
	})
}

func (listener *ActualStateListener) bumpFreshness() {
	err := listener.store.BumpActualFreshness(listener.timeProvider.Time())
	if err != nil {
		listener.logger.Error("Could not update actual freshness", err)
	} else {
		listener.logger.Info("Bumped freshness")
	}
}
