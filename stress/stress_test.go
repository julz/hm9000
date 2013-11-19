package stress_test

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"strings"
	"time"
)

var _ = Describe("Stress", func() {
	It("should survive", func() {
		natsStreamByte, err := ioutil.ReadFile("stream")
		Ω(err).ShouldNot(HaveOccured())
		natsStream := string(natsStreamByte)
		timeAndHeartbeats := strings.Split(natsStream, "\n")

		times := []time.Time{}
		heartbeats := []string{}
		for _, tAndH := range timeAndHeartbeats {
			twoThings := strings.Split(tAndH, "Received on [dea.heartbeat] :")
			heartbeats = append(heartbeats, twoThings[1][2:len(twoThings[1])-1])
			date, err := time.Parse("[2006-1-2 15:04:05 -0700", strings.Split(twoThings[0], "] [")[0])
			Ω(err).ShouldNot(HaveOccured())
			times = append(times, date)
		}

		secondsFromBeginning := []int{}
		for _, timeEntry := range times {
			secondsFromBeginning = append(secondsFromBeginning, int(timeEntry.Sub(times[0]).Seconds()))
		}
		fmt.Printf("%#v", secondsFromBeginning)

		now := time.Now()
		lastIndex := 0
		for {
			currentSeconds := int(time.Since(now).Seconds())
			for i := lastIndex; i < len(secondsFromBeginning); i++ {
				if currentSeconds > secondsFromBeginning[i] {
					fmt.Printf("SENDING %d\n, %d", i, currentSeconds)
					natsRunner.MessageBus.Publish("dea.heartbeat", heartbeats[i])
					lastIndex = i + 1
				}
			}
			time.Sleep(1000 * time.Millisecond)
			if lastIndex >= len(secondsFromBeginning) {
				break
			}
		}
	})
})
