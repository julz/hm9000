package stress_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"time"

	"github.com/cloudfoundry/hm9000/testhelpers/natsrunner"
	"github.com/cloudfoundry/hm9000/testhelpers/storerunner"
	"testing"
)

var natsRunner *natsrunner.NATSRunner
var etcdRunner storerunner.StoreRunner
var listener *exec.Cmd

func TestStress(t *testing.T) {
	cmd := exec.Command("go", "install", "github.com/cloudfoundry/hm9000")
	output, err := cmd.CombinedOutput()
	if err != nil {
		println("FAILED TO COMPILE HM9000")
		println(string(output))
		os.Exit(1)
	}

	natsRunner = natsrunner.NewNATSRunner(4222)
	natsRunner.Start()

	etcdRunner = storerunner.NewETCDClusterRunner(4001, 1)
	etcdRunner.Start()

	listener = exec.Command("hm9000", "listen", "--config=/Users/onsi/workspace/hm-workspace/src/github.com/cloudfoundry/hm9000/config/default_config.json")
	outPipe, _ := listener.StdoutPipe()
	errPipe, _ := listener.StderrPipe()

	go func() {
		io.Copy(os.Stdout, outPipe)
	}()

	go func() {
		io.Copy(os.Stdout, errPipe)
	}()

	listener.Start()
	time.Sleep(1 * time.Second)

	RegisterFailHandler(Fail)
	RunSpecs(t, "Stress Suite")

	stopAllThings()
}

func stopAllThings() {
	natsRunner.Stop()
	etcdRunner.Stop()
	listener.Process.Signal(os.Interrupt)
	listener.Wait()
}

func registerSignalHandler() {
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill)

		select {
		case <-c:
			stopAllThings()
			os.Exit(0)
		}
	}()
}
