package service_adapter

import (
	"log"
	"time"

	controllerClient "github.com/fission/fission/controller/client"
)

type (
	AdapterSync struct {
		controller *controllerClient.Client
		manager    *AdapterManager
	}
)

func MakeAdapterSync(controller *controllerClient.Client, manager *AdapterManager) *AdapterSync {
	as := &AdapterSync{
		controller: controller,
		manager:    manager,
	}
	go as.syncSvc()
	return as
}

func (as *AdapterSync) syncSvc() {
	failureCount := 0
	maxFailures := 6
	for {
		adapters, err := as.controller.ServiceAdapterList()
		if err != nil {
			failureCount++
			if failureCount > maxFailures {
				log.Fatalf("Failed to connect to controller: %v", err)
			}
			time.Sleep(10 * time.Second)
			continue
		}
		as.manager.Sync(adapters)
		time.Sleep(3 * time.Second)
	}
}
