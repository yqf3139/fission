/*
Copyright 2017 The Fission Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package service_adapter

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	catalogclientset "github.com/kubernetes-incubator/service-catalog/pkg/client/clientset_generated/clientset"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	controllerClient "github.com/fission/fission/controller/client"
)

// Get a service catalog client
func getServiceCatalogClient() (*catalogclientset.Clientset, error) {
	config := &rest.Config{
		Host: "http://catalog-catalog-apiserver.catalog",
	}

	// creates the clientset
	clientset, err := catalogclientset.NewForConfig(config)
	if err != nil {
		log.Printf("Error getting service catalog client: %v", err)
		return nil, err
	}

	return clientset, nil
}

// Get a kubernetes client using the pod's service account.  This only
// works when we're running inside a kubernetes cluster.
func getKubernetesClient() (*kubernetes.Clientset, error) {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Printf("Error getting kubernetes client config: %v", err)
		return nil, err
	}

	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Printf("Error getting kubernetes client: %v", err)
		return nil, err
	}

	return clientset, nil
}

func Start(controllerUrl string, routerUrl string) error {
	controller := controllerClient.MakeClient(controllerUrl)
	catalogClient, err := getServiceCatalogClient()
	if err != nil {
		log.Printf("Failed to get service catalog client: %v", err)
		return err
	}
	kubernetesClient, err := getKubernetesClient()
	if err != nil {
		log.Printf("Failed to get kubernetes client: %v", err)
		return err
	}

	natsService := MakeNatsService("fissionMQTrigger", "fissionAdapter",
		"nats://nats-streaming.fission:4222")
	minioAdapter := &MinioAdapterFactory{
		natsService: natsService,
	}
	rethinkDBAdapter := MakeRethinkDBAdapterFactory(natsService)

	manager := MakeAdapterManager(map[string]AdapterFactory{
		"minio":     minioAdapter,
		"rethinkdb": rethinkDBAdapter,
	}, catalogClient, kubernetesClient)

	MakeAdapterSync(controller, manager)

	// endpoint for minio webhook
	r := mux.NewRouter()
	r.HandleFunc("/minio/{id}", minioAdapter.eventHandler).Methods("POST")

	port := "8888"
	address := fmt.Sprintf(":%v", port)
	log.Printf("starting adapter at port %v", port)
	log.Fatal(http.ListenAndServe(address, handlers.LoggingHandler(os.Stdout, r)))
	return nil
}
