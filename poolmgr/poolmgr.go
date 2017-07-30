/*
Copyright 2016 The Fission Authors.

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

package poolmgr

import (
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/dchest/uniuri"
	controllerclient "github.com/fission/fission/controller/client"
	catalogclientset "github.com/kubernetes-incubator/service-catalog/pkg/client/clientset_generated/clientset"
	"github.com/opentracing/opentracing-go"
	zipkin "github.com/openzipkin/zipkin-go-opentracing"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

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

func serveMetric() {
	// Expose the registered metrics via HTTP.
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(metricAddr, nil))
}

func initTracing(svcName string, port int) {
	collector, _ := zipkin.NewHTTPCollector(
		fmt.Sprintf("http://%s:9411/api/v1/spans", "zipkin.fission-tracing"))
	tracer, _ := zipkin.NewTracer(
		zipkin.NewRecorder(collector, false, fmt.Sprintf("%v:%v", svcName, port), svcName))
	opentracing.SetGlobalTracer(tracer)
}

func StartPoolmgr(controllerUrl string, namespace string, port int) error {
	controllerUrl = strings.TrimSuffix(controllerUrl, "/")
	controllerClient := controllerclient.MakeClient(controllerUrl)

	kubernetesClient, err := getKubernetesClient()
	if err != nil {
		log.Printf("Failed to get kubernetes client: %v", err)
		return err
	}

	catalogClient, err := getServiceCatalogClient()
	if err != nil {
		log.Printf("Failed to get service catalog client: %v", err)
		return err
	}

	initTracing("poolmgr", port)

	instanceId := uniuri.NewLen(8)
	cleanupOldPoolmgrResources(kubernetesClient, namespace, instanceId)

	fsCache := MakeFunctionServiceCache()
	gpm := MakeGenericPoolManager(controllerUrl, kubernetesClient, catalogClient, namespace, fsCache, instanceId)

	api := MakeAPI(gpm, controllerClient, fsCache)
	go api.Serve(port)
	go serveMetric()

	return nil
}
