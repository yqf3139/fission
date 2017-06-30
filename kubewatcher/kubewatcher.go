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

package kubewatcher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"k8s.io/client-go/kubernetes"
	"github.com/fission/fission"
	"github.com/fission/fission/publisher"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/errors"
)

type requestType int

const (
	SYNC requestType = iota
)

type (
	KubeWatcher struct {
		watches          map[string]watchSubscription
		kubernetesClient *kubernetes.Clientset
		requestChannel   chan *kubeWatcherRequest
		publisher        publisher.Publisher
		routerUrl        string
	}

	watchSubscription struct {
		fission.Watch
		kubeWatch           watch.Interface
		lastResourceVersion string
		stopped             *int32
		kubernetesClient    *kubernetes.Clientset
		publisher           publisher.Publisher
	}

	kubeWatcherRequest struct {
		requestType
		watches         []fission.Watch
		responseChannel chan *kubeWatcherResponse
	}
	kubeWatcherResponse struct {
		error
	}
)

func MakeKubeWatcher(kubernetesClient *kubernetes.Clientset, publisher publisher.Publisher) *KubeWatcher {
	kw := &KubeWatcher{
		watches:          make(map[string]watchSubscription),
		kubernetesClient: kubernetesClient,
		publisher:        publisher,
		requestChannel:   make(chan *kubeWatcherRequest),
	}
	go kw.svc()
	return kw
}

func (kw *KubeWatcher) Sync(watches []fission.Watch) error {
	req := &kubeWatcherRequest{
		requestType:     SYNC,
		watches:         watches,
		responseChannel: make(chan *kubeWatcherResponse),
	}
	kw.requestChannel <- req
	resp := <-req.responseChannel
	return resp.error
}

func (kw *KubeWatcher) svc() {
	for {
		req := <-kw.requestChannel
		switch req.requestType {
		case SYNC:
			newWatchUids := make(map[string]bool)
			for _, w := range req.watches {
				newWatchUids[w.Metadata.Uid] = true
			}
			// Remove old watches
			for uid, ws := range kw.watches {
				if _, ok := newWatchUids[uid]; !ok {
					kw.removeWatch(&ws.Watch)
				}
			}
			// Add new watches
			for _, w := range req.watches {
				if _, ok := kw.watches[w.Metadata.Uid]; !ok {
					kw.addWatch(&w)
				}
			}
			req.responseChannel <- &kubeWatcherResponse{error: nil}
		}
	}
}

// TODO lifted from kubernetes/pkg/kubectl/resource_printer.go.
func printKubernetesObject(obj runtime.Object, w io.Writer) error {
	switch obj := obj.(type) {
	case *runtime.Unknown:
		var buf bytes.Buffer
		err := json.Indent(&buf, obj.Raw, "", "    ")
		if err != nil {
			return err
		}
		buf.WriteRune('\n')
		_, err = buf.WriteTo(w)
		return err
	}

	data, err := json.MarshalIndent(obj, "", "    ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = w.Write(data)
	return err
}

func createKubernetesWatch(kubeClient *kubernetes.Clientset, w *fission.Watch, resourceVersion string) (watch.Interface, error) {
	var wi watch.Interface
	var err error
	var watchTimeoutSec int64 = 120

	// TODO populate labelselector and fieldselector
	listOptions := v1.ListOptions{
		ResourceVersion: resourceVersion,
		TimeoutSeconds:  &watchTimeoutSec,
	}

	// TODO handle the full list of types
	switch strings.ToUpper(w.ObjType) {
	case "POD":
		wi, err = kubeClient.CoreV1().Pods(w.Namespace).Watch(listOptions)
	case "SERVICE":
		wi, err = kubeClient.CoreV1().Services(w.Namespace).Watch(listOptions)
	case "REPLICATIONCONTROLLER":
		wi, err = kubeClient.CoreV1().ReplicationControllers(w.Namespace).Watch(listOptions)
	case "JOB":
		wi, err = kubeClient.BatchV1().Jobs(w.Namespace).Watch(listOptions)
	default:
		msg := fmt.Sprintf("Error: unknown obj type '%v'", w.ObjType)
		log.Println(msg)
		err = errors.NewBadRequest(msg)
	}
	return wi, err
}

func (kw *KubeWatcher) addWatch(w *fission.Watch) error {
	log.Printf("Adding watch %v: %v", w.Metadata.Name, w.Function.Name)
	ws, err := MakeWatchSubscription(w, kw.kubernetesClient, kw.publisher)
	if err != nil {
		return err
	}
	kw.watches[w.Metadata.Uid] = *ws
	return nil
}

func (kw *KubeWatcher) removeWatch(w *fission.Watch) error {
	log.Printf("Removing watch %v: %v", w.Metadata.Name, w.Function.Name)
	ws, ok := kw.watches[w.Metadata.Uid]
	if !ok {
		return fission.MakeError(fission.ErrorNotFound,
			fmt.Sprintf("watch doesn't exist: %v", w.Metadata))
	}
	delete(kw.watches, w.Metadata.Uid)
	atomic.StoreInt32(ws.stopped, 1)
	ws.kubeWatch.Stop()
	return nil
}

// 	wi, err := kw.createKubernetesWatch(w)
// 	if err != nil {
// 		return err
// 	}
// 	var stopped int32 = 0
// 	ws := &watchSubscription{
// 		Watch:     *w,
// 		kubeWatch: wi,
// 		stopped:   &stopped,
// 	}
// 	kw.watches[w.Metadata.Uid] = *ws
// 	go ws.eventDispatchLoop(kw.publisher)
// 	return nil
// }

func MakeWatchSubscription(w *fission.Watch, kubeClient *kubernetes.Clientset, publisher publisher.Publisher) (*watchSubscription, error) {
	var stopped int32 = 0
	ws := &watchSubscription{
		Watch:               *w,
		kubeWatch:           nil,
		stopped:             &stopped,
		kubernetesClient:    kubeClient,
		publisher:           publisher,
		lastResourceVersion: "",
	}

	err := ws.restartWatch()
	if err != nil {
		return nil, err
	}

	go ws.eventDispatchLoop()
	return ws, nil
}

func (ws *watchSubscription) restartWatch() error {
	retries := 60
	for {
		log.Printf("(re)starting watch %v (ns:%v type:%v) at rv:%v",
			ws.Watch.Metadata, ws.Watch.Namespace, ws.Watch.ObjType, ws.lastResourceVersion)
		wi, err := createKubernetesWatch(ws.kubernetesClient, &ws.Watch, ws.lastResourceVersion)
		if err != nil {
			retries--
			if retries > 0 {
				time.Sleep(500 * time.Millisecond)
				continue
			} else {
				return err
			}
		}
		ws.kubeWatch = wi
		return nil
	}
}

func getResourceVersion(obj runtime.Object) (string, error) {
	m, err := meta.Accessor(obj)
	if err != nil {
		return "", err
	}
	return m.GetResourceVersion(), nil
}

func (ws *watchSubscription) eventDispatchLoop() {
	log.Println("Listening to watch ", ws.Watch.Metadata.Name)
	for {
		for {
			ev, more := <-ws.kubeWatch.ResultChan()
			if !more {
				log.Println("Watch stopped", ws.Watch.Metadata.Name)
				break
			}
			if ev.Type == watch.Error {
				e := errors.FromObject(ev.Object)
				log.Printf("Watch error, retrying in a second: %v", e)
				// Start from the beginning to get around "too old resource version"
				ws.lastResourceVersion = ""
				time.Sleep(time.Second)
				break
			}
			rv, err := getResourceVersion(ev.Object)
			if err != nil {
				log.Printf("Error getting resourceVersion from object: %v", err)
			} else {
				log.Printf("rv=%v", rv)
				ws.lastResourceVersion = rv
			}

			// Serialize the object
			var buf bytes.Buffer
			err = printKubernetesObject(ev.Object, &buf)
			if err != nil {
				log.Printf("Failed to serialize object: %v", err)
				// TODO send a POST request indicating error
			}

			// Event and object type aren't in the serialized object
			headers := map[string]string{
				"Content-Type":             "application/json",
				"X-Kubernetes-Event-Type":  string(ev.Type),
				"X-Kubernetes-Object-Type": reflect.TypeOf(ev.Object).Elem().Name(),
			}
			// Event and object type aren't in the serialized object
			ws.publisher.Publish(buf.String(), headers, ws.Watch.Target)
		}
		if atomic.LoadInt32(ws.stopped) == 0 {
			err := ws.restartWatch()
			if err != nil {
				log.Panicf("Failed to restart watch: %v", err)
			}
		}
	}
}
