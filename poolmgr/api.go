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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/opentracing/opentracing-go"

	"github.com/fission/fission"
	"github.com/fission/fission/cache"
	controllerclient "github.com/fission/fission/controller/client"
)

type createFuncServiceRequest struct {
	funcMeta *fission.Metadata
	respChan chan *createFuncServiceResponse
	parent   opentracing.Span
}
type createFuncServiceResponse struct {
	address string
	err     error
}

type API struct {
	poolMgr          *GenericPoolManager
	functionEnv      *cache.Cache // map[fission.Metadata]fission.Environment
	function         *cache.Cache // map[fission.Metadata]fission.Function
	fsCache          *functionServiceCache
	controller       *controllerclient.Client
	fsCreateChannels map[fission.Metadata]*sync.WaitGroup
	requestChan      chan *createFuncServiceRequest
}

func MakeAPI(gpm *GenericPoolManager, controller *controllerclient.Client, fsCache *functionServiceCache) *API {
	api := API{
		poolMgr:          gpm,
		functionEnv:      cache.MakeCache(time.Minute, 0),
		function:         cache.MakeCache(time.Minute, 0),
		fsCache:          fsCache,
		controller:       controller,
		fsCreateChannels: make(map[fission.Metadata]*sync.WaitGroup),
		requestChan:      make(chan *createFuncServiceRequest),
	}
	go api.serveCreateFuncServices()
	return &api
}

// All non-cached function service requests go through this goroutine
// serially. It parallelizes requests for different functions, and
// ensures that for a given function, only one request causes a pod to
// get specialized. In other words, it ensures that when there's an
// ongoing request for a certain function, all other requests wait for
// that request to complete.
func (api *API) serveCreateFuncServices() {
	for {
		req := <-api.requestChan
		m := req.funcMeta
		parent := req.parent

		// Cache miss -- is this first one to request the func?
		wg, found := api.fsCreateChannels[*m]
		if !found {
			// create a waitgroup for other requests for
			// the same function to wait on
			wg := &sync.WaitGroup{}
			wg.Add(1)
			api.fsCreateChannels[*m] = wg

			// launch a goroutine for each request, to parallelize
			// the specialization of different functions
			go func() {
				address, err := api.createServiceForFunction(m, parent)
				req.respChan <- &createFuncServiceResponse{
					address: address,
					err:     err,
				}
				delete(api.fsCreateChannels, *m)
				wg.Done()
			}()
		} else {
			// There's an existing request for this function, wait for it to finish
			go func() {
				log.Printf("Waiting for concurrent request for the same function: %v", m)
				wg.Wait()

				// get the function service from the cache
				fsvc, err := api.fsCache.GetByFunction(m)
				address := ""
				if err == nil {
					address = fsvc.getAddress()
				}
				req.respChan <- &createFuncServiceResponse{
					address: address,
					err:     err,
				}
			}()
		}
	}
}

func startTracingFromHttpHeader(opName string, r *http.Request) opentracing.Span {
	var sp opentracing.Span
	wireContext, err := opentracing.GlobalTracer().Extract(
		opentracing.TextMap,
		opentracing.HTTPHeadersCarrier(r.Header))
	if err != nil {
		// If for whatever reason we can't join, go ahead an start a new root span.
		sp = opentracing.StartSpan(opName)
	} else {
		sp = opentracing.StartSpan(opName, opentracing.ChildOf(wireContext))
	}
	return sp
}

func (api *API) getServiceForFunctionApi(w http.ResponseWriter, r *http.Request) {
	sp := startTracingFromHttpHeader("poolmgr::GetServiceForFunctionApi", r)
	defer sp.Finish()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request", 500)
		return
	}

	// get function metadata
	m := fission.Metadata{}
	err = json.Unmarshal(body, &m)
	if err != nil {
		http.Error(w, "Failed to parse request", 400)
		return
	}

	serviceName, err := api.getServiceForFunction(&m, sp)
	if err != nil {
		code, msg := fission.GetHTTPError(err)
		log.Printf("Error: %v: %v", code, msg)
		http.Error(w, msg, code)
	}

	w.Write([]byte(serviceName))
}

func (api *API) getFunctionInfo(m *fission.Metadata) (*fission.Environment, *fission.Function, error) {
	// Cached ?
	e, err := api.functionEnv.Get(*m)
	f, err := api.function.Get(*m)
	if err == nil {
		return e.(*fission.Environment), f.(*fission.Function), nil
	}

	// Cache miss -- get func from controller
	log.Printf("[%v] getting function from controller", m)
	fnc, err := api.controller.FunctionGet(m)
	if err != nil {
		return nil, nil, err
	}

	// Get env from metadata
	log.Printf("[%v] getting env from controller", m)
	env, err := api.controller.EnvironmentGet(&fnc.Environment)
	if err != nil {
		return nil, nil, err
	}

	// cache for future
	api.functionEnv.Set(*m, env)
	api.function.Set(*m, fnc)

	return env, fnc, nil
}

func (api *API) getServiceForFunction(m *fission.Metadata, parent opentracing.Span) (string, error) {
	// Make sure we have the full metadata.  This ensures that
	// poolmgr does not implicitly interpret empty-UID as latest
	// version.
	if len(m.Uid) == 0 {
		return "", fission.MakeError(fission.ErrorInvalidArgument,
			fmt.Sprintf("invalid metadata for function %v", m.Name))
	}

	// Check function -> svc cache
	log.Printf("[%v] Checking for cached function service", m.Name)
	fsvc, err := api.fsCache.GetByFunction(m)
	if err == nil {
		// Cached, return svc address
		return fsvc.getAddress(), nil
	}

	respChan := make(chan *createFuncServiceResponse)
	api.requestChan <- &createFuncServiceRequest{
		funcMeta: m,
		respChan: respChan,
		parent:   parent,
	}
	resp := <-respChan
	return resp.address, resp.err
}

func (api *API) createServiceForFunction(m *fission.Metadata, parent opentracing.Span) (string, error) {
	traceCreateNewFuncSvc := opentracing.StartSpan("poolmgr::CreateNewFuncSvc",
		opentracing.ChildOf(parent.Context()))
	defer traceCreateNewFuncSvc.Finish()

	// None exists, so create a new funcSvc:
	log.Printf("[%v] No cached function service found, creating one", m.Name)

	// from Func -> get Env
	log.Printf("[%v] getting environment for function", m.Name)
	traceGetFunctionEnv := opentracing.StartSpan("poolmgr::GetFunctionEnv",
		opentracing.ChildOf(parent.Context()))
	env, fnc, err := api.getFunctionInfo(m)
	traceGetFunctionEnv.Finish()
	if err != nil {
		return "", err
	}

	// from Env -> get GenericPool
	log.Printf("[%v] getting generic pool for env", m.Name)
	traceGetGenericPool := opentracing.StartSpan("poolmgr::GetGenericPool",
		opentracing.ChildOf(parent.Context()))
	pool, err := api.poolMgr.GetPool(env)
	traceGetGenericPool.Finish()
	if err != nil {
		return "", err
	}

	// from GenericPool -> get one function container
	// (this also adds to the cache)
	log.Printf("[%v] getting function service from pool", m.Name)
	traceGetFunctionContainer := opentracing.StartSpan("poolmgr::GetFunctionContainer",
		opentracing.ChildOf(parent.Context()))
	funcSvc, err := pool.GetFuncSvc(m, fnc, env)
	traceGetFunctionContainer.Finish()
	if err != nil {
		return "", err
	}

	increaseColdStarts(m.Name, m.Uid)
	return funcSvc.getAddress(), nil
}

// find funcSvc and update its atime
func (api *API) tapService(w http.ResponseWriter, r *http.Request) {
	sp := startTracingFromHttpHeader("poolmgr::TapService", r)
	defer sp.Finish()

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request", 500)
		return
	}
	svcName := string(body)
	svcHost := strings.TrimPrefix(svcName, "http://")

	err = api.fsCache.TouchByAddress(svcHost)
	if err != nil {
		log.Printf("funcSvc tap error: %v", err)
		http.Error(w, "Not found", 404)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (api *API) Serve(port int) {
	r := mux.NewRouter()
	r.HandleFunc("/v1/getServiceForFunction", api.getServiceForFunctionApi).Methods("POST")
	r.HandleFunc("/v1/tapService", api.tapService).Methods("POST")

	address := fmt.Sprintf(":%v", port)
	log.Printf("starting poolmgr at port %v", port)
	log.Fatal(http.ListenAndServe(address, handlers.LoggingHandler(os.Stdout, r)))
}
