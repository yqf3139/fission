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

package controller

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	log "github.com/Sirupsen/logrus"

	"github.com/fission/fission"
)

func (api *API) ServiceAdapterApiList(w http.ResponseWriter, r *http.Request) {
	adapters, err := api.ServiceAdapterStore.List()
	if err != nil {
		api.respondWithError(w, err)
		return
	}

	resp, err := json.Marshal(adapters)
	if err != nil {
		api.respondWithError(w, err)
		return
	}

	api.respondWithSuccess(w, resp)
}

func (api *API) ServiceAdapterApiCreate(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		api.respondWithError(w, err)
		return
	}

	var adapter fission.ServiceAdapter
	err = json.Unmarshal(body, &adapter)
	if err != nil {
		api.respondWithError(w, err)
		return
	}

	adapters, err := api.ServiceAdapterStore.List()
	if err != nil {
		api.respondWithError(w, err)
		return
	}
	for _, trigger := range adapters {
		if trigger.Name == adapter.Name {
			err = fission.MakeError(fission.ErrorNameExists,
				"ServiceAdapter with same name already exists")
			api.respondWithError(w, err)
			return
		}
	}

	uid, err := api.ServiceAdapterStore.Create(&adapter)
	if err != nil {
		api.respondWithError(w, err)
		return
	}

	m := &fission.Metadata{Name: adapter.Metadata.Name, Uid: uid}
	resp, err := json.Marshal(m)
	if err != nil {
		api.respondWithError(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	api.respondWithSuccess(w, resp)
}

func (api *API) ServiceAdapterApiGet(w http.ResponseWriter, r *http.Request) {
	var m fission.Metadata

	vars := mux.Vars(r)
	m.Name = vars["adapter"]
	m.Uid = r.FormValue("uid") // empty if uid is absent

	adapter, err := api.ServiceAdapterStore.Get(&m)
	if err != nil {
		api.respondWithError(w, err)
		return
	}

	resp, err := json.Marshal(adapter)
	if err != nil {
		api.respondWithError(w, err)
		return
	}

	api.respondWithSuccess(w, resp)
}

func (api *API) ServiceAdapterApiUpdate(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["adapter"]

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		api.respondWithError(w, err)
		return
	}

	var adapter fission.ServiceAdapter
	err = json.Unmarshal(body, &adapter)
	if err != nil {
		api.respondWithError(w, err)
		return
	}

	if name != adapter.Metadata.Name {
		err = fission.MakeError(fission.ErrorInvalidArgument, "ServiceAdapter name doesn'adapter match URL")
		api.respondWithError(w, err)
		return
	}

	uid, err := api.ServiceAdapterStore.Update(&adapter)
	if err != nil {
		api.respondWithError(w, err)
		return
	}

	m := &fission.Metadata{Name: adapter.Metadata.Name, Uid: uid}
	resp, err := json.Marshal(m)
	if err != nil {
		api.respondWithError(w, err)
		return
	}
	api.respondWithSuccess(w, resp)
}

func (api *API) ServiceAdapterApiDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	var m fission.Metadata
	m.Name = vars["adapter"]

	m.Uid = r.FormValue("uid") // empty if uid is absent
	if len(m.Uid) == 0 {
		log.WithFields(log.Fields{"adapter": m.Name}).Info("Deleting all versions")
	}

	err := api.ServiceAdapterStore.Delete(m)
	if err != nil {
		api.respondWithError(w, err)
		return
	}

	api.respondWithSuccess(w, []byte(""))
}
