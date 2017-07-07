/*
CopyrigserviceAdapter 2017 The Fission Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    serviceAdaptertp://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"github.com/fission/fission"
	uuid "github.com/satori/go.uuid"
)

type ServiceAdapterStore struct {
	ResourceStore
}

func (sas *ServiceAdapterStore) Create(serviceAdapter *fission.ServiceAdapter) (string, error) {
	serviceAdapter.Metadata.Uid = uuid.NewV4().String()
	return serviceAdapter.Metadata.Uid, sas.ResourceStore.create(serviceAdapter)
}

func (sas *ServiceAdapterStore) Get(m *fission.Metadata) (*fission.ServiceAdapter, error) {
	var serviceAdapter fission.ServiceAdapter
	err := sas.ResourceStore.read(m.Name, &serviceAdapter)
	if err != nil {
		return nil, err
	}
	return &serviceAdapter, nil
}

func (sas *ServiceAdapterStore) Update(serviceAdapter *fission.ServiceAdapter) (string, error) {
	serviceAdapter.Metadata.Uid = uuid.NewV4().String()
	return serviceAdapter.Metadata.Uid, sas.ResourceStore.update(serviceAdapter)
}

func (sas *ServiceAdapterStore) Delete(m fission.Metadata) error {
	typeName, err := getTypeName(fission.ServiceAdapter{})
	if err != nil {
		return err
	}
	return sas.ResourceStore.delete(typeName, m.Name)
}

func (sas *ServiceAdapterStore) List() ([]fission.ServiceAdapter, error) {
	typeName, err := getTypeName(fission.ServiceAdapter{})
	if err != nil {
		return nil, err
	}
	bufs, err := sas.ResourceStore.getAll(typeName)
	if err != nil {
		return nil, err
	}

	triggers := make([]fission.ServiceAdapter, 0, len(bufs))
	js := JsonSerializer{}
	for _, buf := range bufs {
		var serviceAdapter fission.ServiceAdapter
		err = js.deserialize([]byte(buf), &serviceAdapter)
		if err != nil {
			return nil, err
		}
		triggers = append(triggers, serviceAdapter)
	}

	return triggers, nil
}
