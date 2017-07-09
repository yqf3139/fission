package service_adapter

import (
	"log"

	catalogclientset "github.com/kubernetes-incubator/service-catalog/pkg/client/clientset_generated/clientset"

	"github.com/fission/fission"
	meta_v1 "github.com/kubernetes-incubator/service-catalog/pkg/apis/servicecatalog/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type AdapterFactory interface {
	create(adapter fission.ServiceAdapter, instanceName string, credentials map[string]string) error
	delete(adapter fission.ServiceAdapter, instanceName string, credentials map[string]string) error
}

type (
	AdapterManager struct {
		factories     map[string]AdapterFactory
		adapterMap    map[string]fission.ServiceAdapter
		catalogClient *catalogclientset.Clientset
		k8sClient     *kubernetes.Clientset
	}
)

func MakeAdapterManager(factories map[string]AdapterFactory,
	catalogClient *catalogclientset.Clientset, k8sClient *kubernetes.Clientset) *AdapterManager {
	return &AdapterManager{
		factories:     factories,
		adapterMap:    make(map[string]fission.ServiceAdapter),
		catalogClient: catalogClient,
		k8sClient:     k8sClient,
	}
}

func (manager *AdapterManager) GetServiceInstance(name string) *meta_v1.Instance {
	instance, err := manager.catalogClient.Instances("fission").Get(name, v1.GetOptions{})
	if err != nil {
		log.Println("Error getting instance ", name, err)
		return nil
	}
	return instance
}

func (manager *AdapterManager) GetInstanceCredentials(instance *meta_v1.Instance) map[string]string {
	bindings, err := manager.catalogClient.Bindings("fission").List(v1.ListOptions{})
	if err != nil {
		log.Println("Error getting binding", instance.Spec, err)
		return nil
	}
	credentials := make(map[string]string)
	for _, binding := range bindings.Items {
		if binding.Spec.InstanceRef.Name != instance.Name {
			continue
		}
		// get secret
		secret, err := manager.k8sClient.CoreV1().Secrets("fission").Get(
			binding.Spec.SecretName, v1.GetOptions{})
		if err != nil {
			log.Println("Error getting secret", binding.Spec.SecretName, err)
			return nil
		}
		for k, v := range secret.Data {
			credentials[k] = string(v)
		}
		break
	}
	return credentials
}

func (manager *AdapterManager) Sync(adapters []fission.ServiceAdapter) {
	newAdapters := make(map[string]fission.ServiceAdapter)
	for _, a := range adapters {
		newAdapters[a.Uid] = a
	}
	for _, a := range manager.adapterMap {
		if _, found := newAdapters[a.Uid]; !found {
			// delete adapters
			instance := manager.GetServiceInstance(a.InstanceName)
			if instance == nil {
				continue
			}
			svcType := instance.Spec.ServiceClassName
			if factory, ok := manager.factories[svcType]; ok {
				credentials := manager.GetInstanceCredentials(instance)
				if credentials == nil {
					continue
				}

				log.Println("Delete adapter, ", a.Name)
				err := factory.delete(a, instance.Name, credentials)
				if err != nil {
					log.Println("Error when deleting adapter, ", err)
				}
			} else {
				log.Println("Cannot found adapter factory for type: ", svcType)
			}
		}
	}
	for _, a := range newAdapters {
		if _, found := manager.adapterMap[a.Uid]; !found {
			// create adapters
			instance := manager.GetServiceInstance(a.InstanceName)
			if instance == nil {
				continue
			}
			svcType := instance.Spec.ServiceClassName
			if factory, ok := manager.factories[svcType]; ok {
				credentials := manager.GetInstanceCredentials(instance)
				if credentials == nil {
					continue
				}
				log.Println("Create adapter, ", a.Name)
				err := factory.create(a, instance.Name, credentials)
				if err != nil {
					log.Println("Error when creating adapter, ", err)
				}
			} else {
				log.Println("Cannot found adapter factory for type: ", svcType)
			}
		}
	}
	manager.adapterMap = newAdapters
}
