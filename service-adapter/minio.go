package service_adapter

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"errors"
	"github.com/fission/fission"
	"github.com/gorilla/mux"
	"github.com/jmoiron/jsonq"
	"github.com/minio/minio-go"
)

type MinioAdapterFactory struct {
	natsService *NatsService
}

func isIn(array []string, key string) bool {
	for _, a := range array {
		if a == key {
			return true
		}
	}
	return false
}

func getMinioClient(credentials map[string]string) (*minio.Client, error) {
	// Initialize minio client object.
	endpoint := credentials["host"]
	port := credentials["port"]
	accessKey := credentials["keys.access"]
	secretKey := credentials["keys.secret"]

	return minio.New(fmt.Sprintf("%v:%v", endpoint, port), accessKey, secretKey, false)
}

func (f *MinioAdapterFactory) create(adapter fission.ServiceAdapter,
	instanceName string, credentials map[string]string) error {

	minioClient, err := getMinioClient(credentials)
	if err != nil {
		fmt.Println("Error getting minio client", err)
		return err
	}

	bucket := adapter.Spec["bucket"]
	if bucket == "" {
		return errors.New("Bucket name is empty")
	}

	exists, err := minioClient.BucketExists(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("Bucket not exists")
	}

	arn := minio.NewArn("minio", "sqs", "us-east-1", "1", "webhook")
	config := minio.NewNotificationConfig(arn)
	config.AddEvents(minio.ObjectCreatedAll, minio.ObjectRemovedAll)
	if prefix := adapter.Spec["prefix"]; prefix != "" {
		config.AddFilterPrefix(prefix)
	}
	if suffix := adapter.Spec["suffix"]; suffix != "" {
		config.AddFilterSuffix(suffix)
	}

	bucketNotification := minio.BucketNotification{}
	bucketNotification.AddQueue(config)

	return minioClient.SetBucketNotification(bucket, bucketNotification)
}

func (f *MinioAdapterFactory) delete(adapter fission.ServiceAdapter,
	instanceName string, credentials map[string]string) error {

	minioClient, err := getMinioClient(credentials)
	if err != nil {
		fmt.Println("Error getting minio client", err)
		return err
	}

	bucket := adapter.Spec["bucket"]
	if bucket == "" {
		return errors.New("Bucket name is empty")
	}
	exists, err := minioClient.BucketExists(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("Bucket not exists")
	}

	return minioClient.RemoveAllBucketNotification(bucket)
}

func (f *MinioAdapterFactory) eventHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request", 500)
		return
	}

	if !isIn(r.Header["Content-Type"], "application/json") {
		log.Println("Minio init webhook: ", string(body))
		w.WriteHeader(http.StatusOK)
		return
	}
	bodyStr := string(body)
	data := map[string]interface{}{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		log.Println("Failed to read json: ", bodyStr, err)
		w.WriteHeader(http.StatusOK)
		return
	}

	jq := jsonq.NewQuery(data)
	bucketName, err := jq.String("Records", "0", "s3", "bucket", "name")
	if err != nil {
		log.Println("Failed to read bucket name: ", bodyStr, err)
		w.WriteHeader(http.StatusOK)
		return
	}

	topic := fmt.Sprintf("fission.service-adapter.minio.%v.%v", id, bucketName)

	log.Println("pub event to ", topic)
	f.natsService.Publish(topic, body)
	w.WriteHeader(http.StatusOK)
}
