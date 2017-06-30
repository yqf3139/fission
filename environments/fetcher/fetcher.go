package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/fission/fission"
)

type Fetcher struct {
	sharedVolumePath string
}

func MakeFetcher(sharedVolumePath string) *Fetcher {
	return &Fetcher{sharedVolumePath: sharedVolumePath}
}

func (fetcher *Fetcher) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "", 404)
		return
	}

	// parse request
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body")
		http.Error(w, err.Error(), 500)
		return
	}
	var req fission.FetchRequest
	err = json.Unmarshal(body, &req)
	if err != nil {
		log.Printf("Error reading request body: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	log.Printf("fetcher request: %v", req)

	// fetch the file and save it to tmp path
	resp, err := http.Get(req.Url)
	if err != nil {
		e := fmt.Sprintf("Failed to fetch from url: %v", err)
		log.Printf(e)
		http.Error(w, e, 400)
		return
	}
	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		e := fmt.Sprintf("Failed to read from url: %v", err)
		log.Printf(e)
		http.Error(w, e, 400)
		return
	}
	tmpFile := req.Filename + ".tmp"
	tmpPath := filepath.Join(fetcher.sharedVolumePath, tmpFile)
	err = ioutil.WriteFile(tmpPath, body, 0600)
	if err != nil {
		e := fmt.Sprintf("Failed to write file: %v", err)
		log.Printf(e)
		http.Error(w, e, 500)
		return
	}

	// TODO: add signature verification

	// move tmp file to requested filename
	err = os.Rename(tmpPath, filepath.Join(fetcher.sharedVolumePath, req.Filename))
	if err != nil {
		e := fmt.Sprintf("Failed to move file: %v", err)
		log.Printf(e)
		http.Error(w, e, 500)
		return
	}

	// save the service instances into file
	body, err = json.Marshal(req.ServicesInstances)
	if err != nil {
		e := fmt.Sprintf("Failed to marshal instances: %v", err)
		log.Printf(e)
		http.Error(w, e, 500)
		return
	}

	instancesFilepath := filepath.Join(fetcher.sharedVolumePath, "service-instances.json")
	err = ioutil.WriteFile(instancesFilepath, body, 0600)
	if err != nil {
		e := fmt.Sprintf("Failed to write file: %v", err)
		log.Printf(e)
		http.Error(w, e, 500)
		return
	}
	log.Printf("service-instances.json: %v", string(body))

	// all done
	w.WriteHeader(http.StatusOK)
}

// Usage: fetcher <shared volume path>
func main() {
	dir := os.Args[1]
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dir, os.ModeDir|0700)
			if err != nil {
				log.Fatalf("Error creating directory: %v", err)
			}
		}
	}
	fetcher := MakeFetcher(dir)
	mux := http.NewServeMux()
	mux.HandleFunc("/", fetcher.handler)
	http.ListenAndServe(":8000", mux)
}
