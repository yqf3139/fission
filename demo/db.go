package main

import (
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"sync/atomic"
)

func main() {
	var counter uint64 = 0

	r := mux.NewRouter()
	r.HandleFunc("/count", func(resp http.ResponseWriter, req *http.Request) {
		atomic.AddUint64(&counter, 1)
		resp.WriteHeader(200)
	}).Methods("POST")
	r.HandleFunc("/count", func(resp http.ResponseWriter, req *http.Request) {
		value := atomic.LoadUint64(&counter)
		resp.WriteHeader(200)
		resp.Write([]byte(fmt.Sprintln(value)))
	}).Methods("GET")
	r.HandleFunc("/count", func(resp http.ResponseWriter, req *http.Request) {
		atomic.StoreUint64(&counter, 0)
		resp.WriteHeader(200)
	}).Methods("DELETE")
	http.ListenAndServe(":8080", r)
}
