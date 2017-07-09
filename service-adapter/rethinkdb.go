package service_adapter

import (
	"fmt"
	"log"

	r "gopkg.in/gorethink/gorethink.v3"

	"encoding/json"
	"github.com/fission/fission"
)

type RethinkDBAdapter struct {
	database    string
	table       string
	key         string
	value       string
	topic       string
	session     *r.Session
	natsService *NatsService
	closeChan   chan struct{}
}

type RethinkDBAdapterFactory struct {
	natsService *NatsService
	connMap     map[string]*r.Session
	adapterMap  map[string]*RethinkDBAdapter
}

func MakeRethinkDBAdapterFactory(natsService *NatsService) *RethinkDBAdapterFactory {
	return &RethinkDBAdapterFactory{
		natsService: natsService,
		connMap:     make(map[string]*r.Session),
		adapterMap:  make(map[string]*RethinkDBAdapter),
	}
}

func getRethinkDBConn(credentials map[string]string) (*r.Session, error) {
	host := credentials["host"]
	port := credentials["ports.driver"]
	user := "admin"
	password := credentials["password.admin"]
	db := "test"

	session, err := r.Connect(r.ConnectOpts{
		Address:  fmt.Sprintf("%v:%v", host, port),
		Database: db,
		Username: user,
		Password: password,
	})
	if err != nil {
		log.Println("Error getting RethinkDB session, ", err)
		return nil, err
	}

	return session, nil
}

func (f *RethinkDBAdapterFactory) create(adapter fission.ServiceAdapter,
	instanceName string, credentials map[string]string) error {

	session, found := f.connMap[instanceName]
	var err error
	if !found {
		session, err = getRethinkDBConn(credentials)
		if err != nil {
			return err
		}
		f.connMap[instanceName] = session
	}
	if !session.IsConnected() {
		err = session.Reconnect()
		if err != nil {
			return err
		}
		f.connMap[instanceName] = session
	}

	rethinkDBAdapter := &RethinkDBAdapter{
		database: adapter.Spec["database"],
		table:    adapter.Spec["table"],
		key:      adapter.Spec["key"],
		value:    adapter.Spec["value"],
		topic:    fmt.Sprintf("fission.service-adapter.rethinkdb.%v", adapter.Name),

		session:     session,
		natsService: f.natsService,
		closeChan:   make(chan struct{}),
	}
	f.adapterMap[adapter.Name] = rethinkDBAdapter
	go rethinkDBAdapter.listenChanges()
	return nil
}

func (f *RethinkDBAdapterFactory) delete(adapter fission.ServiceAdapter,
	instanceName string, credentials map[string]string) error {
	rethinkDBAdapter, found := f.adapterMap[adapter.Name]
	if !found {
		return fmt.Errorf("RethinkDB adapter not found: %v", adapter.Name)
	}
	rethinkDBAdapter.closeChan <- struct{}{}
	return nil
}

func (a *RethinkDBAdapter) listenChanges() {
	term := r.DB(a.database).Table(a.table).Changes()
	if a.key != "" && a.value != "" {
		term = term.Filter(r.Row.Field("new_val").Field(a.key).Eq(a.value))
	}
	res, err := term.Run(a.session)
	if err != nil {
		log.Println("Error listen changes on ", a.database, a.table, err)
		return
	}
	ch := make(chan interface{})
	res.Listen(ch)
	for {
		select {
		case <-a.closeChan:
			res.Close()
			log.Println("Close listener", a.table)
			return
		case item := <-ch:
			data, err := json.Marshal(item)
			if err != nil {
				log.Println("Error marshal item", item, err)
				continue
			}
			log.Println("Pub change to topic: ", a.topic, item)
			a.natsService.Publish(a.topic, data)
		}
	}
}
