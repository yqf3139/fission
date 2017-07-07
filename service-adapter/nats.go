package service_adapter

import (
	"github.com/nats-io/go-nats-streaming"
	"log"
	"time"
)

type NatsMessage struct {
	topic string
	body  []byte
}

type NatsService struct {
	reqChan    chan *NatsMessage
	clusterID  string
	clientID   string
	clusterUrl string
	conn       stan.Conn
}

func MakeNatsService(clusterID string, clientID string, clusterUrl string) *NatsService {
	ns := &NatsService{
		reqChan:    make(chan *NatsMessage),
		clusterID:  clusterID,
		clientID:   clientID,
		clusterUrl: clusterUrl,
		conn:       nil,
	}
	go ns.service()
	return ns
}

func (ns *NatsService) Publish(topic string, body []byte) {
	ns.reqChan <- &NatsMessage{
		topic: topic,
		body:  body,
	}
}

func (ns *NatsService) connect() {
	sc, err := stan.Connect(ns.clusterID, ns.clientID, stan.NatsURL(ns.clusterUrl))
	if err != nil {
		log.Printf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s",
			err, ns.clusterUrl)
		ns.conn = nil
		return
	}
	ns.conn = sc
}

func (ns *NatsService) service() {
	for {
		req := <-ns.reqChan
		for ns.conn == nil {
			ns.connect()
			if ns.conn != nil {
				break
			}
			time.Sleep(time.Second)
		}
		err := ns.conn.Publish(req.topic, req.body)
		if err != nil {
			log.Fatalf("Error during publish: %v\n", err)
		}
		log.Printf("Published [%s] : '%s'\n", req.topic, req.body)
	}
}
