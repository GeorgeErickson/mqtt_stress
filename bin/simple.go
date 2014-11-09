package main

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"flag"
	"fmt"
	"github.com/GeorgeErickson/mqtt_stress/mqtt"
	"github.com/GeorgeErickson/mqtt_stress/utils"
	"sync"
	"time"
)

var (
	host    = flag.String("host", "mqtt.gee.io", "MQTT host")
	port    = flag.String("port", "1883", "MQTT port")
	numConn = flag.Int("n", 1000, "half the number of concurrent connections")
	numMsg  = flag.Int("numMsg", 3, "number of messages sent by each client per second")
)

type MsgType int

const (
	PUB MsgType = 0
	SUB MsgType = 1
)

type MsgInfo struct {
	msg_id    string
	timestamp time.Time
	msg_type  MsgType
}

type Message struct {
	SentAt   time.Time
	MsgCount int
}

type Device struct {
	id         string
	stop       chan struct{}
	pub_client *mqtt.Client
	sub_client *mqtt.Client
}

func (d *Device) OnMessage(buf *bytes.Buffer) {
	var msg Message
	utils.DecodeGob(buf, &msg)
	fmt.Println(time.Now().Sub(msg.SentAt))
}

func NewDevice() *Device {
	id := uuid.New()

	device := &Device{
		id:         id,
		stop:       make(chan struct{}),
		pub_client: mqtt.CreateClient("pub-"+id, *host, *port),
		sub_client: mqtt.CreateClient("sub-"+id, *host, *port),
	}

	return device
}

func worker(wg *sync.WaitGroup) {
	defer wg.Done()

	device := NewDevice()
	device.pub_client.Start()
	device.sub_client.Start()

	go device.sub_client.Subscribe(device.id, device.OnMessage)

	publishTicker := time.NewTicker(time.Second * 1)
	msg_count := 0

	for t := range publishTicker.C {
		msg_count += 1
		device.pub_client.Publish(device.id, &Message{t, msg_count})
	}
}

func main() {
	flag.Parse()
	var wg sync.WaitGroup
	fmt.Println(*numConn)
	for i := 0; i < *numConn; i++ {
		wg.Add(1)
		go worker(&wg)
		time.Sleep(1 * time.Second)
	}
	fmt.Println("All Conn")

	wg.Wait()
}
