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

type SentMsg struct {
	DeviceId string
	MsgId    string
	SentAt   time.Time
}

type Device struct {
	id         string
	stop       chan struct{}
	pub_client *mqtt.Client
	sub_client *mqtt.Client
}

func (d *Device) OnMessage(buf *bytes.Buffer) {
	var msg SentMsg
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

	for _ = range publishTicker.C {
		msg := &SentMsg{device.id, uuid.New(), time.Now()}
		device.pub_client.Publish(device.id, msg)
		// TODO - log sent
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
