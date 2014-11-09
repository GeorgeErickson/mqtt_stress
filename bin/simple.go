package main

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"flag"
	"fmt"
	"github.com/GeorgeErickson/mqtt_stress/mqtt"
	"github.com/GeorgeErickson/mqtt_stress/utils"
	// "sync"
	"log"
	"sync/atomic"
	"time"
)

var (
	host       = flag.String("host", "mqtt.gee.io", "MQTT host")
	port       = flag.String("port", "1883", "MQTT port")
	numDevices = flag.Int("n", 1000, "number of devices to simulate")
)

type Benchmark struct {
	ConnectionCount uint64
	Devices         map[string]*Device
}

func NewBenchmark() *Benchmark {
	return &Benchmark{0, make(map[string]*Device)}
}

func (bench *Benchmark) Start() {
	go bench.PrintMetrics()
	bench.CreateDevices()
}

func (bench *Benchmark) PrintMetrics() {
	for _ = range time.NewTicker(time.Second * 1).C {
		fmt.Println(bench.ConnectionCount)
	}
}

func (bench *Benchmark) CreateDevices() {
	tickChan := time.NewTicker(time.Millisecond * 100)

	i := 0
	errCount := uint64(0)
	for _ = range tickChan.C {
		go func(bench *Benchmark) {
			device, err := NewDevice()
			if err != nil {
				atomic.AddUint64(&errCount, 1)
				if errCount >= 5 {
					log.Fatal("Max devices: ", bench.ConnectionCount)
				}
			} else {
				bench.Devices[device.id] = device
				atomic.AddUint64(&bench.ConnectionCount, 1)
			}

		}(bench)

		i += 1

		if i == *numDevices {
			return
		}
	}

}

type SentMsg struct {
	DeviceId string
	MsgId    string
	SentAt   time.Time
}

type Device struct {
	id        string
	stop      chan struct{}
	lostCount uint64
	pub       *mqtt.Client
	sub       *mqtt.Client
}

func (d *Device) OnMessage(buf *bytes.Buffer) {
	var msg SentMsg
	utils.DecodeGob(buf, &msg)
	fmt.Println(time.Now().Sub(msg.SentAt))
}

func NewDevice() (*Device, error) {
	id := uuid.New()

	device := &Device{
		id:        id,
		stop:      make(chan struct{}),
		lostCount: 0,
	}
	on_lost := func(reason error) {
		fmt.Println(reason)
		atomic.AddUint64(&device.lostCount, 1)
	}
	device.pub = mqtt.CreateClient("pub-"+id, *host, *port, on_lost)
	device.sub = mqtt.CreateClient("sub-"+id, *host, *port, on_lost)
	_, err := device.pub.PahoClient.Start()
	if err != nil {
		return nil, err
	}
	_, err = device.sub.PahoClient.Start()
	if err != nil {
		return nil, err
	}

	return device, nil
}

// func worker(wg *sync.WaitGroup) {
// 	defer wg.Done()

// 	device := NewDevice()

// 	go device.sub.Subscribe(device.id, device.OnMessage)

// 	publishTicker := time.NewTicker(time.Second * 1)

// 	for _ = range publishTicker.C {
// 		msg := &SentMsg{device.id, uuid.New(), time.Now()}
// 		device.pub.Publish(device.id, msg)
// 		// TODO - log sent
// 	}
// }

func main() {
	flag.Parse()
	bench := NewBenchmark()
	bench.Start()

}
