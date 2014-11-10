package mqtt

import (
	"bytes"
	"encoding/gob"
	"fmt"
	paho "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	"github.com/GeorgeErickson/mqtt_stress/utils"
	"log"
)

type MessageHandler func(buf *bytes.Buffer)
type OnConnectionLost func(reason error)

type Client struct {
	PahoClient *paho.MqttClient
}

func (c *Client) Start() {
	_, err := c.PahoClient.Start()
	if err != nil {
		log.Print(err)
	}
}

func (c *Client) Publish(topic string, payload interface{}) {
	var msg bytes.Buffer
	err := gob.NewEncoder(&msg).Encode(payload)
	utils.CheckErrFatal(err)
	c.PahoClient.Publish(paho.QOS_ONE, topic, msg.Bytes())
}

func (c *Client) Subscribe(topic string, cb MessageHandler) {
	on_msg := func(client *paho.MqttClient, paho_msg paho.Message) {
		var buf bytes.Buffer
		buf.Write(paho_msg.Payload())
		cb(&buf)
	}
	topicFilter, err := paho.NewTopicFilter(topic, 1)
	utils.CheckErrFatal(err)
	c.PahoClient.StartSubscription(on_msg, topicFilter)
}

func CreateClient(id string, host string, port string, cb OnConnectionLost) *Client {
	on_lost := func(client *paho.MqttClient, reason error) {
		fmt.Println(reason)
		cb(reason)
	}
	addr := fmt.Sprintf("tcp://%s:%s", host, port)
	opts := paho.NewClientOptions().AddBroker(addr).SetClientId(id).SetOnConnectionLost(on_lost).SetCleanSession(true)

	client := &Client{paho.NewClient(opts)}
	return client
}
