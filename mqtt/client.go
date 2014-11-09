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

type Client struct {
	client *paho.MqttClient
}

func (c *Client) Start() {
	_, err := c.client.Start()
	if err != nil {
		log.Print(err)
	}
}

func (c *Client) Publish(topic string, payload interface{}) {
	var msg bytes.Buffer
	err := gob.NewEncoder(&msg).Encode(payload)
	utils.CheckErrFatal(err)
	c.client.Publish(paho.QOS_ONE, topic, msg.Bytes())
}

func (c *Client) Subscribe(topic string, cb MessageHandler) {
	on_msg := func(client *paho.MqttClient, paho_msg paho.Message) {
		var buf bytes.Buffer
		buf.Write(paho_msg.Payload())
		cb(&buf)
	}
	topicFilter, err := paho.NewTopicFilter(topic, 1)
	utils.CheckErrFatal(err)
	c.client.StartSubscription(on_msg, topicFilter)
}

func CreateClient(id string, host string, port string) *Client {
	on_lost := func(client *paho.MqttClient, reason error) {
		fmt.Println(reason)
	}
	addr := fmt.Sprintf("tcp://%s:%s", host, port)
	opts := paho.NewClientOptions().AddBroker(addr).SetClientId(id).SetOnConnectionLost(on_lost)

	client := &Client{paho.NewClient(opts)}
	return client
}
