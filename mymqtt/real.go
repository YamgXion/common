package mymqtt

import (
	"fmt"

	config "../myconfig"
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

// MqttClient mqtt客户端
type MqttClient struct {
	Client MQTT.Client
	URL    string
}

// Start 开启服务器
func (mq *MqttClient) Start() error {
	cfg := config.Cfg.Rxmqtt
	mq.URL = fmt.Sprintf("tcp://%s:%d", cfg.Host, cfg.Port)
	opts := MQTT.NewClientOptions().AddBroker(mq.URL)
	opts.SetClientID(cfg.Clientid)
	opts.SetCleanSession(cfg.Clean)
	opts.SetUsername(cfg.User)
	opts.SetPassword(cfg.Password)
	opts.SetProtocolVersion(4)
	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	mq.Client = c

	sub := cfg.Subtops
	mq.subcallback(sub.Hour, handleHour)
	mq.subcallback(sub.Fminute, handleFminute)
	mq.subcallback(sub.Minute, handleMinute)
	mq.subcallback(sub.Real, handleReal)
	mq.subcallback(sub.State, handleState)
	mq.subcallback(sub.Qc, handleQc)
	mq.subcallback(sub.Env, handleEnv)
	mq.subcallback(sub.Alarm, handleAlarm)

	return nil
}

// Send 推送数据
func (mq *MqttClient) Send(topic string, qos byte, payload interface{}) {
	// topic string, qos byte, retained bool, payload interface{}
	if mq.Client != nil {
		mq.Client.Publish(topic, qos, false, payload)
	}
}

func (mq *MqttClient) subcallback(topic config.SubTopic, cb MQTT.MessageHandler) error {
	if len(topic.Topic) > 0 {
		if token := mq.Client.Subscribe(topic.Topic, topic.Qos, cb); token.Wait() && token.Error() != nil {
			return token.Error()
		}
	}
	return nil
}

func handleHour(client MQTT.Client, msg MQTT.Message) {

}

func handleFminute(client MQTT.Client, msg MQTT.Message) {

}

func handleMinute(client MQTT.Client, msg MQTT.Message) {

}

func handleReal(client MQTT.Client, msg MQTT.Message) {

}

func handleState(client MQTT.Client, msg MQTT.Message) {

}

func handleQc(client MQTT.Client, msg MQTT.Message) {

}

func handleEnv(client MQTT.Client, msg MQTT.Message) {

}

func handleAlarm(client MQTT.Client, msg MQTT.Message) {

}
