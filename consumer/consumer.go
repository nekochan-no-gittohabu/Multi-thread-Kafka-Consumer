package consumer

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func New(topic string, brokerAddr string, id string) (*kafka.Consumer, error) {
	fmt.Println("consumer init")

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               brokerAddr,
		"group.id":                        id,
		"auto.offset.reset":               "earliest",
		"go.application.rebalance.enable": true,
		"enable.auto.commit":              false,
	})
	if err != nil {
		return nil, err
	}
	c.SubscribeTopics([]string{topic}, nil)
	return c, nil
}

func Consume(c *kafka.Consumer) {
	fmt.Println("consume")

	for {
		randWait := rand.Intn(6) + 5
		time.Sleep(time.Duration(randWait) * time.Second)

		event := c.Poll((100))
		if event == nil {
			continue
		}
		switch e := event.(type) {
		case *kafka.Message:
			fmt.Println("Received ", string(e.Value))
			_, err := c.CommitMessage(e)
			if err != nil {
				fmt.Println("Cannot commit ", err)
			}
		case kafka.Error:
			panic(e.String())
		default:
			fmt.Println("Ignored ", e)
		}

	}
}
