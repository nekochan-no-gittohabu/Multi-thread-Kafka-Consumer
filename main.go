package main

import (
	"argedor/consumer"
	"argedor/producer"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const topic string = "test_topic"
const message = "Hello World"
const brokerAddress = "localhost:29092"
const numOfConThreads = 5

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokerAddress})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	c, err := consumer.New(topic, brokerAddress, "consumer_id")
	if err != nil {
		panic(err)
	}
	defer c.Close()

	for i := 0; i < numOfConThreads; i++ {
		go consumer.Consume(c)
	}

	producer.Produce(p, topic, message, time.Second)
}
