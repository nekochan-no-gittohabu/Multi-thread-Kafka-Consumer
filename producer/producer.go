package producer

import (
	"fmt"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Produce(p *kafka.Producer, topic string, message string, interval time.Duration) {
	fmt.Println("produce")
	i := 0
	for {
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(strconv.Itoa(i)),
			Value:          []byte(message),
		}, nil)
		if err != nil {
			panic("Could not enqueue message " + err.Error())
		}

		event := <-p.Events()
		message := event.(*kafka.Message)

		if message.TopicPartition.Error != nil {
			fmt.Println("Delivery failed due to error: ", message.TopicPartition.Error)
		} else {
			fmt.Println("Delivered msg: " + string(message.Value))
		}

		i++
		time.Sleep(interval)
	}
}
