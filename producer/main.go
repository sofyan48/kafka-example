package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

func main() {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Retry.Max = 1
	kafkaConfig.Net.WriteTimeout = 5 * time.Second

	brokers := []string{"localhost:9092"}
	fmt.Println(brokers)
	producer, err := sarama.NewSyncProducer(brokers, kafkaConfig)
	if err != nil {
		log.Println("ERROR:", err)
		os.Exit(1)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Println("ERROR:", err)
			os.Exit(1)
		}
	}()

	topic := "test"
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("Something Cool"),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		panic(err)
	}

	fmt.Printf("topic: %s| partition: %d | offset %d\n", topic, partition, offset)
}
