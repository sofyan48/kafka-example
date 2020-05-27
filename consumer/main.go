package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
)

func main() {

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Net.WriteTimeout = 5 * time.Second
	switch "RANGE" {
	case "STICKY":
		kafkaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "ROUND_ROBIN":
		kafkaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "RANGE":
		kafkaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		kafkaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	}

	kafkaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest

	// Specify brokers address. This is default one
	brokers := []string{"localhost:9092"}

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, kafkaConfig)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	topic := "test"
	// How to decide partition, is it fixed value...?
	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0

	// Get signnal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Println("Received messages", string(msg.Key), string(msg.Value))
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}
