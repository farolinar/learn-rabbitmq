package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"order-system/pkg/config"
	"os"
	"os/signal"
	"strings"
)

type EventPayload struct {
	RoutingKey string    `json:"routingKey"`
	OrderID    string    `json:"orderId"`
	Amount     float64   `json:"amount"`
}

func main() {
	conn, ch, err := config.ConnectRabbitMQ()
	if err != nil {
		log.Fatalf("failed to setup: %v", err)
	}
	defer conn.Close()
	defer ch.Close()

	exchangeName := "order_topic_exchange"
	region := "au"

	if len(os.Args) > 1 {
		// config region through command
		region = os.Args[1]
	}

	bindingPattern := fmt.Sprintf("*.created.%s", region)

	err = ch.ExchangeDeclare(
		exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed declaring exchange: %v", err)
	}

	q, err := ch.QueueDeclare(
		fmt.Sprintf("%s_fulfillment_queue", region),
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed declaring queue: %v", err)
	}

	err = ch.QueueBind(
		q.Name,
		bindingPattern,
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed declaring queue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgs, err := ch.ConsumeWithContext(ctx,
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed consumer register: %v", err)
	}

	log.Printf("[%s Fulfillment Service] Subscribed to pattern: '%s'", strings.ToUpper(region), bindingPattern)

	go func(){
		for d := range msgs {
			var payload EventPayload
			err = json.Unmarshal(d.Body, &payload)
			if err != nil {
				log.Printf("failed to unmarshal message: %v", err)
				d.Nack(false, false)
			}
			log.Printf("[%s Fulfillment] 📦 Packing order #%s for %s delivery (Routing: %s)", strings.ToUpper(region), payload.OrderID, strings.ToUpper(region), d.RoutingKey)
			d.Ack(false)
		}
	}()

	sig := make (chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	log.Println("Shutdown signal received, stopping consumer...")
	cancel()
}
