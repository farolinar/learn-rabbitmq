package main

import (
	"context"
	"encoding/json"
	"log"
	"order-system/pkg/config"
	"os"
	"os/signal"
)

type BroadcastPayload struct {
	OrderID string  `json:"orderId"`
	Total   float64 `json:"total"`
}

func main() {
	conn, ch, err := config.ConnectRabbitMQ()
	if err != nil {
		log.Fatalf("failed to setup RabbitMQ: %v", err)
	}
	defer conn.Close()
	defer ch.Close()

	exchangeName := "order_broadcast"

	err = ch.ExchangeDeclare(
		exchangeName,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to declare exchange: %v", err)
	}

	q, err := ch.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to declare queue: %v", err)
	}

	err = ch.QueueBind(
		q.Name,
		"",
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to bind query and exchange: %v", err)
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
		log.Fatalf("failed to consume: %v", err)
	}

	go func() {
		for d := range msgs {
			var event BroadcastPayload
			err = json.Unmarshal(d.Body, &event)
			if err != nil {
				log.Printf("failed to unmarshal a message: %v", err)
				d.Nack(false, false)
			}
			log.Printf("[Analytics Service] 📊 Recording metric: Order #%s revenue +$%.2f", event.OrderID, event.Total)
			d.Ack(false)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	log.Println("Shutdown signal received, stopping consumer...")
	cancel()
}
