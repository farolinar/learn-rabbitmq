package main

import (
	"context"
	"encoding/json"
	"log"
	"order-system/pkg/config"
	"os"
	"os/signal"
	"time"
)

type BroadcastPayload struct {
	EventID   string    `json:"eventId"`
	EventType string    `json:"eventType"`
	Timestamp time.Time `json:"timestamp"`
	OrderID   string    `json:"orderId"`
	CustomerID  string    `json:"customerId"`
	Total     float64   `json:"total"`
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

	// Declare temporary exclusive queue
	q, err := ch.QueueDeclare(
		"",		// empty: broker generates unique queue name
		false,	// non-durable (will not survive server restart)
		true,	// delete when unused
		true,	// exclusive
		false,	// no-wait
		nil,
	)
	if err != nil {
		log.Fatalf("failed to declare queue: %v", err)
	}

	// Bind queue to exchange
	err = ch.QueueBind(
		q.Name,
		"",
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to bind queue with exchange: %v", err)
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

	log.Printf("[Notification Service] Subscribed to '%s'. Queue: %s", exchangeName, q.Name)

	go func() {
		for d := range msgs {
			var event BroadcastPayload
			err = json.Unmarshal(d.Body, &event)
			if err != nil {
				log.Printf("failed to unmarshal a message: %v", err)
				d.Nack(false, false)
			}
			log.Printf("[Notification Service] 📧 Sending Email/SMS for Order #%s (Customer: %s)", event.OrderID, event.CustomerID)
			d.Ack(false)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	log.Println("Shutdown signal received, stopping consumer...")
	cancel()
}