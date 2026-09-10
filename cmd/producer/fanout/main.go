package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"order-system/pkg/config"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type BroadcastPayload struct {
	EventID string `json:"eventId"`
	EventType string `json:"eventType"`
	Timestamp time.Time `json:"timestamp"`
	OrderID string `json:"orderId"`
	CustomerID string `json:"customerId"`
	Total float64 `json:"total"`
}

func main() {
	rand.Seed(time.Now().Unix())

	conn, ch, err := config.ConnectRabbitMQ()
	if err != nil {
		log.Fatalf("failed to setup RabbitMQ: %v", err)
	}
	defer conn.Close()
	defer ch.Close()

	exchangeName := "order_broadcast"

	err = ch.ExchangeDeclare(
		exchangeName, 	// name
		"fanout",		// type
		true,			// durable
		false,			// auto-delete
		false,			// internal
		false,			// no-wait
		nil,			// arguments
	)
	if err != nil {
		log.Fatalf("failed to declare exchange: %v", err)
	}

	payload := BroadcastPayload{
		EventID: fmt.Sprintf("evt_%d", time.Now().UnixNano()),
		EventType: "order.placed",
		Timestamp: time.Now(),
		OrderID: fmt.Sprintf("ord_%d", rand.Intn(1000)),
		CustomerID: fmt.Sprintf("cst_%d", rand.Intn(1000)),
		Total: float64(rand.Intn(1000000)),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("failed to marshal JSON: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		exchangeName,
		"",
		false,
		false,
		amqp091.Publishing{
			ContentType: "application/json",
			Body: body,
		},
	)
	if err != nil {
		log.Fatalf("failed to publish order %s from customer %s: %v", payload.OrderID, payload.CustomerID, err)
	}

	log.Printf("[Fanout Publisher] Broadcasted event to '%s': %s", exchangeName, string(body))
}
