package main

import (
	"context"
	"encoding/json"
	"log"
	"order-system/pkg/config"
	"os"
	"os/signal"
)

type EventPayload struct {
	OrderID string  `json:"orderId"`
	Amount  float64 `json:"amount"`
}

func main() {
	conn, ch, err := config.ConnectRabbitMQ()
	if err != nil {
		log.Fatalf("failed to setup rabbitmq: %v", err)
	}
	defer conn.Close()
	defer ch.Close()

	exchangeName := "order_topic_exchange"
	bindingPattern := "order.#" // Catches ALL events in the order domain

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

	queueName := "audit_log_queue"

	q, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("error declaring queue: %v", err)
	}

	err = ch.QueueBind(
		q.Name,
		bindingPattern,
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed binding queue: %v", err)
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
		log.Fatalf("error consumer register: %v", err)
	}

	log.Printf("[Audit Log Service] Subscribed to pattern: '%s'", bindingPattern)

	go func() {
		for d := range msgs {
			var payload EventPayload
			err = json.Unmarshal(d.Body, &payload)
			if err != nil {
				log.Printf("error unmarshalling json: %v", err)
				d.Nack(false, false)
			}
			log.Printf("[Audit Log] 📑 AUDIT EVENT: key='%s' payload=Order #%s ($%.2f)", d.RoutingKey, payload.OrderID, payload.Amount)
			d.Ack(false)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	log.Println("Shutdown signal received, stopping consumer...")
	cancel()
}
