package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"order-system/pkg/config"
	"os"
	"strings"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type EventPayload struct {
	EventID    string    `json:"eventId"`
	RoutingKey string    `json:"routingKey"`
	Timestamp  time.Time `json:"timestamp"`
	OrderID    string    `json:"orderId"`
	Amount     float64   `json:"amount"`
}

func main() {
	conn, ch, err := config.ConnectRabbitMQ()
	if err != nil {
		log.Fatalf("failed to setup RabbitMQ: %v", err)
	}
	defer conn.Close()
	defer ch.Close()

	exchangeName := "order_topic_exchange"

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
	
	region := "au"
	orderID := fmt.Sprintf("ord_%d", rand.Intn(10000))

	if len(os.Args) > 1 {
		region = os.Args[1]
	}
	if len(os.Args) > 2 {
		orderID = fmt.Sprintf("ord_%s", os.Args[2])
	}

	routingKey := fmt.Sprintf("order.created.%s", region)

	body, err := json.Marshal(EventPayload{
		EventID: fmt.Sprintf("evt_%d", time.Now().Unix()),
		RoutingKey: routingKey,
		Timestamp: time.Now(),
		OrderID: orderID,
		Amount: float64(rand.Intn(1000000)),
	})
	if err != nil {
		log.Fatalf("error marshalling event payload: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		exchangeName,
		routingKey,
		false,
		false,
		amqp091.Publishing{
			ContentType: "application/json",
			Body: body,
		},
	)
	if err != nil {
		log.Fatalf("error publishing message: %v", err)
	}

	log.Printf("[Topic Publisher] Published Order %s from %s to key '%s': %s", orderID, strings.ToUpper(region), routingKey, string(body))
}
