package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"order-system/pkg/config"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type TaskPayload struct {
	TaskID string `json:"taskId"`
	Description string `json:"description"`
	DurationMs int `json:"durationMs"`
}

func main() {
	conn, ch, err := config.ConnectRabbitMQ()
	if err != nil {
		log.Fatalf("failed setup: %v", err)
	}
	defer conn.Close()
	defer ch.Close()

	queueName := "task_queue"

	// Declare queue
	q, err := ch.QueueDeclare(
		queueName,	// name
		true,		// durable (true: keeps queue intact across broker restarts)
		false,		// delete when unused (false: without active consumers will survive and not be removed)
		false,		// exclusive (false: not only accessible by the connection that declares them and will not be deleted when the connection closes)
		false,		// no-wait (false: a channel exception will not arrive if the conditions are met for existing queues or attempting to modify an existing queue from a different connection.)
		nil,		// arguments
	)
	if err != nil {
		log.Fatalf("failed to declare queue: %v", err)
	}

	taskName := "Process Order Batch"
	durationMs := 2000
	if len(os.Args) > 1 {
		taskName = os.Args[1]
	}
	if len(os.Args) > 2 {
		if d, err := strconv.Atoi(os.Args[2]); err != nil {
			durationMs = d
		}
	}

	payload := TaskPayload{
		TaskID: fmt.Sprintf("task_%d", time.Now().UnixNano()),
		Description: taskName,
		DurationMs: durationMs,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("failed to marshal JSON: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		"", // exchange (empty string = default direct exchange)
		q.Name, // routing key
		false, // mandatory (false: publishings would still be deliverable when no matching queue is bound)
		false, // immediate (false: publishings would still be deliverable even if no consumer on the matched queue)
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // persistent messages will be restored to durable queues and lost on non-durable queues during server restart
			ContentType: "application/json",
			Body: body,
		},
	)
	if err != nil {
		log.Fatalf("failed to publish message: %v", err)
	}

	log.Printf("[Producer] Sent task to queue '%s': %s", q.Name, string(body))
}
