package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"order-system/pkg/config"
	"os"
	"os/signal"
	"time"
)

type TaskPayload struct {
    TaskID      string `json:"taskId"`
    Description string `json:"description"`
    DurationMs  int    `json:"durationMs"`
}

func main() {
	rand.Seed(time.Now().UnixNano())
	workerID := fmt.Sprintf("worker_%d", rand.Intn(1000))

	conn, ch, err := config.ConnectRabbitMQ()
	if err != nil {
		log.Fatalf("failed setup :%v", err)
	}
	defer conn.Close()
	defer ch.Close()

	queueName := "task_queue"

	_, err = ch.QueueDeclare(
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

	// Fair dispatch: prefetch = 1
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Fatalf("Failed to set QoS: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgs, err := ch.ConsumeWithContext(
		ctx,
		queueName, // queue
		"", // consumer tag (if empty string, generate a unique identity)
		false, // auto-ack
		false, // exclusive (false: the server will fairly distribute deliveries across multiple consumers in this queue.)
		false, // no-local (not supported by RabbitMQ. must use different connections for publish and consume)
		false, // no-wait (false: wait for the server to confirm the request to begin deliveries)
		nil, // args
	)
	if err != nil {
		log.Fatalf("failed to register consumer: %v", err)
	}

	log.Printf("[%s] Waiting for tasks in '%s'. TO exit press CTRL+C", workerID, queueName)

	// var wg sync.WaitGroup
	// wg.Add(1)

	go func() {
		// defer wg.Done()
		for d := range msgs {
			var task TaskPayload
			if err := json.Unmarshal(d.Body, &task); err != nil {
				log.Printf("[%s] error unmarshalling body: %v", workerID, err)
				d.Nack(false, false)
			}

			log.Printf("[%s] received task: %s (%s)", workerID, task.TaskID, task.Description)

			// Simulate processing time
			time.Sleep(time.Duration(task.DurationMs) * time.Millisecond)

			log.Printf("[%s] Done processing task: %s", workerID, task.TaskID)

			// send explicit ACK to RabbitMQ
			d.Ack(false)
		}
	}()

	// wait for OS signal, then cancel context to stop Consumer
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	log.Println("Shutdown signal received, stopping consumer...")
	cancel()

	// wait for worker goroutine to exit (draining remaining deliveries)
	// wg.Wait()
	log.Println("consumer stopped, exiting...")
}
