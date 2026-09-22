# RabbitMQ Event-Driven Order System

This project is a practical refresher on RabbitMQ and event-driven design written in Go. The core idea is simple but powerful: producers do not talk directly to consumers. Instead, they publish messages to RabbitMQ, and RabbitMQ routes those messages to the right queues and exchanges. In a publish/subscribe model, one event can fan out to many listeners; in a work queue, many workers compete to process the same backlog of tasks. This turns distributed systems into loosely coupled services that can evolve independently and handle load more gracefully.

The project was shaped around the phase-by-phase learning path in the repository docs and guided by the practical RabbitMQ and event-driven concepts taught in the community learning materials referenced at the end of this document.

## I. Work Queue and Competing Consumers

This is the first building block: one producer pushes jobs into a queue, and multiple consumers listen to the same queue. Each message is delivered to only one worker, which creates the classic competing-consumer pattern. That matters because it lets you scale processing horizontally without duplicating work. RabbitMQ keeps the queue durable and uses acknowledgments to make sure a message is not lost when a worker is still processing it.

In the order-system examples, the producer sends tasks into a shared queue and workers pull them off one by one. If several workers are started at the same time, the broker distributes tasks across them. The important concept is fairness: a worker should not be given more than one unacknowledged message at a time, preventing one slow consumer from starving the rest of the system.

### How to run and test it

1. Start RabbitMQ:

```bash
docker compose up -d
```

2. Start one or more workers in separate terminals:

```bash
go run ./cmd/consumers/inventory
```

3. Publish a task from the producer:

```bash
go run ./cmd/producer "Process Order Batch" 2000
```

4. Watch the logs in each terminal. You should see tasks being assigned to whichever worker is available, with each message processed once and acknowledged back to RabbitMQ.

5. Open the management UI at http://localhost:15672 and inspect the queue, consumers, and message flow.

This section is the foundation: it teaches how asynchronous backlog processing works without coupling the producer to a specific consumer process.

## II. Publish / Subscribe with Fanout

Once a queue is understood, the next pattern is broadcast. In a fanout exchange, a producer sends a single message and RabbitMQ pushes it to every queue that is bound to that exchange. This is the classic publish/subscribe model: one event reaches all interested subscribers, not just one consumer.

For the order system, that means a single order event can be consumed by multiple downstream services at the same time, such as notifications, analytics, and other observers that do not need to know about one another. The key idea is decoupling: the publisher does not care who is listening, and the subscribers do not care who published the event.

### How to run and test it

1. Make sure RabbitMQ is running.

2. Start two subscribers in separate terminals:

```bash
go run ./cmd/consumers/notification
go run ./cmd/consumers/analytics
```

3. Publish a fanout event:

```bash
go run ./cmd/producer/fanout
```

4. Observe both subscribers receiving the same event. Each queue gets its own copy, which is the behavioral signature of a fanout exchange.

5. In the RabbitMQ UI, inspect the exchange bindings. You will see the broadcast flow from one exchange to multiple queues.

This is an important step away from point-to-point messaging and toward event distribution across many services.

## III. Topic Routing

The next layer is selective routing. A topic exchange allows messages to be filtered by routing keys using patterns such as one-word wildcards and multi-word wildcards. Instead of broadcasting to every queue, a producer can send a message with a route like order.created.us, and only matching consumers receive it.

This is especially useful in event-driven systems where different services are interested in different slices of the same event stream. A fulfillment service may care only about US deliveries, while an audit consumer may care about every order event. Topic routing makes that segmentation explicit and efficient.

### How to run and test it

1. Start the topic-based consumers in separate terminals:

```bash
go run ./cmd/consumers/fulfillment
go run ./cmd/consumers/audit
```

2. Publish a route-specific event:

```bash
go run ./cmd/producer/topic order.created.us ord_301
```

3. Try other routing keys to see different outcomes:

```bash
go run ./cmd/producer/topic order.cancelled.eu ord_302
```

4. Watch which consumers receive which messages. The fulfillment service should react only to the matching US event, while the audit service can pick up broader order traffic.

5. Check the management UI to compare the binding patterns and active queue activity.

This section is where RabbitMQ starts to feel like an event router instead of a simple queueing system.

## IV. Resilience, Dead Letter Handling, and Retries

The final concept focuses on failure handling. A message can be malformed, a consumer can crash while processing, or a downstream dependency can fail. In those scenarios, a robust system should not silently lose work. RabbitMQ supports dead-letter exchanges, rejection patterns, and idempotency checks so that failed messages can be inspected, quarantined, and retried intentionally.

In this project, a payment-processing consumer deliberately simulates failure, rejects the message without requeueing it, and sends it to a dead-letter queue. A DLQ inspector watches that queue so the team can investigate what went wrong. This turns a noisy failure into a visible, manageable operational event.

### How to run and test it

1. Start the processing consumer and the DLQ inspector:

```bash
go run ./cmd/consumers/payment
go run ./cmd/consumers/dlq_inspector
```

2. Publish a failing payment message:

```bash
go run ./cmd/producer/resilient ord_999 50 fail
```

3. Watch the logs. The payment worker should reject the message, RabbitMQ should route it to the dead-letter exchange, and the DLQ inspector should show the dead-lettered payload and metadata.

4. Try a success case with a valid amount:

```bash
go run ./cmd/producer/resilient ord_1000 150
```

5. Compare the successful flow to the failing flow. The difference is what makes message resilience visible and testable.

This is the most operationally important section because it teaches how long-lived systems survive poor messages, unexpected failures, and duplicates without losing traceability.

## Common startup flow

For local development, the simplest sequence is:

```bash
docker compose up -d
go mod download
```

Then start the relevant producers and consumers for the pattern you want to explore. RabbitMQ Management stays available at http://localhost:15672 for queue inspection, exchange bindings, and message diagnostics.

## What this project teaches

This repository is deliberately small, but the concepts it exercises are foundational:

- producers and consumers are decoupled
- queues distribute work among competing workers
- fanout patterns broadcast one event to many listeners
- topic routing filters traffic by interest
- dead-letter handling protects systems from poison messages
- acknowledgments and idempotency are essential to reliable messaging

The practical value is not just the code; it is the mental model. Once you understand how messages flow through exchanges, queues, and consumers, the architecture becomes easier to reason about in larger distributed systems.

## Credit

This learning project builds on the practical teaching approach and RabbitMQ workshop style shared by [athallarizky/agent-playbooks](https://github.com/athallarizky/agent-playbooks). The project is a personal revisit to these concepts in a Go-based, order-processing context to reinforce understanding through practice.
