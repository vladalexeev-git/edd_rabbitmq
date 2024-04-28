package main

import (
	"context"
	"eventdrivenrabbit/internal"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

func main() {
	// conn for Pushing or Publishing
	conn, err := internal.ConnectRabbitMQ("vlpc", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Never use the same Connection for Consume and Publish, so create conn for consuming
	consumeConn, err := internal.ConnectRabbitMQ("vlpc", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer consumeConn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	consumeClient, err := internal.NewRabbitMQClient(consumeConn)
	if err != nil {
		panic(err)
	}
	defer consumeClient.Close()

	// Create Unnamed Queue which will generate a random name, set AutoDelete to True
	queue, err := consumeClient.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	if err := consumeClient.CreateBinding(queue.Name, queue.Name, "customer_callbacks"); err != nil {
		panic(err)
	}

	messageBus, err := consumeClient.Consume(queue.Name, "customer-api", true)
	if err != nil {
		panic(err)
	}

	go func() {
		for message := range messageBus {
			log.Printf("Message Callback %s\n", message.CorrelationId)
		}
	}()

	// Create context to manage timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create customer from sweden
	for i := 1; i <= 10; i++ {
		if err := client.Send(ctx, "customer_events", "customers.created.se", amqp091.Publishing{
			ContentType:  "text/plain",       // The payload we send is plaintext, could be JSON or others.
			DeliveryMode: amqp091.Persistent, // This tells rabbitMQ that this message should be Saved if no resources accepts it before a restart (durable)
			Body:         []byte("An cool message between services"),
			// We add a REPLYTO which defines the
			ReplyTo: queue.Name,
			// CorrelationId can be used to know which Event this relates to
			CorrelationId: fmt.Sprintf("customer_created_%d", i),
		}); err != nil {
			panic(err)
		}
	}

	//if err := client.Send(ctx, "customer_events", "customers.test", amqp091.Publishing{
	//	ContentType:  "text/plain",
	//	DeliveryMode: amqp091.Transient, // This tells rabbitMQ that this message can be deleted if no resources accepts it before a restart (non-durable)
	//	Body:         []byte("A second cool message"),
	//}); err != nil {
	//	panic(err)
	//}

	var blocking chan struct{}
	log.Println(client)

	log.Println("Waiting on Callbacks, to close the program press CTRL+C")
	// This will block forever
	<-blocking
}
