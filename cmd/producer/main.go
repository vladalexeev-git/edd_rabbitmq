package main

import (
	"context"
	"eventdrivenrabbit/internal"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("vlad", "secret", "localhost:5672", "customers")
	if err != nil {
		log.Fatal(err)
	}

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.CreateQueue("customers_created", true, false); err != nil {
		panic(err)
	}

	if err := client.CreateQueue("customers_test", false, true); err != nil {
		panic(err)
	}

	// Create binding between the customer_events exchange and the customers-created queue
	if err := client.CreateBinding("customers_created", "customers.created.*", "customer_events"); err != nil {
		panic(err)
	}

	// Create binding between the customer_events exchange and the customers-test queue
	if err := client.CreateBinding("customers_test", "customers.*", "customer_events"); err != nil {
		panic(err)
	}

	// Create context to manage timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create customer from sweden
	for i := 0; i < 10; i++ {
		if err := client.Send(ctx, "customer_events", "customers.created.se", amqp091.Publishing{
			ContentType:  "text/plain",       // The payload we send is plaintext, could be JSON or others.
			DeliveryMode: amqp091.Persistent, // This tells rabbitMQ that this message should be Saved if no resources accepts it before a restart (durable)
			Body:         []byte("An cool message between services"),
		}); err != nil {
			panic(err)
		}

		if err := client.Send(ctx, "customer_events", "customers.test", amqp091.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp091.Transient, // This tells rabbitMQ that this message can be deleted if no resources accepts it before a restart (non-durable)
			Body:         []byte("A second cool message"),
		}); err != nil {
			panic(err)
		}
	}

	//log.Println(client)
	//
	//
	//time.Sleep(10 * time.Second)
	log.Println(client)
}
