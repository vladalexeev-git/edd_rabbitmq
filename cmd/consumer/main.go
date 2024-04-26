package main

import (
	"context"
	"eventdrivenrabbit/internal"
	"golang.org/x/sync/errgroup"
	"log"
	"time"
)

func main() {

	conn, err := internal.ConnectRabbitMQ("vlad", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}

	mqClient, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}

	messageBus, err := mqClient.Consume("customers_created", "email-service", false)
	if err != nil {
		panic(err)
	}

	// blocking is used to block forever
	var blocking chan struct{}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Create an Errgroup to manage concurrecy
	g, ctx := errgroup.WithContext(ctx)

	// Set amount of concurrent tasks
	g.SetLimit(10)

	go func() {
		for message := range messageBus {
			msg := message
			// Spawn a worker
			g.Go(func() error {
				log.Printf("New Message: %v", string(msg.Body))

				//if !message.Redelivered {
				//	// Nack multiple, Set Requeue to true
				//	message.Nack(false, true)
				//
				//}
				time.Sleep(2 * time.Second)
				//Multiple means that we acknowledge a batch of messages, leave false for now
				if err := message.Ack(false); err != nil {
					log.Printf("Acknowledged message failed: Retry ? Handle manually %s\n", msg.MessageId)
					return err
				}
				log.Printf("Acknowledged message %s\n", msg.MessageId)
				return nil
			})

		}
	}()
	//go func() {
	//	for message := range messageBus {
	//		log.Printf("New Message: %v", message)
	//		panic("Whops I failed here for some reason")
	//
	//	}
	//}()

	log.Println("Consuming..., to close the program press CTRL+C")
	// This will block forever
	<-blocking

}
