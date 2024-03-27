package main

import (
	"eventdrivenrabbit/internal"
	"log"
	"time"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("vlpc", "secret", "localhost:5672", "customers")
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

	time.Sleep(10 * time.Second)
	log.Println(client)
}
