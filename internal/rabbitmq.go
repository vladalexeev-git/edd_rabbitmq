package internal

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

// RabbitClient is used to keep track of RabbitMQ connection
type RabbitClient struct {
	conn *amqp.Connection
	//The channel that processes sends messages
	ch *amqp.Channel
}

// ConnectRabbitMQ will spawn a Connection
func ConnectRabbitMQ(username, password, host, vhost string) (*amqp.Connection, error) {

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", username, password, host, vhost))
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// NewRabbitMQClient will connect and return a Rabbitclient with an open connection
// Accepts an amqp Connection to be reused, to avoid spawning one TCP connection per concurrent client
func NewRabbitMQClient(conn *amqp.Connection) (RabbitClient, error) {
	const op = "internal.NewRabbitMQClient"
	// Unique, Conncurrent Server Channel to process/send messages
	// A good rule of thumb is to always REUSE Conn across applications
	// But spawn a new Channel per routine
	ch, err := conn.Channel()
	if err != nil {
		return RabbitClient{}, fmt.Errorf("%s: %w", op, err)
	}

	if err := ch.Confirm(false); err != nil {
		return RabbitClient{}, fmt.Errorf("%s: %w", op, err)
	}

	return RabbitClient{
		conn: conn,
		ch:   ch,
	}, nil
}

// Close will close the channel
func (rc RabbitClient) Close() error {
	return rc.ch.Close()
}

// CreateQueue will create a new queue based on given cfgs
func (rc RabbitClient) CreateQueue(queueName string, durable, autodelete bool) (amqp.Queue, error) {
	q, err := rc.ch.QueueDeclare(queueName, durable, autodelete, false, false, nil)
	if err != nil {
		return amqp.Queue{}, nil
	}

	return q, nil
}

// CreateBinding is used to connect a queue to an Exchange using the binding rule
func (rc RabbitClient) CreateBinding(name, binding, exchange string) error {
	// leaving nowait false, having nowait set to false will cause the channel to return an error and close if it cannot bind
	// the final argument is the extra headers, but we won't be doing that now

	return rc.ch.QueueBind(name, binding, exchange, false, nil)
}

// Send is used to publish a payload onto an exchange with a given routing key
func (rc RabbitClient) Send(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {
	// PublishWithDeferredConfirmWithContext will wait for server to ACK the message
	confirmation, err := rc.ch.PublishWithDeferredConfirmWithContext(ctx,
		exchange,   // exchange
		routingKey, // routing key
		// Mandatory is used when we HAVE to have the message return an error, if there is no route or queue then
		// setting this to true will make the message bounce back
		// If this is False, and the message fails to deliver, it will be dropped
		true, // mandatory
		// immediate Removed in MQ 3 or up https://blog.rabbitmq.com/posts/2012/11/breaking-things-with-rabbitmq-3-0§
		false,   // immediate
		options, // amqp publishing struct
	)
	if err != nil {
		return err
	}
	// Blocks until ACK from Server is receieved
	log.Println(confirmation.Wait())
	return nil

	//PublishWithContext without blocking:
	//return rc.ch.PublishWithContext(ctx,
	//	exchange,   // exchange
	//	routingKey, // routing key
	//	// Mandatory is used when we HAVE to have the message return an error, if there is no route or queue then
	//	// setting this to true will make the message bounce back
	//	// If this is False, and the message fails to deliver, it will be dropped
	//	true, // mandatory
	//	// immediate Removed in MQ 3 or up https://blog.rabbitmq.com/posts/2012/11/breaking-things-with-rabbitmq-3-0§
	//	false,   // immediate
	//	options, // amqp publishing struct
	//)
}

// Consume is a wrapper around consume, it will return a Channel that can be used to digest messages
// Queue is the name of the queue to Consume
// Consumer is a unique identifier for the service instance that is consuming, can be used to cancel etc
// autoAck is important to understand, if set to true, it will automatically Acknowledge that processing is done
// This is good, but remember that if the Process fails before completion, then an ACK is already sent, making a message lost
// if not handled properly
func (rc RabbitClient) Consume(queue, consumer string, autoAck bool) (<-chan amqp.Delivery, error) {
	return rc.ch.Consume(queue, consumer, autoAck, false, false, false, nil)
}

// ApplyQos is used to apply quality of service to the channel
// Prefetch count - How many messages the server will try to keep on the Channel
// prefetch Size - How many Bytes the server will try to keep on the channel
// global -- Any other Consumers on the connection in the future will apply the same rules if TRUE
func (rc RabbitClient) ApplyQos(count, size int, global bool) error {
	// Apply Quality of Serivce
	return rc.ch.Qos(
		count,
		size,
		global,
	)
}
