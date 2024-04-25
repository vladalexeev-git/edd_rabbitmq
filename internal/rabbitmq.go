package internal

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
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
// Accepts a amqp Connection to be reused, to avoid spawning one TCP connection per concurrent client
func NewRabbitMQClient(conn *amqp.Connection) (RabbitClient, error) {
	// Unique, Conncurrent Server Channel to process/send messages
	// A good rule of thumb is to always REUSE Conn across applications
	// But spawn a new Channel per routine
	ch, err := conn.Channel()
	if err != nil {
		return RabbitClient{}, err
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
func (rc RabbitClient) CreateQueue(queueName string, durable, autodelete bool) error {
	_, err := rc.ch.QueueDeclare(queueName, durable, autodelete, false, false, nil)
	return err
}

// CreateBinding is used to connect a queue to an Exchange using the binding rule
func (rc RabbitClient) CreateBinding(name, binding, exchange string) error {
	// leaving nowait false, having nowait set to false will cause the channel to return an error and close if it cannot bind
	// the final argument is the extra headers, but we won't be doing that now

	return rc.ch.QueueBind(name, binding, exchange, false, nil)
}

// Send is used to publish a payload onto an exchange with a given routing key
func (rc RabbitClient) Send(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {
	return rc.ch.PublishWithContext(ctx,
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
}
