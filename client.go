package queue

import (
	"fmt"
	"log"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

const (
	exchangeType = "topic"
)

// Client abstracts the RabbitMQ interactions.
// It opens up exactly one channel and automatically puts it into transaction mode.
type Client struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewClient instantiates a new RabbitMQ client.
func NewClient(host, user string) (*Client, error) {
	conn, err := amqp.Dial(formatURL(host, user))
	if err != nil {
		return nil, errors.Wrap(err, "Error dialing the RabbitMQ instance")
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "Error opening RabbitMQ channel")
	}
	if err = ch.Tx(); err != nil {
		return nil, errors.Wrap(err, "Error putting the channel into transaction mode")
	}
	return &Client{conn: conn, channel: ch}, nil
}

// Commit commits the channel transaction and starts a new transaction
func (c *Client) Commit() error {
	return c.channel.TxCommit()
}

// Register declares a queue, an exchange if they weren't done so yet
// and binds them together with topic exchange type.
func (c *Client) Register(exchange, queue, keyPattern string) error {
	defer c.Commit()

	log.Println("Declaring exchange... | Exchange:", exchange, "| Type:", exchangeType)
	err := c.channel.ExchangeDeclare(
		exchange,
		exchangeType,
		true,
		false,
		false,
		false,
		nil)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Error declaring exchange '%s'", exchange))
	}

	log.Println("Declaring queue... | Queue:", queue)
	q, err := c.channel.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Error declaring queue '%s'", queue))
	}

	log.Println("Binding queue to the exchange... | Exchange:", exchange, "| Queue:", q.Name, "| Pattern:", keyPattern)
	err = c.channel.QueueBind(
		q.Name,
		keyPattern,
		exchange,
		false,
		nil)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Error binding queue '%s' to the exchange '%s'", q.Name, exchange))
	}

	return nil
}

// Publish publishes a new message to the RabbitMQ instance
func (c *Client) Publish(exchange, key string, body []byte) error {
	defer c.Commit()
	log.Println("body = ", string(body))

	err := c.channel.Publish(
		exchange, // exchange
		key,      // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "text/json",
			Body:        body,
		})

	return errors.Wrap(err, "error publishing the message")
}

// Consume opens a channel that receives messages from the given queue
func (c *Client) Consume(queue, consumer string, autoAck bool) (<-chan amqp.Delivery, error) {
	defer c.Commit()

	return c.channel.Consume(queue, consumer, autoAck, false, false, false, nil)
}

// Close closes the connection and its channel
func (c *Client) Close() error {
	if err := c.channel.Close(); err != nil {
		return errors.Wrap(err, "Error closing channel")
	}
	if err := c.conn.Close(); err != nil {
		return errors.Wrap(err, "Error closing connection")
	}
	return nil
}

func formatURL(host, user string) string {
	return fmt.Sprintf("amqp://%s:%s@%s", user, user, host)
}
