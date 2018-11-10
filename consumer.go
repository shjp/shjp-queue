package queue

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"

	"github.com/shjp/shjp-core"
)

// Consumer is a wrapper around RabbitMQ client to abstract consumer operations
type Consumer struct {
	client     *Client
	name       string
	queueName  string
	deliveries <-chan amqp.Delivery
	done       chan bool
}

// HandleMessage is a callback function to handle when a message is consumed
type HandleMessage func(msg *core.Message)

// NewConsumer creates a new consumer client
func NewConsumer(host, user, origin string, intent core.Intent) (*Consumer, error) {
	c, err := NewClient(host, user)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating a new consumer")
	}

	queueName := composeQueueName(origin, intent)
	keyPattern := composeTopicExchangePattern(intent)

	if err = c.Register(string(intent), queueName, keyPattern); err != nil {
		return nil, errors.Wrap(err, "Error registering queue")
	}
	consumerName := composeConsumerName(origin, intent)
	log.Println("Initializing the Queue Consumer... | Name:", consumerName, "| Queue:", queueName)
	return &Consumer{
		client:    c,
		name:      consumerName,
		queueName: queueName,
	}, nil
}

// Consume starts consuming messages and invokes handler for each message received
func (c *Consumer) Consume(autoAck bool, handle HandleMessage) error {
	var err error
	c.deliveries, err = c.client.Consume(c.queueName, c.name, autoAck)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Error starting to consume for consumer '%s'", c.queueName))
	}

	go func() {
		for {
			select {
			case d := <-c.deliveries:
				handle(convertToMessage(d))
			case <-c.done:
				log.Println("Closing consumer connection")
				return
			}
		}
	}()

	return nil
}

// Stop stops consuming messages from the queue
func (c *Consumer) Stop() {
	c.done <- true
}

func convertToMessage(d amqp.Delivery) *core.Message {
	var msg core.Message
	err := json.Unmarshal(d.Body, &msg)
	if err != nil {
		return core.ReadFailureMessage(d.Body, err)
	}
	return &msg
}

func composeConsumerName(origin string, intent core.Intent) string {
	return origin + ":" + string(intent)
}
