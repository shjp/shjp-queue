package queue

import (
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/shjp/shjp-core"
)

// Producer wraps the RabbitMQ client to abstract producer operations
type Producer struct {
	client *Client
}

// NewProducer instantiates a new producer
func NewProducer(host, user string) (*Producer, error) {
	c, err := NewClient(host, user)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating a new producer")
	}
	return &Producer{client: c}, nil
}

// Publish publishes a new message to the RabbitMQ instance
func (p *Producer) Publish(exchange string, msg *core.Message) error {
	raw, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "Error marshalling message")
	}

	return p.client.Publish(exchange, msg.Key, raw)
}
