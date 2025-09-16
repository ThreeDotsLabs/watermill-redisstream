package nonpersistent

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

// Publisher publishes messages to Redis Pub/Sub channels.
type Publisher struct {
	ctx        context.Context
	rc         redis.UniversalClient
	marshaller Marshaller
	logger     watermill.LoggerAdapter
	closed     bool
}

// NewPublisher creates a new Redis Pub/Sub publisher.
func NewPublisher(ctx context.Context, rc redis.UniversalClient, marshaller Marshaller, logger watermill.LoggerAdapter) (message.Publisher, error) {
	if logger == nil {
		logger = &watermill.NopLogger{}
	}
	return &Publisher{
		ctx:        ctx,
		rc:         rc,
		marshaller: marshaller,
		logger:     logger,
		closed:     false,
	}, nil
}

// Publish sends messages to a Redis Pub/Sub channel.
// Blocking call: waits for Redis PUBLISH response.
func (p *Publisher) Publish(topic string, msgs ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	logFields := watermill.LogFields{"topic": topic}

	for _, msg := range msgs {
		logFields["message_uuid"] = msg.UUID
		p.logger.Trace("Publishing message to Redis Pub/Sub", logFields)

		payload, err := p.marshaller.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		// Redis Pub/Sub Publish
		cmd := p.rc.Publish(p.ctx, topic, payload)
		count, err := cmd.Result()
		if err != nil {
			return errors.Wrapf(err, "cannot publish message %s", msg.UUID)
		}

		logFields["subscribers_received"] = count
		p.logger.Trace("Message published to Redis Pub/Sub", logFields)
	}

	return nil
}

// Close marks the publisher as closed. Does NOT close the Redis client.
func (p *Publisher) Close() error {
	p.closed = true
	p.logger.Debug("Redis Pub/Sub publisher closed", nil)
	return nil
}
