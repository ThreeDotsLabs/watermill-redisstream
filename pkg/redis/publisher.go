package redis

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

type Publisher struct {
	ctx       context.Context
	rc        redis.UniversalClient
	marshaler Marshaler

	logger watermill.LoggerAdapter

	closed bool
}

// NewPublisher creates a new redis stream Publisher.
func NewPublisher(ctx context.Context, rc redis.UniversalClient, marshaller Marshaler, logger watermill.LoggerAdapter) (message.Publisher, error) {
	if logger == nil {
		logger = &watermill.NopLogger{}
	}

	return &Publisher{ctx, rc, marshaller, logger, false}, nil
}

// Publish publishes message to redis stream
//
// Publish is blocking and wait for redis response
// When one of messages delivery fails - function is interrupted.
func (p *Publisher) Publish(topic string, msgs ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	logFields := make(watermill.LogFields, 3)
	logFields["topic"] = topic

	for _, msg := range msgs {
		logFields["message_uuid"] = msg.UUID
		p.logger.Trace("Sending message to redis stream", logFields)

		values, err := p.marshaler.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		id, err := p.rc.XAdd(context.Background(), &redis.XAddArgs{
			Stream: topic,
			Values: values,
		}).Result()
		if err != nil {
			return errors.Wrapf(err, "cannot xadd message %s", msg.UUID)
		}

		logFields["xadd_id"] = id
		p.logger.Trace("Message sent to redis stream", logFields)
	}

	return nil
}

func (p *Publisher) Close() error {
	p.closed = true
	// do not close redis client here
	return nil
}
