package persistent

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

// Publisher publishes messages to redis streams via XADD.
type Publisher struct {
	ctx        context.Context
	config     PublisherConfig
	rc         redis.UniversalClient
	marshaller Marshaller

	logger watermill.LoggerAdapter

	closed bool
}

// NewPublisher creates a new redis stream Publisher.
func NewPublisher(ctx context.Context, config PublisherConfig, rc redis.UniversalClient, marshaller Marshaller, logger watermill.LoggerAdapter) (message.Publisher, error) {
	if logger == nil {
		logger = &watermill.NopLogger{}
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Publisher{
		ctx:        ctx,
		config:     config,
		rc:         rc,
		marshaller: marshaller,
		logger:     logger,
		closed:     false,
	}, nil
}

type PublisherConfig struct {
	Maxlens map[string]int64
}

func (sc *PublisherConfig) Validate() error {
	for topic, maxlen := range sc.Maxlens {
		if maxlen < 0 {
			sc.Maxlens[topic] = 0
		}
	}
	return nil
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

		values, err := p.marshaller.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		maxlen, ok := p.config.Maxlens[topic]
		if !ok {
			maxlen = 0
		}

		id, err := p.rc.XAdd(context.Background(), &redis.XAddArgs{
			Stream: topic,
			Values: values,
			MaxLen: maxlen,
			Approx: true,
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
