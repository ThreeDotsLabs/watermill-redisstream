package redisstream

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

type Publisher struct {
	config PublisherConfig
	client redis.UniversalClient
	logger watermill.LoggerAdapter

	closed     bool
	closeMutex sync.Mutex
}

// NewPublisher creates a new redis stream Publisher.
func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = &watermill.NopLogger{}
	}

	return &Publisher{
		config: config,
		client: config.Client,
		logger: logger,
		closed: false,
	}, nil
}

type PublisherConfig struct {
	Client     redis.UniversalClient
	Marshaller Marshaller
	Maxlens    map[string]int64
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaller == nil {
		c.Marshaller = DefaultMarshallerUnmarshaller{}
	}
}

func (c *PublisherConfig) Validate() error {
	if c.Client == nil {
		return errors.New("redis client is empty")
	}
	for topic, maxlen := range c.Maxlens {
		if maxlen < 0 {
			// zero maxlen stream indicates unlimited stream length
			c.Maxlens[topic] = 0
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

		values, err := p.config.Marshaller.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		maxlen, ok := p.config.Maxlens[topic]
		if !ok {
			maxlen = 0
		}

		id, err := p.client.XAdd(context.Background(), &redis.XAddArgs{
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
	p.closeMutex.Lock()
	defer p.closeMutex.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	if err := p.client.Close(); err != nil {
		return err
	}

	return nil
}
