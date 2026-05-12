package redisstream

import (
	"context"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// NoPublishTimeout can be set as PublisherConfig.PublishTimeout to disable
// the per-XAdd deadline. Use only when callers always pass a context with
// their own deadline via msg.Context() and you want no library-imposed cap.
const NoPublishTimeout time.Duration = -1

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
	Client        redis.UniversalClient
	Marshaller    Marshaller
	Maxlens       map[string]int64
	DefaultMaxlen int64

	// PublishTimeout caps the duration of each XAdd call. Defaults to 15s
	// when zero. Set to NoPublishTimeout to disable the library-imposed
	// deadline (caller's msg.Context() still applies).
	PublishTimeout time.Duration
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaller == nil {
		c.Marshaller = DefaultMarshallerUnmarshaller{}
	}
	if c.PublishTimeout == 0 {
		c.PublishTimeout = 15 * time.Second
	}
}

func (c *PublisherConfig) Validate() error {
	if c.Client == nil {
		return errors.New("redis client is empty")
	}
	for topic, maxlen := range c.Maxlens {
		if maxlen < 0 {
			// zero maxlen stream indicates unlimited stream length
			c.Maxlens[topic] = c.DefaultMaxlen
		}
	}
	return nil
}

// Publish publishes message to redis stream
//
// Publish is blocking and waits for redis response.
// When any of messages delivery fails - function is interrupted.
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
			maxlen = p.config.DefaultMaxlen
		}

		if err := p.checkPoolNotExhausted(); err != nil {
			return errors.Wrapf(err, "cannot xadd message %s", msg.UUID)
		}

		ctx := msg.Context()
		var cancel context.CancelFunc
		if p.config.PublishTimeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, p.config.PublishTimeout)
		}
		id, err := p.client.XAdd(ctx, &redis.XAddArgs{
			Stream: topic,
			Values: values,
			MaxLen: maxlen,
			Approx: true,
		}).Result()
		if cancel != nil {
			cancel()
		}
		if err != nil {
			return errors.Wrapf(err, "cannot xadd message %s", msg.UUID)
		}

		logFields["xadd_id"] = id
		p.logger.Trace("Message sent to redis stream", logFields)
	}

	return nil
}

// checkPoolNotExhausted returns an error if the client's connection pool is
// currently fully utilized, so Publish can fail fast instead of queueing on
// PoolTimeout / PublishTimeout. Skipped when the pool size cannot be
// determined (non-standard UniversalClient implementations).
//
// Cost: ~16ns per call (one PoolStats read + a type switch). Negligible
// relative to a network XAdd.
func (p *Publisher) checkPoolNotExhausted() error {
	poolSize, ok := effectivePoolSize(p.client)
	if !ok {
		return nil
	}
	stats := p.client.PoolStats()
	if stats == nil {
		return nil
	}
	inUse := int(stats.TotalConns - stats.IdleConns)
	if inUse >= poolSize {
		return errors.Errorf(
			"redis pool of size %d is exhausted (%d connections in use); "+
				"Publish would block. Increase Client.PoolSize or reduce concurrent "+
				"long-blocking operations (e.g. Subscriber reads on the same client)",
			poolSize, inUse,
		)
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
