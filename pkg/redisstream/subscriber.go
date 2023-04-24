package redisstream

import (
	"context"
	"sync"
	"time"

	"github.com/Rican7/retry"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

const (
	groupStartid   = ">"
	redisBusyGroup = "BUSYGROUP Consumer Group name already exists"
)

const (
	// NoSleep can be set to SubscriberConfig.NackResendSleep
	NoSleep time.Duration = -1

	DefaultBlockTime time.Duration = time.Millisecond * 100

	DefaultMaxFetch int64 = 256

	DefaultClaimInterval time.Duration = time.Second * 5

	// Default max idle time for pending message.
	// After timeout, the message will be claimed and its idle consumer will be removed from consumer group
	DefaultMaxIdleTime time.Duration = time.Second * 60
)

type Subscriber struct {
	config        SubscriberConfig
	client        redis.UniversalClient
	logger        watermill.LoggerAdapter
	closing       chan struct{}
	subscribersWg sync.WaitGroup

	closed     bool
	closeMutex sync.Mutex
}

// NewSubscriber creates a new redis stream Subscriber
func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = &watermill.NopLogger{}
	}

	return &Subscriber{
		config:  config,
		client:  config.Client,
		logger:  logger,
		closing: make(chan struct{}),
	}, nil
}

type SubscriberConfig struct {
	Client redis.UniversalClient

	Unmarshaller Unmarshaller

	// Redis stream consumer id, paired with ConsumerGroup
	Consumer string
	// When empty, fan-out mode will be used
	ConsumerGroup string

	// How long after Nack message should be redelivered
	NackResendSleep time.Duration

	// Block to wait next redis stream message
	BlockTime time.Duration

	// Max number of fetched messages in each read
	MaxFetch int64

	// Claim idle pending message interval
	ClaimInterval time.Duration

	// How long should we treat a consumer as offline
	MaxIdleTime time.Duration

	// Start consumption from the specified message ID
	// When using "0", the consumer group will consume from the very first message
	// When using "$", the consumer group will consume from the latest message
	OldestId string
}

func (sc *SubscriberConfig) setDefaults() {
	if sc.Unmarshaller == nil {
		sc.Unmarshaller = DefaultMarshallerUnmarshaller{}
	}
	if sc.Consumer == "" {
		sc.Consumer = watermill.NewShortUUID()
	}
	if sc.NackResendSleep == 0 {
		sc.NackResendSleep = NoSleep
	}
	if sc.BlockTime == 0 {
		sc.BlockTime = DefaultBlockTime
	}
	if sc.MaxFetch == 0 {
		sc.MaxFetch = DefaultMaxFetch
	}
	if sc.ClaimInterval == 0 {
		sc.ClaimInterval = DefaultClaimInterval
	}
	if sc.MaxIdleTime == 0 {
		sc.MaxIdleTime = DefaultMaxIdleTime
	}
	// Consume from scratch by default
	if sc.OldestId == "" {
		sc.OldestId = "0"
	}
}

func (sc *SubscriberConfig) Validate() error {
	if sc.Client == nil {
		return errors.New("redis client is empty")
	}
	return nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	s.subscribersWg.Add(1)

	logFields := watermill.LogFields{
		"provider":       "redis",
		"topic":          topic,
		"consumer_group": s.config.ConsumerGroup,
		"consumer_uuid":  s.config.Consumer,
	}
	s.logger.Info("Subscribing to redis stream topic", logFields)

	// we don't want to have buffered channel to not consume messsage from redis stream when consumer is not consuming
	output := make(chan *message.Message)

	consumeClosed, err := s.consumeMessages(ctx, topic, output, logFields)
	if err != nil {
		s.subscribersWg.Done()
		return nil, err
	}

	go func() {
		<-consumeClosed
		close(output)
		s.subscribersWg.Done()
	}()

	return output, nil
}

func (s *Subscriber) consumeMessages(ctx context.Context, topic string, output chan *message.Message, logFields watermill.LogFields) (consumeMessageClosed chan struct{}, err error) {
	s.logger.Info("Starting consuming", logFields)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-s.closing:
			s.logger.Debug("Closing subscriber, cancelling consumeMessages", logFields)
			cancel()
		case <-ctx.Done():
			// avoid goroutine leak
		}
	}()
	if s.config.ConsumerGroup != "" {
		// create consumer group
		if _, err := s.client.XGroupCreateMkStream(ctx, topic, s.config.ConsumerGroup, s.config.OldestId).Result(); err != nil && err.Error() != redisBusyGroup {
			return nil, err
		}
	}

	consumeMessageClosed, err = s.consumeStreams(ctx, topic, output, logFields)
	if err != nil {
		s.logger.Debug(
			"Starting consume failed, cancelling context",
			logFields.Add(watermill.LogFields{"err": err}),
		)
		cancel()
		return nil, err
	}

	return consumeMessageClosed, nil
}

func (s *Subscriber) consumeStreams(ctx context.Context, stream string, output chan *message.Message, logFields watermill.LogFields) (chan struct{}, error) {
	messageHandler := s.createMessageHandler(output)
	consumeMessageClosed := make(chan struct{})

	go func() {
		defer close(consumeMessageClosed)

		readChannel := make(chan *redis.XStream, 1)
		go s.read(ctx, stream, readChannel, logFields)

		for {
			select {
			case xs := <-readChannel:
				if xs == nil {
					s.logger.Debug("readStreamChannel is closed, stopping readStream", logFields)
					return
				}
				if err := messageHandler.processMessage(ctx, xs.Stream, &xs.Messages[0], logFields); err != nil {
					s.logger.Error("processMessage fail", err, logFields)
					return
				}
			case <-s.closing:
				s.logger.Debug("Subscriber is closing, stopping readStream", logFields)
				return
			case <-ctx.Done():
				s.logger.Debug("Ctx was cancelled, stopping readStream", logFields)
				return
			}
		}
	}()

	return consumeMessageClosed, nil
}

func (s *Subscriber) read(ctx context.Context, stream string, readChannel chan<- *redis.XStream, logFields watermill.LogFields) {
	wg := &sync.WaitGroup{}
	claimCtx, claimCancel := context.WithCancel(ctx)
	defer func() {
		claimCancel()
		wg.Wait()
		close(readChannel)
	}()
	var (
		streamsGroup = []string{stream, groupStartid}

		fanOutStartid               = "$"
		countFanOut   int64         = 0
		blockTime     time.Duration = 0

		xss []redis.XStream
		xs  *redis.XStream
		err error
	)

	if s.config.ConsumerGroup != "" {
		// 1. get pending message from idle consumer
		wg.Add(1)
		s.claim(claimCtx, stream, readChannel, false, wg, logFields)

		// 2. background
		wg.Add(1)
		go s.claim(claimCtx, stream, readChannel, true, wg, logFields)
	}

	for {
		select {
		case <-s.closing:
			return
		case <-ctx.Done():
			return
		default:
			if s.config.ConsumerGroup != "" {
				xss, err = s.client.XReadGroup(
					ctx,
					&redis.XReadGroupArgs{
						Group:    s.config.ConsumerGroup,
						Consumer: s.config.Consumer,
						Streams:  streamsGroup,
						Count:    s.config.MaxFetch,
						Block:    blockTime,
					}).Result()
			} else {
				xss, err = s.client.XRead(
					ctx,
					&redis.XReadArgs{
						Streams: []string{stream, fanOutStartid},
						Count:   countFanOut,
						Block:   blockTime,
					}).Result()
			}
			if err == redis.Nil {
				continue
			} else if err != nil {
				s.logger.Error("read fail", err, logFields)
			}
			if len(xss) < 1 || len(xss[0].Messages) < 1 {
				continue
			}
			// update last delivered message
			xs = &xss[0]
			if s.config.ConsumerGroup == "" {
				fanOutStartid = xs.Messages[len(xs.Messages)-1].ID
				countFanOut = s.config.MaxFetch
			}

			blockTime = s.config.BlockTime

			select {
			case <-s.closing:
				return
			case <-ctx.Done():
				return
			case readChannel <- xs:
			}
		}
	}
}

func (s *Subscriber) claim(ctx context.Context, stream string, readChannel chan<- *redis.XStream, keep bool, wg *sync.WaitGroup, logFields watermill.LogFields) {
	defer wg.Done()
	var (
		start             = "0"
		end               = "+"
		count       int64 = 100
		maxIdleTime time.Duration
		xps         []redis.XPendingExt
		err         error
		xp          redis.XPendingExt
		xm          []redis.XMessage
		tick        = time.NewTicker(s.config.ClaimInterval)
		initCh      = make(chan byte, 1)
	)
	defer func() {
		tick.Stop()
		close(initCh)
	}()
	if !keep { // if not keep, run immediately
		initCh <- 1
	}

OUTER_LOOP:
	for {
		select {
		case <-s.closing:
			return
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-initCh:
		}
		xps, err = s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: stream,
			Group:  s.config.ConsumerGroup,
			Start:  start,
			End:    end,
			Count:  count,
		}).Result()
		if err != nil {
			s.logger.Error(
				"xpendingext fail",
				err,
				logFields,
			)
			continue
		}
		for _, xp = range xps {
			if maxIdleTime < xp.Idle {
				maxIdleTime = xp.Idle
			}

			if xp.Idle >= s.config.MaxIdleTime {
				// assign the ownership of a pending message to the current consumer
				xm, err = s.client.XClaim(ctx, &redis.XClaimArgs{
					Stream:   stream,
					Group:    s.config.ConsumerGroup,
					Consumer: s.config.Consumer,
					MinIdle:  s.config.MaxIdleTime,
					Messages: []string{xp.ID},
				}).Result()
				if err != nil {
					s.logger.Error(
						"xclaim fail",
						err,
						logFields.Add(watermill.LogFields{"xp": xp}),
					)
					continue OUTER_LOOP
				}
				// delete idle consumer
				if err = s.client.XGroupDelConsumer(ctx, stream, s.config.ConsumerGroup, xp.Consumer).Err(); err != nil {
					s.logger.Error(
						"xgroupdelconsumer fail",
						err,
						logFields.Add(watermill.LogFields{"xp": xp}),
					)
					continue OUTER_LOOP
				}
				if len(xm) > 0 {
					select {
					case <-s.closing:
						return
					case <-ctx.Done():
						return
					case readChannel <- &redis.XStream{Stream: stream, Messages: xm}:
					}
				}
			}
		}
		if len(xps) == 0 || int64(len(xps)) < count { // done
			if !keep {
				return
			}
			continue
		}
	}
}

func (s *Subscriber) createMessageHandler(output chan *message.Message) messageHandler {
	return messageHandler{
		outputChannel:   output,
		rc:              s.client,
		consumerGroup:   s.config.ConsumerGroup,
		unmarshaller:    s.config.Unmarshaller,
		nackResendSleep: s.config.NackResendSleep,
		logger:          s.logger,
		closing:         s.closing,
	}
}

func (s *Subscriber) Close() error {
	s.closeMutex.Lock()
	defer s.closeMutex.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closing)
	s.subscribersWg.Wait()

	if err := s.client.Close(); err != nil {
		return err
	}

	s.logger.Debug("Redis stream subscriber closed", nil)

	return nil
}

type messageHandler struct {
	outputChannel chan<- *message.Message
	rc            redis.UniversalClient
	consumerGroup string
	unmarshaller  Unmarshaller

	nackResendSleep time.Duration

	logger  watermill.LoggerAdapter
	closing chan struct{}
}

func (h *messageHandler) processMessage(ctx context.Context, stream string, xm *redis.XMessage, messageLogFields watermill.LogFields) error {
	receivedMsgLogFields := messageLogFields.Add(watermill.LogFields{
		"xid": xm.ID,
	})

	h.logger.Trace("Received message from redis stream", receivedMsgLogFields)

	msg, err := h.unmarshaller.Unmarshal(xm.Values)
	if err != nil {
		return errors.Wrapf(err, "message unmarshal failed")
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	receivedMsgLogFields = receivedMsgLogFields.Add(watermill.LogFields{
		"message_uuid": msg.UUID,
		"stream":       stream,
		"xid":          xm.ID,
	})

ResendLoop:
	for {
		select {
		case h.outputChannel <- msg:
			h.logger.Trace("Message sent to consumer", receivedMsgLogFields)
		case <-h.closing:
			h.logger.Trace("Closing, message discarded", receivedMsgLogFields)
			return nil
		case <-ctx.Done():
			h.logger.Trace("Closing, ctx cancelled before sent to consumer", receivedMsgLogFields)
			return nil
		}

		select {
		case <-msg.Acked():
			// deadly retry ack
			p := h.rc.Pipeline()
			if h.consumerGroup != "" {
				p.XAck(ctx, stream, h.consumerGroup, xm.ID)
			}
			err := retry.Retry(func(attempt uint) error {
				_, err := p.Exec(ctx)
				return err
			}, func(attempt uint) bool {
				if attempt != 0 {
					time.Sleep(time.Millisecond * 100)
				}
				return true
			}, func(attempt uint) bool {
				select {
				case <-h.closing:
				case <-ctx.Done():
				default:
					return true
				}
				return false
			})
			if err != nil {
				h.logger.Error("Message Acked fail", err, receivedMsgLogFields)
			} else {
				h.logger.Trace("Message Acked", receivedMsgLogFields)
			}
			break ResendLoop
		case <-msg.Nacked():
			h.logger.Trace("Message Nacked", receivedMsgLogFields)

			// reset acks, etc.
			msg = msg.Copy()
			if h.nackResendSleep != NoSleep {
				time.Sleep(h.nackResendSleep)
			}

			continue ResendLoop
		case <-h.closing:
			h.logger.Trace("Closing, message discarded before ack", receivedMsgLogFields)
			return nil
		case <-ctx.Done():
			h.logger.Trace("Closing, ctx cancelled before ack", receivedMsgLogFields)
			return nil
		}
	}

	return nil
}
