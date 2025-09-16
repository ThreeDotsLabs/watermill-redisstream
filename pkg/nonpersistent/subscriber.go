package nonpersistent

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/renstrom/shortuuid"
)

// Subscriber implements Watermill's Subscriber interface using Redis Pub/Sub.
type Subscriber struct {
	ctx context.Context
	rc  redis.UniversalClient

	buffersize   int
	unmarshaller Unmarshaller
	logger       watermill.LoggerAdapter

	closing       chan struct{}
	subscribersWg sync.WaitGroup
	closed        bool
}

// NewSubscriber creates a new Redis Pub/Sub subscriber.
func NewSubscriber(
	ctx context.Context,
	rc redis.UniversalClient,
	unmarshaller Unmarshaller,
	logger watermill.LoggerAdapter,
	buffersize int,
) (message.Subscriber, error) {
	if logger == nil {
		logger = &watermill.NopLogger{}
	}

	return &Subscriber{
		ctx:          ctx,
		rc:           rc,
		unmarshaller: unmarshaller,
		logger:       logger,
		closing:      make(chan struct{}),
		buffersize:   buffersize,
		closed:       false,
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	s.subscribersWg.Add(1)

	logFields := watermill.LogFields{
		"provider":      "redis-pubsub",
		"topic":         topic,
		"consumer_uuid": shortuuid.New(),
	}
	s.logger.Info("Subscribing to redis pubsub channel", logFields)

	output := make(chan *message.Message, s.buffersize)

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

func (s *Subscriber) consumeMessages(
	ctx context.Context,
	topic string,
	output chan *message.Message,
	logFields watermill.LogFields,
) (chan struct{}, error) {
	s.logger.Info("Starting consume loop", logFields)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-s.closing:
			s.logger.Debug("Closing subscriber, cancelling consumeMessages", logFields)
			cancel()
		case <-ctx.Done():
		}
	}()

	return s.consumePubSub(ctx, topic, output, logFields)
}

func (s *Subscriber) consumePubSub(
	ctx context.Context,
	channel string,
	output chan *message.Message,
	logFields watermill.LogFields,
) (chan struct{}, error) {
	messageHandler := s.createMessageHandler(output)
	consumeClosed := make(chan struct{})

	ready := make(chan struct{})
	// Sicherstellen, dass der Goroutine gestartet ist, bevor wir zurückkehren
	go func() {
		defer close(consumeClosed)

		s.logger.Debug("Start listening to subscription channel.", logFields)
		pubsub := s.rc.Subscribe(ctx, channel)
		ch := pubsub.Channel()
		s.logger.Debug("Subscribed to Redis channel", logFields.Add(watermill.LogFields{"channel": channel}))
		close(ready)
		for {
			s.logger.Trace("Waiting for message or shutdown signal", logFields)

			select {
			case msg, ok := <-ch:
				if !ok {
					s.logger.Debug("Redis pubsub channel closed", logFields)
					return
				}
				s.logger.Debug("Message received from redis pubsub", logFields.Add(watermill.LogFields{"redis_channel": msg.Channel}))
				if err := messageHandler.processMessage(ctx, msg, logFields); err != nil {
					s.logger.Error("processMessage failed", err, logFields)
					return
				}
			case <-s.closing:
				s.logger.Debug("Subscriber is closing", logFields)
				_ = pubsub.Close()
				return
			case <-ctx.Done():
				s.logger.Debug("Context cancelled, stopping pubsub", logFields)
				_ = pubsub.Close()
				return
			}
		}
	}()
	<-ready // warten, bis der Goroutine gestartet ist
	return consumeClosed, nil
}

func (s *Subscriber) createMessageHandler(output chan *message.Message) messageHandler {
	return messageHandler{
		outputChannel: output,
		unmarshaler:   s.unmarshaller,
		logger:        s.logger,
	}
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.closing)
	s.subscribersWg.Wait()
	s.logger.Debug("Redis pubsub subscriber closed", nil)
	return nil
}

type messageHandler struct {
	outputChannel chan<- *message.Message
	unmarshaler   Unmarshaller
	logger        watermill.LoggerAdapter
}

func (h *messageHandler) processMessage(
	ctx context.Context,
	xm *redis.Message,
	logFields watermill.LogFields,
) error {
	receivedFields := logFields.Add(watermill.LogFields{
		"redis_channel": xm.Channel,
	})

	h.logger.Trace("Received message from redis pubsub", receivedFields)

	msg, err := h.unmarshaler.Unmarshal(xm)
	if err != nil {
		return errors.Wrap(err, "message unmarshal failed")
	}

	// Context an Message hängen
	ctx, cancel := context.WithCancel(ctx)
	msg.SetContext(ctx)

	// Nachricht in den gepufferten Output-Channel legen
	h.outputChannel <- msg

	// NICHT sofort ack! -> das macht BulkRead / der Konsument
	h.logger.Trace("Message delivered to output channel", receivedFields.Add(watermill.LogFields{"uuid": msg.UUID}))

	// Cancel darf erst passieren, wenn die Message nicht mehr gebraucht wird
	// -> hier nicht direkt defer cancel() verwenden, sonst invalidierst du Context zu früh
	// Stattdessen: Message-Lifetime dem Konsumenten überlassen
	go func() {
		<-msg.Acked() // wartet bis Konsument ack/nack macht
		cancel()
	}()

	return nil
}
