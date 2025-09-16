package nonpersistent

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/renstrom/shortuuid"
	"github.com/stretchr/testify/require"
)

// Redis client singleton
var client redis.UniversalClient

func redisClient(ctx context.Context) (redis.UniversalClient, error) {
	if client == nil {
		client = redis.NewClient(&redis.Options{
			Addr:         "127.0.0.1:6379",
			DB:           0,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			MinIdleConns: 10,
		})
		if err := client.Ping(ctx).Err(); err != nil {
			return nil, errors.Wrap(err, "redis connect fail")
		}
	}
	return client, nil
}

// helper function to create publisher and subscriber
func newPubSub(t *testing.T, marshaler MarshalerUnmarshaler) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, true)
	ctx := context.Background()

	rc, err := redisClient(ctx)
	require.NoError(t, err)

	publisher, err := NewPublisher(ctx, rc, marshaler, logger)
	require.NoError(t, err)

	subscriber, err := NewSubscriber(ctx, rc, marshaler, logger, 1)
	require.NoError(t, err)

	return publisher, subscriber
}

func TestPublishSubscribe(t *testing.T) {
	publisher, subscriber := newPubSub(t, &DefaultMarshaller{})
	topic := "test-pubsub-topic"
	msgCount := 20

	messages, err := subscriber.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond) // Subscriber sicher aktiv

	for i := 0; i < msgCount; i++ {
		payload := []byte("msg-" + strconv.Itoa(i))
		require.NoError(t, publisher.Publish(topic, message.NewMessage(shortuuid.New(), payload)))
	}

	for i := 0; i < msgCount; i++ {
		msg := <-messages
		require.NotNil(t, msg)

		expected := "msg-" + strconv.Itoa(i)
		require.Equal(t, expected, string(msg.Payload))

		msg.Ack()
	}

	require.NoError(t, subscriber.Close())
}

func TestFanOut(t *testing.T) {
	publisher, _ := newPubSub(t, &DefaultMarshaller{})

	topic := "test-pubsub-fanout"
	subscriberCount := 2
	msgCount := 20

	ctx := context.Background()
	rc, _ := redisClient(ctx)

	subscribers := make([]message.Subscriber, subscriberCount)
	messagesCh := make([]<-chan *message.Message, subscriberCount)

	// Subscriber starten
	for i := 0; i < subscriberCount; i++ {
		s, err := NewSubscriber(ctx, rc, &DefaultMarshaller{}, watermill.NewStdLogger(true, false), 1)
		require.NoError(t, err)
		subscribers[i] = s

		ch, err := s.Subscribe(ctx, topic)
		require.NoError(t, err)
		messagesCh[i] = ch
	}

	// kurz warten, dass Subscriber bereit sind
	time.Sleep(500 * time.Millisecond)

	// Publisher Nachrichten senden (direkt Binär-Payload)
	for i := 0; i < msgCount; i++ {
		rawPayload := "fanout-msg-" + strconv.Itoa(i)
		payload := []byte(rawPayload)
		require.NoError(t, publisher.Publish(topic, message.NewMessage(shortuuid.New(), payload)))
	}
	require.NoError(t, publisher.Close())

	// Nachrichten prüfen
	for i := 0; i < subscriberCount; i++ {
		for j := 0; j < msgCount; j++ {
			msg := <-messagesCh[i]
			require.NotNil(t, msg)

			expected := "fanout-msg-" + strconv.Itoa(j)
			require.Equal(t, expected, string(msg.Payload))
			t.Logf("Subscriber %d received: %s", i+1, string(msg.Payload))

			msg.Ack()
		}
		require.NoError(t, subscribers[i].Close())
	}
}
