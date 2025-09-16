package persistent

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/renstrom/shortuuid"
	"github.com/stretchr/testify/require"
)

var (
	client redis.UniversalClient
)

func redisClient(ctx context.Context) (redis.UniversalClient, error) {
	if client == nil {
		client = redis.NewClient(&redis.Options{
			Addr:         "127.0.0.1:6379",
			DB:           0,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			MinIdleConns: 10,
		})
		err := client.Ping(ctx).Err()
		if err != nil {
			return nil, errors.Wrap(err, "redis simple connect fail")
		}
	}
	return client, nil
}

func newPubSub(t *testing.T, marshaler MarshalerUnmarshaler, subConfig *SubscriberConfig) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	ctx := context.Background()
	rc, err := redisClient(ctx)
	require.NoError(t, err)

	publisher, err := NewPublisher(ctx, PublisherConfig{}, rc, marshaler, logger)
	require.NoError(t, err)

	subscriber, err := NewSubscriber(ctx, *subConfig, rc, marshaler, logger)
	require.NoError(t, err)

	return publisher, subscriber
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGroup(t, shortuuid.New())
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, &DefaultMarshaller{}, &SubscriberConfig{
		Consumer:      shortuuid.New(),
		ConsumerGroup: consumerGroup,
	})
}

func TestPublishSubscribe(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:                      true,
		ExactlyOnceDelivery:                 false,
		GuaranteedOrder:                     false,
		GuaranteedOrderWithSingleSubscriber: true,
		Persistent:                          true,
		//RestartServiceCommand: []string{
		//	`docker`,
		//	`restart`,
		//	`redis-simple`,
		//},
		RequireSingleInstance:            false,
		NewSubscriberReceivesOldMessages: true,
	}

	tests.TestPubSub(t, features, createPubSub, createPubSubWithConsumerGroup)
}

func TestSubscriber(t *testing.T) {

	topic := "test-topic-subscriber"
	ctx := context.Background()
	rc, err := redisClient(ctx)
	require.NoError(t, err)

	subscriber, err := NewSubscriber(
		ctx,
		SubscriberConfig{
			Consumer:      shortuuid.New(),
			ConsumerGroup: "test-consumer-group",
		},
		rc,
		&DefaultMarshaller{},
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)
	messages, err := subscriber.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	publisher, err := NewPublisher(ctx, PublisherConfig{}, rc, &DefaultMarshaller{}, watermill.NewStdLogger(false, false))
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		require.NoError(t, publisher.Publish(topic, message.NewMessage(shortuuid.New(), []byte("test"+strconv.Itoa(i)))))
	}
	require.NoError(t, publisher.Close())

	for i := 0; i < 50; i++ {
		msg := <-messages
		if msg == nil {
			t.Fatal("msg nil")
		}
		t.Logf("%v %v %v", msg.UUID, msg.Metadata, string(msg.Payload))
		msg.Ack()
	}

	require.NoError(t, subscriber.Close())
}

func TestFanOut(t *testing.T) {
	topic := "test-topic-fanout"
	ctx := context.Background()
	rc, err := redisClient(ctx)
	require.NoError(t, err)

	subscriber1, err := NewSubscriber(
		ctx,
		SubscriberConfig{
			Consumer:      shortuuid.New(),
			ConsumerGroup: "",
		},
		rc,
		&DefaultMarshaller{},
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)

	subscriber2, err := NewSubscriber(
		ctx,
		SubscriberConfig{
			Consumer:      shortuuid.New(),
			ConsumerGroup: "",
		},
		rc,
		&DefaultMarshaller{},
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)

	publisher, err := NewPublisher(ctx, PublisherConfig{}, rc, &DefaultMarshaller{}, watermill.NewStdLogger(false, false))
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, publisher.Publish(topic, message.NewMessage(shortuuid.New(), []byte("test"+strconv.Itoa(i)))))
	}

	messages1, err := subscriber1.Subscribe(context.Background(), topic)
	require.NoError(t, err)
	messages2, err := subscriber2.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	// wait for initial XREAD before publishing messages to avoid message loss
	time.Sleep(2 * DefaultBlockTime)
	for i := 10; i < 50; i++ {
		require.NoError(t, publisher.Publish(topic, message.NewMessage(shortuuid.New(), []byte("test"+strconv.Itoa(i)))))
	}
	require.NoError(t, publisher.Close())

	for i := 10; i < 50; i++ {
		msg := <-messages1
		if msg == nil {
			t.Fatal("msg nil")
		}
		t.Logf("subscriber 1: %v %v %v", msg.UUID, msg.Metadata, string(msg.Payload))
		require.Equal(t, string(msg.Payload), ("test" + strconv.Itoa(i)))
		msg.Ack()
	}
	for i := 10; i < 50; i++ {
		msg := <-messages2
		if msg == nil {
			t.Fatal("msg nil")
		}
		t.Logf("subscriber 2: %v %v %v", msg.UUID, msg.Metadata, string(msg.Payload))
		msg.Ack()
	}

	require.NoError(t, subscriber1.Close())
	require.NoError(t, subscriber2.Close())
}
