package redisstream

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func redisClient() (redis.UniversalClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:6379",
		DB:          0,
		ReadTimeout: -1,
		PoolTimeout: 15 * time.Minute,
	})
	err := client.Ping(context.Background()).Err()
	if err != nil {
		return nil, errors.Wrap(err, "redis simple connect fail")
	}
	return client, nil
}

func newPubSub(t *testing.T, subConfig *SubscriberConfig) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, false)

	pubClient, err := redisClient()
	require.NoError(t, err)

	publisher, err := NewPublisher(
		PublisherConfig{
			Client:     pubClient,
			Marshaller: &DefaultMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)

	subscriber, err := NewSubscriber(*subConfig, logger)
	require.NoError(t, err)

	return publisher, subscriber
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGroup(t, watermill.NewShortUUID())
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	subClient, err := redisClient()
	require.NoError(t, err)

	return newPubSub(t, &SubscriberConfig{
		Client:        subClient,
		Unmarshaller:  &DefaultMarshallerUnmarshaller{},
		Consumer:      watermill.NewShortUUID(),
		ConsumerGroup: consumerGroup,
		BlockTime:     10 * time.Millisecond,
		ClaimInterval: 3 * time.Second,
		MaxIdleTime:   5 * time.Second,
	})
}

func TestPublishSubscribe(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:                      true,
		ExactlyOnceDelivery:                 false,
		GuaranteedOrder:                     false,
		GuaranteedOrderWithSingleSubscriber: true,
		Persistent:                          true,
		RequireSingleInstance:               false,
		NewSubscriberReceivesOldMessages:    true,
	}

	tests.TestPubSub(t, features, createPubSub, createPubSubWithConsumerGroup)
}

func TestSubscriber(t *testing.T) {
	topic := watermill.NewShortUUID()

	pubClient, err := redisClient()
	require.NoError(t, err)
	subClient, err := redisClient()
	require.NoError(t, err)

	subscriber, err := NewSubscriber(
		SubscriberConfig{
			Client:        subClient,
			Unmarshaller:  &DefaultMarshallerUnmarshaller{},
			Consumer:      watermill.NewShortUUID(),
			ConsumerGroup: watermill.NewShortUUID(),
		},
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)
	messages, err := subscriber.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	publisher, err := NewPublisher(
		PublisherConfig{
			Client:     pubClient,
			Marshaller: &DefaultMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)

	var sentMsgs message.Messages
	for i := 0; i < 50; i++ {
		msg := message.NewMessage(watermill.NewShortUUID(), nil)
		require.NoError(t, publisher.Publish(topic, msg))
		sentMsgs = append(sentMsgs, msg)
	}

	var receivedMsgs message.Messages
	for i := 0; i < 50; i++ {
		msg := <-messages
		if msg == nil {
			t.Fatal("msg nil")
		}
		receivedMsgs = append(receivedMsgs, msg)
		msg.Ack()
	}
	tests.AssertAllMessagesReceived(t, sentMsgs, receivedMsgs)

	require.NoError(t, publisher.Close())
	require.NoError(t, subscriber.Close())
}

func TestFanOut(t *testing.T) {
	topic := watermill.NewShortUUID()

	fanOutPubClient, err := redisClient()
	require.NoError(t, err)
	fanOutSubClient1, err := redisClient()
	require.NoError(t, err)
	fanOutSubClient2, err := redisClient()
	require.NoError(t, err)

	subscriber1, err := NewSubscriber(
		SubscriberConfig{
			Client:        fanOutSubClient1,
			Unmarshaller:  &DefaultMarshallerUnmarshaller{},
			Consumer:      watermill.NewShortUUID(),
			ConsumerGroup: "",
		},
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)

	subscriber2, err := NewSubscriber(
		SubscriberConfig{
			Client:        fanOutSubClient2,
			Unmarshaller:  &DefaultMarshallerUnmarshaller{},
			Consumer:      watermill.NewShortUUID(),
			ConsumerGroup: "",
		},
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)

	publisher, err := NewPublisher(
		PublisherConfig{
			Client:     fanOutPubClient,
			Marshaller: &DefaultMarshallerUnmarshaller{},
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, publisher.Publish(topic, message.NewMessage(watermill.NewShortUUID(), []byte("test"+strconv.Itoa(i)))))
	}

	messages1, err := subscriber1.Subscribe(context.Background(), topic)
	require.NoError(t, err)
	messages2, err := subscriber2.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	// wait for initial XREAD before publishing messages to avoid message loss
	time.Sleep(2 * DefaultBlockTime)
	for i := 10; i < 50; i++ {
		require.NoError(t, publisher.Publish(topic, message.NewMessage(watermill.NewShortUUID(), []byte("test"+strconv.Itoa(i)))))
	}

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
		require.Equal(t, string(msg.Payload), ("test" + strconv.Itoa(i)))
		msg.Ack()
	}

	require.NoError(t, publisher.Close())
	require.NoError(t, subscriber1.Close())
	require.NoError(t, subscriber2.Close())
}
