package redisstream

import (
	"context"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func redisClient() (redis.UniversalClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:6379",
		DB:          0,
		ReadTimeout: -1,
		PoolTimeout: 10 * time.Minute,
	})
	err := client.Ping(context.Background()).Err()
	if err != nil {
		return nil, errors.Wrap(err, "redis simple connect fail")
	}
	return client, nil
}

func redisClientOrFail(t *testing.T) redis.UniversalClient {
	client, err := redisClient()
	require.NoError(t, err)
	return client
}

func newPubSub(t *testing.T, subConfig *SubscriberConfig) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, false)

	publisher, err := NewPublisher(
		PublisherConfig{
			Client: redisClientOrFail(t),
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
	return newPubSub(t, &SubscriberConfig{
		Client:        redisClientOrFail(t),
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

	subscriber, err := NewSubscriber(
		SubscriberConfig{
			Client:        redisClientOrFail(t),
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
			Client: redisClientOrFail(t),
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

	subscriber1, err := NewSubscriber(
		SubscriberConfig{
			Client:        redisClientOrFail(t),
			Consumer:      watermill.NewShortUUID(),
			ConsumerGroup: "",
		},
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)

	subscriber2, err := NewSubscriber(
		SubscriberConfig{
			Client:        redisClientOrFail(t),
			Consumer:      watermill.NewShortUUID(),
			ConsumerGroup: "",
		},
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)

	publisher, err := NewPublisher(
		PublisherConfig{
			Client: redisClientOrFail(t),
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
		require.Equal(t, string(msg.Payload), "test"+strconv.Itoa(i))
		msg.Ack()
	}
	for i := 10; i < 50; i++ {
		msg := <-messages2
		if msg == nil {
			t.Fatal("msg nil")
		}
		t.Logf("subscriber 2: %v %v %v", msg.UUID, msg.Metadata, string(msg.Payload))
		require.Equal(t, string(msg.Payload), "test"+strconv.Itoa(i))
		msg.Ack()
	}

	require.NoError(t, publisher.Close())
	require.NoError(t, subscriber1.Close())
	require.NoError(t, subscriber2.Close())
}

func TestClaimIdle(t *testing.T) {
	// should be long enough to be robust even for CI boxes
	testInterval := 250 * time.Millisecond

	topic := watermill.NewShortUUID()
	consumerGroup := watermill.NewShortUUID()
	testLogger := watermill.NewStdLogger(true, false)

	router, err := message.NewRouter(message.RouterConfig{
		CloseTimeout: testInterval,
	}, testLogger)
	require.NoError(t, err)

	type messageWithMeta struct {
		msgID        int
		subscriberID int
	}

	receivedCh := make(chan *messageWithMeta)

	// let's start a few subscribers; each will wait between 3 and 5 intervals every time
	// it receives a message
	nSubscribers := 20
	seen := make(map[string]map[string]bool)
	var seenLock sync.Mutex
	for subscriberID := 0; subscriberID < nSubscribers; subscriberID++ {
		// need to assign to a variable local to the loop because of how golang
		// handles loop variables in function literals
		subID := subscriberID

		suscriber, err := NewSubscriber(
			SubscriberConfig{
				Client:        redisClientOrFail(t),
				Consumer:      strconv.Itoa(subID),
				ConsumerGroup: consumerGroup,
				ClaimInterval: testInterval,
				MaxIdleTime:   2 * testInterval,
				// we're only going to claim messages for consumers with odd IDs
				ShouldClaimPendingMessage: func(ext redis.XPendingExt) bool {
					idleConsumerID, err := strconv.Atoi(ext.Consumer)
					require.NoError(t, err)

					if idleConsumerID%2 == 0 {
						return false
					}

					seenLock.Lock()
					defer seenLock.Unlock()

					if seen[ext.ID] == nil {
						seen[ext.ID] = make(map[string]bool)
					}
					if seen[ext.ID][ext.Consumer] {
						return false
					}
					seen[ext.ID][ext.Consumer] = true
					return true
				},
			},
			testLogger,
		)
		require.NoError(t, err)

		router.AddNoPublisherHandler(
			strconv.Itoa(subID),
			topic,
			suscriber,
			func(msg *message.Message) error {
				msgID, err := strconv.Atoi(string(msg.Payload))
				require.NoError(t, err)

				receivedCh <- &messageWithMeta{
					msgID:        msgID,
					subscriberID: subID,
				}
				sleepInterval := (3 + 2*rand.Float64()) * float64(testInterval)
				time.Sleep(time.Duration(sleepInterval))

				return nil
			},
		)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, router.Run(runCtx))
	}()

	// now let's push a few messages
	publisher, err := NewPublisher(
		PublisherConfig{
			Client: redisClientOrFail(t),
		},
		testLogger,
	)
	require.NoError(t, err)

	nMessages := 100
	for msgID := 0; msgID < nMessages; msgID++ {
		msg := message.NewMessage(watermill.NewShortUUID(), []byte(strconv.Itoa(msgID)))
		require.NoError(t, publisher.Publish(topic, msg))
	}

	// now let's wait to receive them
	receivedByID := make(map[int][]*messageWithMeta)
	for len(receivedByID) != nMessages {
		select {
		case msg := <-receivedCh:
			receivedByID[msg.msgID] = append(receivedByID[msg.msgID], msg)
		case <-time.After(8 * testInterval):
			t.Fatalf("timed out waiting for new messages, only received %d unique messages", len(receivedByID))
		}
	}

	// shut down the router and the subscribers
	cancel()
	wg.Wait()

	// now let's look at what we've received:
	// * at least some messages should have been retried
	// * for retried messages, there should be at most one consumer with an even ID
	nMsgsWithRetries := 0
	for _, withSameID := range receivedByID {
		require.Greater(t, len(withSameID), 0)
		if len(withSameID) == 1 {
			// this message was not retried at all
			continue
		}

		nMsgsWithRetries++

		nEvenConsumers := 0
		for _, msg := range withSameID {
			if msg.subscriberID%2 == 0 {
				nEvenConsumers++
			}
		}
		assert.LessOrEqual(t, nEvenConsumers, 1)
	}

	assert.GreaterOrEqual(t, nMsgsWithRetries, 3)
}

func TestSubscriber_ClaimAllMessages(t *testing.T) {
	rdb := redisClientOrFail(t)

	logger := watermill.NewStdLogger(true, true)

	topic := watermill.NewShortUUID()
	consumerGroup := watermill.NewShortUUID()

	// This one should claim all messages
	subGood, err := NewSubscriber(SubscriberConfig{
		Client:                 rdb,
		ConsumerGroup:          consumerGroup,
		Consumer:               "good",
		MaxIdleTime:            500 * time.Millisecond,
		ClaimInterval:          500 * time.Millisecond,
		CheckConsumersInterval: 1 * time.Second,
		ConsumerTimeout:        2 * time.Second,
	}, logger)
	require.NoError(t, err)

	// This one never acks
	subBad, err := NewSubscriber(SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: consumerGroup,
		Consumer:      "bad",
	}, logger)
	require.NoError(t, err)

	pub, err := NewPublisher(PublisherConfig{
		Client: rdb,
	}, logger)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err = pub.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte(strconv.Itoa(i))))
		assert.NoError(t, err)
	}

	badCtx, badCancel := context.WithCancel(context.Background())
	defer badCancel()

	msgs, err := subBad.Subscribe(badCtx, topic)
	require.NoError(t, err)

	// Pull a message, don't ack it!
	<-msgs

	// Cancel the bad subscriber
	badCancel()

	goodCtx, goodCancel := context.WithCancel(context.Background())
	defer goodCancel()

	msgs, err = subGood.Subscribe(goodCtx, topic)
	require.NoError(t, err)

	var processedMessages []string

	// Try to receive all messages
	for i := 0; i < 10; i++ {
		select {
		case msg, ok := <-msgs:
			assert.True(t, ok)
			processedMessages = append(processedMessages, string(msg.Payload))
			msg.Ack()
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting to receive all messages")
		}
	}

	sort.Strings(processedMessages)
	var expected []string
	for i := 0; i < 10; i++ {
		expected = append(expected, strconv.Itoa(i))
	}
	assert.Equal(t, expected, processedMessages)

	assert.Eventually(t, func() bool {
		xic, _ := rdb.XInfoConsumers(context.Background(), topic, consumerGroup).Result()
		return len(xic) == 1 && xic[0].Name == "good"
	}, 5*time.Second, 100*time.Millisecond, "Idle consumer should be deleted")
}
