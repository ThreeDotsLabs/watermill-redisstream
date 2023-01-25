package redisstream

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func BenchmarkSubscriber(b *testing.B) {
	pubClient, err := redisClient()
	if err != nil {
		b.Fatal(err)
	}
	subClient, err := redisClient()
	if err != nil {
		b.Fatal(err)
	}

	tests.BenchSubscriber(b, func(n int) (message.Publisher, message.Subscriber) {
		logger := watermill.NopLogger{}

		publisher, err := NewPublisher(PublisherConfig{Client: pubClient}, logger)
		if err != nil {
			panic(err)
		}

		subscriber, err := NewSubscriber(
			SubscriberConfig{
				Client:        subClient,
				Unmarshaller:  &DefaultMarshallerUnmarshaller{},
				Consumer:      watermill.NewShortUUID(),
				ConsumerGroup: watermill.NewShortUUID(),
			},
			logger,
		)
		if err != nil {
			panic(err)
		}

		return publisher, subscriber
	})
}
