package persistent

import (
	"context"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/renstrom/shortuuid"
)

func BenchmarkSubscriber(b *testing.B) {

	ctx := context.Background()
	rc, err := redisClient(ctx)
	if err != nil {
		b.Fatal(err)
	}
	tests.BenchSubscriber(b, func(n int) (message.Publisher, message.Subscriber) {
		logger := watermill.NopLogger{}

		publisher, err := NewPublisher(ctx, PublisherConfig{}, rc, &DefaultMarshaller{}, logger)
		if err != nil {
			panic(err)
		}

		subscriber, err := NewSubscriber(
			ctx,
			SubscriberConfig{
				Consumer:      shortuuid.New(),
				ConsumerGroup: shortuuid.New(),
			},
			rc,
			&DefaultMarshaller{},
			logger,
		)
		if err != nil {
			panic(err)
		}

		return publisher, subscriber
	})
}
