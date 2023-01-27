//go:build stress

package redisstream

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func TestPublishSubscribe_stress(t *testing.T) {
	tests.TestPubSubStressTest(
		t,
		tests.Features{
			ConsumerGroups:                      true,
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     false,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
			RequireSingleInstance:               false,
			NewSubscriberReceivesOldMessages:    true,
		},
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}
