//go:build stress
// +build stress

package nonpersistent

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func TestPublishSubscribe_stress(t *testing.T) {
	tests.TestPubSubStressTest(
		t,
		tests.Features{
			ConsumerGroups:                      true,
			ExactlyOnceDelivery:                 true,
			GuaranteedOrder:                     false,
			GuaranteedOrderWithSingleSubscriber: false,
			Persistent:                          true,
			NewSubscriberReceivesOldMessages:    true,
		},
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func TestPublishSubscribe_ordered_stress(t *testing.T) {
	tests.TestPubSubStressTest(
		t,
		tests.Features{
			ConsumerGroups:                      true,
			ExactlyOnceDelivery:                 true,
			GuaranteedOrder:                     false,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
			NewSubscriberReceivesOldMessages:    true,
		},
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}
