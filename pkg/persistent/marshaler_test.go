package persistent

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultMarshaler_MarshalUnmarshal(t *testing.T) {
	m := DefaultMarshaller{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := m.Marshal("topic", msg)
	require.NoError(t, err)

	consumerMessage, err := producerToConsumerMessage(marshaled)
	require.NoError(t, err)
	unmarshaledMsg, err := m.Unmarshal(consumerMessage)
	require.NoError(t, err)

	assert.True(t, msg.Equals(unmarshaledMsg))
}

func BenchmarkDefaultMarshaler_Marshal(b *testing.B) {
	m := DefaultMarshaller{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	for i := 0; i < b.N; i++ {
		m.Marshal("foo", msg)
	}
}

func BenchmarkDefaultMarshaler_Unmarshal(b *testing.B) {
	m := DefaultMarshaller{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := m.Marshal("foo", msg)
	if err != nil {
		b.Fatal(err)
	}

	consumedMsg, err := producerToConsumerMessage(marshaled)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		m.Unmarshal(consumedMsg)
	}
}

func producerToConsumerMessage(producerMessage map[string]interface{}) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	for k, v := range producerMessage {
		if b, ok := v.([]byte); ok {
			res[k] = string(b)
		} else {
			res[k] = v
		}
	}
	return res, nil
}
