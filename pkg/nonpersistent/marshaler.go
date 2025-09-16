package nonpersistent

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

const UUIDHeaderKey = "_watermill_message_uuid"

type Marshaller interface {
	Marshal(topic string, msg *message.Message) ([]byte, error)
}

type Unmarshaller interface {
	Unmarshal(rmsg *redis.Message) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaller
	Unmarshaller
}

type DefaultMarshaller struct{}

type redisEnvelope struct {
	UUID     string           `msgpack:"uuid"`
	Metadata message.Metadata `msgpack:"metadata,omitempty"`
	Payload  []byte           `msgpack:"payload"`
}

// Marshal encodes a Watermill message into MsgPack
func (DefaultMarshaller) Marshal(_ string, msg *message.Message) ([]byte, error) {
	if value := msg.Metadata.Get(UUIDHeaderKey); value != "" {
		return nil, errors.Errorf("metadata %s is reserved by watermill for message UUID", UUIDHeaderKey)
	}

	env := redisEnvelope{
		UUID:     msg.UUID,
		Metadata: msg.Metadata,
		Payload:  msg.Payload,
	}

	b, err := msgpack.Marshal(env)
	if err != nil {
		return nil, errors.Wrap(err, "marshal to MsgPack failed")
	}

	return b, nil
}

// Unmarshal decodes a MsgPack payload into a Watermill message
func (DefaultMarshaller) Unmarshal(rmsg *redis.Message) (*message.Message, error) {
	var env redisEnvelope
	if err := msgpack.Unmarshal([]byte(rmsg.Payload), &env); err != nil {
		return nil, errors.Wrap(err, "unmarshal MsgPack failed")
	}

	msg := message.NewMessage(env.UUID, env.Payload)
	if env.Metadata != nil {
		msg.Metadata = env.Metadata
	}

	return msg, nil
}
