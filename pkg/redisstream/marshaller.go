package redisstream

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

const UUIDHeaderKey = "_watermill_message_uuid"

type Marshaller interface {
	Marshal(topic string, msg *message.Message) (map[string]interface{}, error)
}

type Unmarshaller interface {
	Unmarshal(values map[string]interface{}) (msg *message.Message, err error)
}

type MarshallerUnmarshaller interface {
	Marshaller
	Unmarshaller
}

type DefaultMarshallerUnmarshaller struct{}

func (DefaultMarshallerUnmarshaller) Marshal(_ string, msg *message.Message) (map[string]interface{}, error) {
	if value := msg.Metadata.Get(UUIDHeaderKey); value != "" {
		return nil, errors.Errorf("metadata %s is reserved by watermill for message UUID", UUIDHeaderKey)
	}

	var (
		md  []byte
		err error
	)
	if len(msg.Metadata) > 0 {
		if md, err = msgpack.Marshal(msg.Metadata); err != nil {
			return nil, errors.Wrapf(err, "marshal metadata fail")
		}
	}

	return map[string]interface{}{
		UUIDHeaderKey: msg.UUID,
		"metadata":    md,
		"payload":     []byte(msg.Payload),
	}, nil
}

func (DefaultMarshallerUnmarshaller) Unmarshal(values map[string]interface{}) (msg *message.Message, err error) {
	msg = message.NewMessage(values[UUIDHeaderKey].(string), []byte(values["payload"].(string)))

	md := values["metadata"]
	if md != nil {
		s := md.(string)
		if s != "" {
			metadata := make(message.Metadata)
			if err := msgpack.Unmarshal([]byte(s), &metadata); err != nil {
				return nil, errors.Wrapf(err, "unmarshal metadata fail")
			}
			msg.Metadata = metadata
		}

	}

	return msg, nil
}
