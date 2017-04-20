package amqp

import (
	"bytes"
	"io"
	"time"

	"github.com/pkg/errors"
)

const (
	TypeMessageHeader         = 0x70
	TypeDeliveryAnnotations   = 0x71
	TypeMessageAnnotations    = 0x72
	TypeMessageProperties     = 0x73
	TypeApplicationProperties = 0x74
	TypeApplicationData       = 0x75
	TypeAMQPSequence          = 0x76
	TypeAMQPValue             = 0x77
	TypeFooter                = 0x78
)

type Message struct {
	Header                Header
	DeliveryAnnotations   Map // TODO: implement custom type with validation
	MessageAnnotations    Map // TODO: implement custom type with validation
	Properties            Properties
	ApplicationProperties Map    // TODO: implement custom type with validation
	ApplicationData       []byte // TODO: this could be amqp-sequence or amqp-value rather than data
	Footer                Map    // TODO: implement custom type with validation
}

type Map map[string]interface{}

func (mm *Map) UnmarshalBinary(r byteReader) error {
	mr, err := newMapReader(r)
	if err == errNull {
		return nil
	}
	if err != nil {
		return err
	}

	pairs := mr.count / 2

	m := make(Map, pairs)
	for i := 0; i < pairs; i++ {
		var (
			key   string
			value interface{}
		)
		err = mr.next(&key, &value)
		if err != nil {
			return err
		}
		m[key] = value
	}
	*mm = m
	return nil
}

// func (h *Header) MarshalBinary() ([]byte, error) {
// 	return marshalComposite(TypeMessageHeader, []field{
// 		{value: h.Durable, omit: !h.Durable},
// 		{value: h.Priority, omit: h.Priority == 4},
// 		{value: h.TTL, omit: h.TTL.Duration == 0},
// 		{value: h.FirstAcquirer, omit: !h.FirstAcquirer},
// 		{value: h.DeliveryCount, omit: h.DeliveryCount == 0},
// 	}...)
// }

func peekMessageType(buf []byte) (uint8, error) {
	if len(buf) < 3 {
		return 0, errors.New("invalid message")
	}

	if buf[0] != 0 {
		return 0, errors.Errorf("invalid composite header %0x", buf[0])
	}

	v, err := readInt(bytes.NewReader(buf[1:]))
	if err != nil {
		return 0, err
	}
	return uint8(v), err
}

func consumeBytes(r io.ByteReader, n int) error {
	for i := 0; i < n; i++ {
		_, err := r.ReadByte()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Message) UnmarshalBinary(r byteReader) error {
	byter, ok := r.(interface {
		Bytes() []byte
	})
	if !ok {
		return errors.New("unmarshal message requires Bytes() method")
	}

	for {
		buf := byter.Bytes()
		if len(buf) == 0 {
			break
		}
		typ, err := peekMessageType(buf)
		if err != nil {
			return err
		}

		switch typ {
		case TypeMessageHeader:
			err = Unmarshal(r, &m.Header)
		case TypeDeliveryAnnotations:
			if err = consumeBytes(r, 3); err != nil {
				return err
			}
			err = Unmarshal(r, &m.DeliveryAnnotations)
		case TypeMessageAnnotations:
			if err = consumeBytes(r, 3); err != nil {
				return err
			}
			err = Unmarshal(r, &m.MessageAnnotations)
		case TypeMessageProperties:
			err = Unmarshal(r, &m.Properties)
		case TypeApplicationProperties:
			if err = consumeBytes(r, 3); err != nil {
				return err
			}
			err = Unmarshal(r, &m.ApplicationProperties)
		case TypeApplicationData:
			if err = consumeBytes(r, 3); err != nil {
				return err
			}
			err = Unmarshal(r, &m.ApplicationData)
		case TypeFooter:
			if err = consumeBytes(r, 3); err != nil {
				return err
			}
			err = Unmarshal(r, &m.Footer)
		default:
			return errors.Errorf("unknown message section %x", typ)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

/*
<type name="header" class="composite" source="list" provides="section">
    <descriptor name="amqp:header:list" code="0x00000000:0x00000070"/>
    <field name="durable" type="boolean" default="false"/>
    <field name="priority" type="ubyte" default="4"/>
    <field name="ttl" type="milliseconds"/>
    <field name="first-acquirer" type="boolean" default="false"/>
    <field name="delivery-count" type="uint" default="0"/>
</type>
*/
type Header struct {
	Durable       bool
	Priority      uint8
	TTL           Milliseconds
	FirstAcquirer bool
	DeliveryCount uint32
}

func (h *Header) MarshalBinary() ([]byte, error) {
	return marshalComposite(TypeMessageHeader, []field{
		{value: h.Durable, omit: !h.Durable},
		{value: h.Priority, omit: h.Priority == 4},
		{value: h.TTL, omit: h.TTL.Duration == 0},
		{value: h.FirstAcquirer, omit: !h.FirstAcquirer},
		{value: h.DeliveryCount, omit: h.DeliveryCount == 0},
	}...)
}

func (h *Header) UnmarshalBinary(r byteReader) error {
	return unmarshalComposite(r, TypeMessageHeader,
		&h.Durable,
		&h.Priority,
		&h.TTL,
		&h.FirstAcquirer,
		&h.DeliveryCount,
	)
}

/*
<type name="properties" class="composite" source="list" provides="section">
    <descriptor name="amqp:properties:list" code="0x00000000:0x00000073"/>
    <field name="message-id" type="*" requires="message-id"/>
    <field name="user-id" type="binary"/>
    <field name="to" type="*" requires="address"/>
    <field name="subject" type="string"/>
    <field name="reply-to" type="*" requires="address"/>
    <field name="correlation-id" type="*" requires="message-id"/>
    <field name="content-type" type="symbol"/>
    <field name="content-encoding" type="symbol"/>
    <field name="absolute-expiry-time" type="timestamp"/>
    <field name="creation-time" type="timestamp"/>
    <field name="group-id" type="string"/>
    <field name="group-sequence" type="sequence-no"/>
    <field name="reply-to-group-id" type="string"/>
</type>
*/
type Properties struct {
	MessageID          interface{} // TODO: implement custom type with validation
	UserID             []byte
	To                 string
	Subject            string
	ReplyTo            interface{} // TODO: implement custom type with validation
	CorrelationID      interface{} // TODO: implement custom type with validation
	ContentType        Symbol
	ContentEncoding    Symbol
	AbsoluteExpiryTime time.Time
	CreationTime       time.Time
	GroupID            string
	GroupSequence      uint32 // sequence number
	ReplyToGroupID     string
}

func (p *Properties) MarshalBinary() ([]byte, error) {
	return marshalComposite(TypeMessageProperties, []field{
		{value: p.MessageID, omit: p.MessageID != nil},
		{value: p.UserID, omit: len(p.UserID) == 0},
		{value: p.To, omit: p.To == ""},
		{value: p.Subject, omit: p.Subject == ""},
		{value: p.ReplyTo, omit: p.ReplyTo == nil},
		{value: p.CorrelationID, omit: p.CorrelationID == nil},
		{value: p.ContentType, omit: p.ContentType == ""},
		{value: p.ContentEncoding, omit: p.ContentEncoding == ""},
		{value: p.AbsoluteExpiryTime, omit: p.AbsoluteExpiryTime.IsZero()},
		{value: p.CreationTime, omit: p.CreationTime.IsZero()},
		{value: p.GroupID, omit: p.GroupID == ""},
		{value: p.ReplyToGroupID, omit: p.ReplyToGroupID == ""},
	}...)
}

func (p *Properties) UnmarshalBinary(r byteReader) error {
	return unmarshalComposite(r, TypeMessageProperties,
		&p.MessageID,
		&p.UserID,
		&p.To,
		&p.Subject,
		&p.ReplyTo,
		&p.CorrelationID,
		&p.ContentType,
		&p.ContentEncoding,
		&p.AbsoluteExpiryTime,
		&p.CreationTime,
		&p.GroupID,
		&p.GroupSequence,
		&p.ReplyToGroupID,
	)
}
