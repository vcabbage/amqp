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

	r          *Receiver
	deliveryID uint32
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

const (
	TypeStateReceived = 0x23
	TypeStateAccepted = 0x24
	TypeStateRejected = 0x25
	TypeStateReleased = 0x26
	TypeStateModified = 0x27
)

/*
<type name="received" class="composite" source="list" provides="delivery-state">
    <descriptor name="amqp:received:list" code="0x00000000:0x00000023"/>
    <field name="section-number" type="uint" mandatory="true"/>
    <field name="section-offset" type="ulong" mandatory="true"/>
</type>
*/
type StateReceived struct {
	// When sent by the sender this indicates the first section of the message
	// (with section-number 0 being the first section) for which data can be resent.
	// Data from sections prior to the given section cannot be retransmitted for
	// this delivery.
	//
	// When sent by the receiver this indicates the first section of the message
	// for which all data might not yet have been received.
	SectionNumber uint32

	// When sent by the sender this indicates the first byte of the encoded section
	// data of the section given by section-number for which data can be resent
	// (with section-offset 0 being the first byte). Bytes from the same section
	// prior to the given offset section cannot be retransmitted for this delivery.
	//
	// When sent by the receiver this indicates the first byte of the given section
	// which has not yet been received. Note that if a receiver has received all of
	// section number X (which contains N bytes of data), but none of section number
	// X + 1, then it can indicate this by sending either Received(section-number=X,
	// section-offset=N) or Received(section-number=X+1, section-offset=0). The state
	// Received(section-number=0, section-offset=0) indicates that no message data
	// at all has been transferred.
	SectionOffset uint64
}

func (sr *StateReceived) MarshalBinary() ([]byte, error) {
	return marshalComposite(TypeStateReceived, []field{
		{value: sr.SectionNumber, omit: false},
		{value: sr.SectionOffset, omit: false},
	}...)
}

func (sr *StateReceived) UnmarshaleBinary(r byteReader) error {
	return unmarshalComposite(r, TypeStateReceived,
		&sr.SectionNumber,
		&sr.SectionOffset,
	)
}

/*
<type name="accepted" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:accepted:list" code="0x00000000:0x00000024"/>
</type>
*/
type StateAccepted struct{}

func (sa *StateAccepted) MarshalBinary() ([]byte, error) {
	return marshalComposite(TypeStateAccepted)
}

func (sa *StateReceived) UnmarshalBinary(r byteReader) error {
	return unmarshalComposite(r, TypeStateAccepted)
}

/*
<type name="rejected" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:rejected:list" code="0x00000000:0x00000025"/>
    <field name="error" type="error"/>
</type>
*/
type StateRejected struct {
	Error interface{}
}

func (sr *StateRejected) MarshalBinary() ([]byte, error) {
	return marshalComposite(TypeStateRejected,
		field{value: sr.Error, omit: sr.Error == nil},
	)
}

func (sr *StateRejected) UnmarshalBinary(r byteReader) error {
	return unmarshalComposite(r, TypeStateRejected,
		&sr.Error,
	)
}

/*
<type name="released" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:released:list" code="0x00000000:0x00000026"/>
</type>
*/
type StateReleased struct{}

func (sr *StateReleased) MarshalBinary() ([]byte, error) {
	return marshalComposite(TypeStateReleased)
}

func (sr *StateReleased) UnmarshalBinary(r byteReader) error {
	return unmarshalComposite(r, TypeStateReleased)
}

/*
<type name="modified" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:modified:list" code="0x00000000:0x00000027"/>
    <field name="delivery-failed" type="boolean"/>
    <field name="undeliverable-here" type="boolean"/>
    <field name="message-annotations" type="fields"/>
</type>
*/
type StateModified struct {
	// count the transfer as an unsuccessful delivery attempt
	//
	// If the delivery-failed flag is set, any messages modified
	// MUST have their delivery-count incremented.
	DeliveryFailed bool

	// prevent redelivery
	//
	// If the undeliverable-here is set, then any messages released MUST NOT
	// be redelivered to the modifying link endpoint.
	UndeliverableHere bool

	// message attributes
	// Map containing attributes to combine with the existing message-annotations
	// held in the message's header section. Where the existing message-annotations
	// of the message contain an entry with the same key as an entry in this field,
	// the value in this field associated with that key replaces the one in the
	// existing headers; where the existing message-annotations has no such value,
	// the value in this map is added.
	MessageAnnotations Fields
}

func (sm *StateModified) MarshalBinary() ([]byte, error) {
	return marshalComposite(TypeStateModified, []field{
		{value: sm.DeliveryFailed, omit: !sm.DeliveryFailed},
		{value: sm.UndeliverableHere, omit: !sm.UndeliverableHere},
		{value: sm.MessageAnnotations, omit: sm.MessageAnnotations == nil},
	}...)
}

func (sm *StateModified) UnmarshalBinary(r byteReader) error {
	return unmarshalComposite(r, TypeStateModified,
		&sm.DeliveryFailed,
		&sm.UndeliverableHere,
		&sm.MessageAnnotations,
	)
}
