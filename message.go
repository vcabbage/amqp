package amqp

import (
	"bytes"
	"time"
)

// Message is an AMQP message.
type Message struct {
	// The header section carries standard delivery details about the transfer
	// of a message through the AMQP network.
	Header MessageHeader
	// If the header section is omitted the receiver MUST assume the appropriate
	// default values (or the meaning implied by no value being set) for the
	// fields within the header unless other target or node specific defaults
	// have otherwise been set.

	// The delivery-annotations section is used for delivery-specific non-standard
	// properties at the head of the message. Delivery annotations convey information
	// from the sending peer to the receiving peer.
	DeliveryAnnotations map[interface{}]interface{}
	// If the recipient does not understand the annotation it cannot be acted upon
	// and its effects (such as any implied propagation) cannot be acted upon.
	// Annotations might be specific to one implementation, or common to multiple
	// implementations. The capabilities negotiated on link attach and on the source
	// and target SHOULD be used to establish which annotations a peer supports. A
	// registry of defined annotations and their meanings is maintained [AMQPDELANN].
	// The symbolic key "rejected" is reserved for the use of communicating error
	// information regarding rejected messages. Any values associated with the
	// "rejected" key MUST be of type error.
	//
	// If the delivery-annotations section is omitted, it is equivalent to a
	// delivery-annotations section containing an empty map of annotations.

	// The message-annotations section is used for properties of the message which
	// are aimed at the infrastructure.
	Annotations map[interface{}]interface{}
	// The message-annotations section is used for properties of the message which
	// are aimed at the infrastructure and SHOULD be propagated across every
	// delivery step. Message annotations convey information about the message.
	// Intermediaries MUST propagate the annotations unless the annotations are
	// explicitly augmented or modified (e.g., by the use of the modified outcome).
	//
	// The capabilities negotiated on link attach and on the source and target can
	// be used to establish which annotations a peer understands; however, in a
	// network of AMQP intermediaries it might not be possible to know if every
	// intermediary will understand the annotation. Note that for some annotations
	// it might not be necessary for the intermediary to understand their purpose,
	// i.e., they could be used purely as an attribute which can be filtered on.
	//
	// A registry of defined annotations and their meanings is maintained [AMQPMESSANN].
	//
	// If the message-annotations section is omitted, it is equivalent to a
	// message-annotations section containing an empty map of annotations.

	// The properties section is used for a defined set of standard properties of
	// the message.
	Properties MessageProperties
	// The properties section is part of the bare message; therefore,
	// if retransmitted by an intermediary, it MUST remain unaltered.

	// The application-properties section is a part of the bare message used for
	// structured application data. Intermediaries can use the data within this
	// structure for the purposes of filtering or routing.
	ApplicationProperties map[string]interface{}
	// The keys of this map are restricted to be of type string (which excludes
	// the possibility of a null key) and the values are restricted to be of
	// simple types only, that is, excluding map, list, and array types.

	// Message payload.
	Data []byte
	// A data section contains opaque binary data.
	// TODO: this could be data(s), amqp-sequence(s), amqp-value rather than singe data:
	// "The body consists of one of the following three choices: one or more data
	//  sections, one or more amqp-sequence sections, or a single amqp-value section."

	// The footer section is used for details about the message or delivery which
	// can only be calculated or evaluated once the whole bare message has been
	// constructed or seen (for example message hashes, HMACs, signatures and
	// encryption details).
	Footer map[interface{}]interface{}
	// TODO: implement custom type with validation

	link       *link  // link the message was received on
	deliveryID uint32 // used when sending disposition
}

// sendDisposition sends a disposition frame to the peer
func (m *Message) sendDisposition(state interface{}) {
	// TODO: prevent client sending twice?
	m.link.session.txFrame(&performDisposition{
		Role:    true,
		First:   m.deliveryID,
		Settled: true,
		State:   state,
	})
}

// Accept notifies the server that the message has been
// accepted and does not require redelivery.
func (m *Message) Accept() {
	m.sendDisposition(&stateAccepted{})
}

// Reject notifies the server that the message is invalid.
func (m *Message) Reject() {
	m.sendDisposition(&stateRejected{})
}

// Release releases the message back to the server. The message
// may be redelivered to this or another consumer.
func (m *Message) Release() {
	m.sendDisposition(&stateReleased{})
}

// TODO: add support for sending Modified disposition

func (m *Message) unmarshal(r byteReader) error {
	for r.Len() > 0 {
		typ, err := peekMessageType(r.Bytes())
		if err != nil {
			return err
		}

		var (
			section       interface{}
			discardHeader = true
		)
		switch amqpType(typ) {
		case typeCodeMessageHeader:
			discardHeader = false
			section = &m.Header
		case typeCodeDeliveryAnnotations:
			section = &m.DeliveryAnnotations
		case typeCodeMessageAnnotations:
			section = &m.Annotations
		case typeCodeMessageProperties:
			discardHeader = false
			section = &m.Properties
		case typeCodeApplicationProperties:
			section = &m.ApplicationProperties
		case typeCodeApplicationData:
			section = &m.Data
		case typeCodeFooter:
			section = &m.Footer
		default:
			return errorErrorf("unknown message section %x", typ)
		}

		if discardHeader {
			r.Next(3)
		}

		_, err = unmarshal(r, section)
		if err != nil {
			return err
		}
	}
	return nil
}

// peekMessageType reads the message type without
// modifying any data.
func peekMessageType(buf []byte) (uint8, error) {
	if len(buf) < 3 {
		return 0, errorNew("invalid message")
	}

	if buf[0] != 0 {
		return 0, errorErrorf("invalid composite header %0x", buf[0])
	}

	v, err := readInt(bytes.NewBuffer(buf[1:]))
	if err != nil {
		return 0, err
	}
	return uint8(v), err
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

// MessageHeader carries standard delivery details about the transfer
// of a message.
type MessageHeader struct {
	Durable       bool
	Priority      uint8
	TTL           time.Duration // from milliseconds
	FirstAcquirer bool
	DeliveryCount uint32
}

func (h *MessageHeader) marshal() ([]byte, error) {
	return marshalComposite(typeCodeMessageHeader, []marshalField{
		{value: h.Durable, omit: !h.Durable},
		{value: h.Priority, omit: h.Priority == 4},
		{value: milliseconds(h.TTL), omit: h.TTL == 0},
		{value: h.FirstAcquirer, omit: !h.FirstAcquirer},
		{value: h.DeliveryCount, omit: h.DeliveryCount == 0},
	}...)
}

func (h *MessageHeader) unmarshal(r byteReader) error {
	return unmarshalComposite(r, typeCodeMessageHeader, []unmarshalField{
		{field: &h.Durable},
		{field: &h.Priority, handleNull: defaultUint8(&h.Priority, 4)},
		{field: (*milliseconds)(&h.TTL)},
		{field: &h.FirstAcquirer},
		{field: &h.DeliveryCount},
	}...)
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

// MessageProperties is the defined set of properties for AMQP messages.
type MessageProperties struct {
	// TODO: add useful descriptions from spec

	MessageID          interface{} // uint64, UUID, []byte, or string
	UserID             []byte
	To                 string
	Subject            string
	ReplyTo            string
	CorrelationID      interface{} // uint64, UUID, []byte, or string
	ContentType        Symbol
	ContentEncoding    Symbol
	AbsoluteExpiryTime time.Time
	CreationTime       time.Time
	GroupID            string
	GroupSequence      uint32 // RFC-1982 sequence number
	ReplyToGroupID     string
}

func (p *MessageProperties) marshal() ([]byte, error) {
	return marshalComposite(typeCodeMessageProperties, []marshalField{
		{value: p.MessageID, omit: p.MessageID != nil},
		{value: p.UserID, omit: len(p.UserID) == 0},
		{value: p.To, omit: p.To == ""},
		{value: p.Subject, omit: p.Subject == ""},
		{value: p.ReplyTo, omit: p.ReplyTo == ""},
		{value: p.CorrelationID, omit: p.CorrelationID == nil},
		{value: p.ContentType, omit: p.ContentType == ""},
		{value: p.ContentEncoding, omit: p.ContentEncoding == ""},
		{value: p.AbsoluteExpiryTime, omit: p.AbsoluteExpiryTime.IsZero()},
		{value: p.CreationTime, omit: p.CreationTime.IsZero()},
		{value: p.GroupID, omit: p.GroupID == ""},
		{value: p.ReplyToGroupID, omit: p.ReplyToGroupID == ""},
	}...)
}

func (p *MessageProperties) unmarshal(r byteReader) error {
	return unmarshalComposite(r, typeCodeMessageProperties, []unmarshalField{
		{field: &p.MessageID},
		{field: &p.UserID},
		{field: &p.To},
		{field: &p.Subject},
		{field: &p.ReplyTo},
		{field: &p.CorrelationID},
		{field: &p.ContentType},
		{field: &p.ContentEncoding},
		{field: &p.AbsoluteExpiryTime},
		{field: &p.CreationTime},
		{field: &p.GroupID},
		{field: &p.GroupSequence},
		{field: &p.ReplyToGroupID},
	}...)
}

/*
<type name="received" class="composite" source="list" provides="delivery-state">
    <descriptor name="amqp:received:list" code="0x00000000:0x00000023"/>
    <field name="section-number" type="uint" mandatory="true"/>
    <field name="section-offset" type="ulong" mandatory="true"/>
</type>
*/

type stateReceived struct {
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

func (sr *stateReceived) marshal() ([]byte, error) {
	return marshalComposite(typeCodeStateReceived, []marshalField{
		{value: sr.SectionNumber, omit: false},
		{value: sr.SectionOffset, omit: false},
	}...)
}

func (sr *stateReceived) unmarshal(r byteReader) error {
	return unmarshalComposite(r, typeCodeStateReceived, []unmarshalField{
		{field: &sr.SectionNumber, handleNull: required("StateReceived.SectionNumber")},
		{field: &sr.SectionOffset, handleNull: required("StateReceived.SectionOffset")},
	}...)
}

/*
<type name="accepted" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:accepted:list" code="0x00000000:0x00000024"/>
</type>
*/

type stateAccepted struct{}

func (sa *stateAccepted) marshal() ([]byte, error) {
	return marshalComposite(typeCodeStateAccepted)
}

func (sa *stateAccepted) unmarshal(r byteReader) error {
	return unmarshalComposite(r, typeCodeStateAccepted)
}

/*
<type name="rejected" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:rejected:list" code="0x00000000:0x00000025"/>
    <field name="error" type="error"/>
</type>
*/

type stateRejected struct {
	Error *Error
}

func (sr *stateRejected) marshal() ([]byte, error) {
	return marshalComposite(typeCodeStateRejected,
		marshalField{value: sr.Error, omit: sr.Error == nil},
	)
}

func (sr *stateRejected) unmarshal(r byteReader) error {
	return unmarshalComposite(r, typeCodeStateRejected,
		unmarshalField{field: &sr.Error},
	)
}

/*
<type name="released" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:released:list" code="0x00000000:0x00000026"/>
</type>
*/

type stateReleased struct{}

func (sr *stateReleased) marshal() ([]byte, error) {
	return marshalComposite(typeCodeStateReleased)
}

func (sr *stateReleased) unmarshal(r byteReader) error {
	return unmarshalComposite(r, typeCodeStateReleased)
}

/*
<type name="modified" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:modified:list" code="0x00000000:0x00000027"/>
    <field name="delivery-failed" type="boolean"/>
    <field name="undeliverable-here" type="boolean"/>
    <field name="message-annotations" type="fields"/>
</type>
*/

type stateModified struct {
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
	MessageAnnotations map[Symbol]interface{}
}

func (sm *stateModified) marshal() ([]byte, error) {
	return marshalComposite(typeCodeStateModified, []marshalField{
		{value: sm.DeliveryFailed, omit: !sm.DeliveryFailed},
		{value: sm.UndeliverableHere, omit: !sm.UndeliverableHere},
		{value: sm.MessageAnnotations, omit: sm.MessageAnnotations == nil},
	}...)
}

func (sm *stateModified) unmarshal(r byteReader) error {
	return unmarshalComposite(r, typeCodeStateModified, []unmarshalField{
		{field: &sm.DeliveryFailed},
		{field: &sm.UndeliverableHere},
		{field: &sm.MessageAnnotations},
	}...)
}
