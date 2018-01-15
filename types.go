package amqp

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
)

type amqpType uint8

// Type codes
const (
	typeCodeNull amqpType = 0x40

	// Bool
	typeCodeBool      amqpType = 0x56 // boolean with the octet 0x00 being false and octet 0x01 being true
	typeCodeBoolTrue  amqpType = 0x41
	typeCodeBoolFalse amqpType = 0x42

	// Unsigned
	typeCodeUbyte      amqpType = 0x50 // 8-bit unsigned integer (1)
	typeCodeUshort     amqpType = 0x60 // 16-bit unsigned integer in network byte order (2)
	typeCodeUint       amqpType = 0x70 // 32-bit unsigned integer in network byte order (4)
	typeCodeSmallUint  amqpType = 0x52 // unsigned integer value in the range 0 to 255 inclusive (1)
	typeCodeUint0      amqpType = 0x43 // the uint value 0 (0)
	typeCodeUlong      amqpType = 0x80 // 64-bit unsigned integer in network byte order (8)
	typeCodeSmallUlong amqpType = 0x53 // unsigned long value in the range 0 to 255 inclusive (1)
	typeCodeUlong0     amqpType = 0x44 // the ulong value 0 (0)

	// Signed
	typeCodeByte      amqpType = 0x51 // 8-bit two's-complement integer (1)
	typeCodeShort     amqpType = 0x61 // 16-bit two's-complement integer in network byte order (2)
	typeCodeInt       amqpType = 0x71 // 32-bit two's-complement integer in network byte order (4)
	typeCodeSmallint  amqpType = 0x54 // 8-bit two's-complement integer (1)
	typeCodeLong      amqpType = 0x81 // 64-bit two's-complement integer in network byte order (8)
	typeCodeSmalllong amqpType = 0x55 // 8-bit two's-complement integer

	// Decimal
	typeCodeFloat      amqpType = 0x72 // IEEE 754-2008 binary32 (4)
	typeCodeDouble     amqpType = 0x82 // IEEE 754-2008 binary64 (8)
	typeCodeDecimal32  amqpType = 0x74 // IEEE 754-2008 decimal32 using the Binary Integer Decimal encoding (4)
	typeCodeDecimal64  amqpType = 0x84 // IEEE 754-2008 decimal64 using the Binary Integer Decimal encoding (8)
	typeCodeDecimal128 amqpType = 0x94 // IEEE 754-2008 decimal128 using the Binary Integer Decimal encoding (16)

	// Other
	typeCodeChar      amqpType = 0x73 // a UTF-32BE encoded Unicode character (4)
	typeCodeTimestamp amqpType = 0x83 // 64-bit two's-complement integer representing milliseconds since the unix epoch
	typeCodeUUID      amqpType = 0x98 // UUID as defined in section 4.1.2 of RFC-4122

	// Variable Length
	typeCodeVbin8  amqpType = 0xa0 // up to 2^8 - 1 octets of binary data (1 + variable)
	typeCodeVbin32 amqpType = 0xb0 // up to 2^32 - 1 octets of binary data (4 + variable)
	typeCodeStr8   amqpType = 0xa1 // up to 2^8 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (1 + variable)
	typeCodeStr32  amqpType = 0xb1 // up to 2^32 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (4 +variable)
	typeCodeSym8   amqpType = 0xa3 // up to 2^8 - 1 seven bit ASCII characters representing a symbolic value (1 + variable)
	typeCodeSym32  amqpType = 0xb3 // up to 2^32 - 1 seven bit ASCII characters representing a symbolic value (4 + variable)

	// Compound
	typeCodeList0   amqpType = 0x45 // the empty list (i.e. the list with no elements) (0)
	typeCodeList8   amqpType = 0xc0 // up to 2^8 - 1 list elements with total size less than 2^8 octets (1 + compound)
	typeCodeList32  amqpType = 0xd0 // up to 2^32 - 1 list elements with total size less than 2^32 octets (4 + compound)
	typeCodeMap8    amqpType = 0xc1 // up to 2^8 - 1 octets of encoded map data (1 + compound)
	typeCodeMap32   amqpType = 0xd1 // up to 2^32 - 1 octets of encoded map data (4 + compound)
	typeCodeArray8  amqpType = 0xe0 // up to 2^8 - 1 array elements with total size less than 2^8 octets (1 + array)
	typeCodeArray32 amqpType = 0xf0 // up to 2^32 - 1 array elements with total size less than 2^32 octets (4 + array)

	// Composites
	typeCodeOpen        amqpType = 0x10
	typeCodeBegin       amqpType = 0x11
	typeCodeAttach      amqpType = 0x12
	typeCodeFlow        amqpType = 0x13
	typeCodeTransfer    amqpType = 0x14
	typeCodeDisposition amqpType = 0x15
	typeCodeDetach      amqpType = 0x16
	typeCodeEnd         amqpType = 0x17
	typeCodeClose       amqpType = 0x18

	typeCodeSource amqpType = 0x28
	typeCodeTarget amqpType = 0x29
	typeCodeError  amqpType = 0x1d

	typeCodeMessageHeader         amqpType = 0x70
	typeCodeDeliveryAnnotations   amqpType = 0x71
	typeCodeMessageAnnotations    amqpType = 0x72
	typeCodeMessageProperties     amqpType = 0x73
	typeCodeApplicationProperties amqpType = 0x74
	typeCodeApplicationData       amqpType = 0x75
	typeCodeAMQPSequence          amqpType = 0x76
	typeCodeAMQPValue             amqpType = 0x77
	typeCodeFooter                amqpType = 0x78

	typeCodeStateReceived amqpType = 0x23
	typeCodeStateAccepted amqpType = 0x24
	typeCodeStateRejected amqpType = 0x25
	typeCodeStateReleased amqpType = 0x26
	typeCodeStateModified amqpType = 0x27

	typeCodeSASLMechanism amqpType = 0x40
	typeCodeSASLInit      amqpType = 0x41
	typeCodeSASLChallenge amqpType = 0x42
	typeCodeSASLResponse  amqpType = 0x43
	typeCodeSASLOutcome   amqpType = 0x44

	typeCodeDeleteOnClose             amqpType = 0x2b
	typeCodeDeleteOnNoLinks           amqpType = 0x2c
	typeCodeDeleteOnNoMessages        amqpType = 0x2d
	typeCodeDeleteOnNoLinksOrMessages amqpType = 0x2e
)

// Frame structure:
//
//     header (8 bytes)
//       0-3: SIZE (total size, at least 8 bytes for header, uint32)
//       4:   DOFF (data offset,at least 2, count of 4 bytes words, uint8)
//       5:   TYPE (frame type)
//                0x0: AMQP
//                0x1: SASL
//       6-7: type dependent (channel for AMQP)
//     extended header (opt)
//     body (opt)

// frameHeader in a structure appropriate for use with binary.Read()
type frameHeader struct {
	// size: an unsigned 32-bit integer that MUST contain the total frame size of the frame header,
	// extended header, and frame body. The frame is malformed if the size is less than the size of
	// the frame header (8 bytes).
	Size uint32
	// doff: gives the position of the body within the frame. The value of the data offset is an
	// unsigned, 8-bit integer specifying a count of 4-byte words. Due to the mandatory 8-byte
	// frame header, the frame is malformed if the value is less than 2.
	DataOffset uint8
	FrameType  uint8
	Channel    uint16
}

const (
	frameTypeAMQP = 0x0
	frameTypeSASL = 0x1

	frameHeaderSize = 8
)

// protoHeader in a structure appropriate for use with binary.Read()
type protoHeader struct {
	Proto    [4]byte
	ProtoID  protoID
	Major    uint8
	Minor    uint8
	Revision uint8
}

// frame is the decoded representation of a frame
type frame struct {
	typ     uint8     // AMQP/SASL
	channel uint16    // channel this frame is for
	body    frameBody // body of the frame

	// optional channel which will be closed after net transmit
	done chan struct{}
}

// frameBody is the interface all frame bodies must implement
type frameBody interface {
	// if the frame is for a link, link() should return (link#, true),
	// otherwise it should return (0, false)
	link() (handle uint32, ok bool)
}

/*
<type name="open" class="composite" source="list" provides="frame">
    <descriptor name="amqp:open:list" code="0x00000000:0x00000010"/>
    <field name="container-id" type="string" mandatory="true"/>
    <field name="hostname" type="string"/>
    <field name="max-frame-size" type="uint" default="4294967295"/>
    <field name="channel-max" type="ushort" default="65535"/>
    <field name="idle-time-out" type="milliseconds"/>
    <field name="outgoing-locales" type="ietf-language-tag" multiple="true"/>
    <field name="incoming-locales" type="ietf-language-tag" multiple="true"/>
    <field name="offered-capabilities" type="symbol" multiple="true"/>
    <field name="desired-capabilities" type="symbol" multiple="true"/>
    <field name="properties" type="fields"/>
</type>
*/

type performOpen struct {
	ContainerID         string // required
	Hostname            string
	MaxFrameSize        uint32        // default: 4294967295
	ChannelMax          uint16        // default: 65535
	IdleTimeout         time.Duration // from milliseconds
	OutgoingLocales     []symbol
	IncomingLocales     []symbol
	OfferedCapabilities []symbol
	DesiredCapabilities []symbol
	Properties          map[symbol]interface{}
}

func (o *performOpen) link() (uint32, bool) {
	return 0, false
}

func (o *performOpen) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeOpen, []marshalField{
		{value: o.ContainerID, omit: false},
		{value: o.Hostname, omit: o.Hostname == ""},
		{value: o.MaxFrameSize, omit: o.MaxFrameSize == 0},
		{value: o.ChannelMax, omit: o.ChannelMax == 0},
		{value: milliseconds(o.IdleTimeout), omit: o.IdleTimeout == 0},
		{value: o.OutgoingLocales, omit: len(o.OutgoingLocales) == 0},
		{value: o.IncomingLocales, omit: len(o.IncomingLocales) == 0},
		{value: o.OfferedCapabilities, omit: len(o.OfferedCapabilities) == 0},
		{value: o.DesiredCapabilities, omit: len(o.DesiredCapabilities) == 0},
		{value: o.Properties, omit: len(o.Properties) == 0},
	}...)
}

func (o *performOpen) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeOpen, []unmarshalField{
		{field: &o.ContainerID, handleNull: required("Open.ContainerID")},
		{field: &o.Hostname},
		{field: &o.MaxFrameSize, handleNull: defaultUint32(&o.MaxFrameSize, 4294967295)},
		{field: &o.ChannelMax, handleNull: defaultUint16(&o.ChannelMax, 65535)},
		{field: (*milliseconds)(&o.IdleTimeout)},
		{field: &o.OutgoingLocales},
		{field: &o.IncomingLocales},
		{field: &o.OfferedCapabilities},
		{field: &o.DesiredCapabilities},
		{field: &o.Properties},
	}...)
}

/*
<type name="begin" class="composite" source="list" provides="frame">
    <descriptor name="amqp:begin:list" code="0x00000000:0x00000011"/>
    <field name="remote-channel" type="ushort"/>
    <field name="next-outgoing-id" type="transfer-number" mandatory="true"/>
    <field name="incoming-window" type="uint" mandatory="true"/>
    <field name="outgoing-window" type="uint" mandatory="true"/>
    <field name="handle-max" type="handle" default="4294967295"/>
    <field name="offered-capabilities" type="symbol" multiple="true"/>
    <field name="desired-capabilities" type="symbol" multiple="true"/>
    <field name="properties" type="fields"/>
</type>
*/
type performBegin struct {
	// the remote channel for this session
	// If a session is locally initiated, the remote-channel MUST NOT be set.
	// When an endpoint responds to a remotely initiated session, the remote-channel
	// MUST be set to the channel on which the remote session sent the begin.
	RemoteChannel uint16

	// the transfer-id of the first transfer id the sender will send
	NextOutgoingID uint32 // required, sequence number http://www.ietf.org/rfc/rfc1982.txt

	// the initial incoming-window of the sender
	IncomingWindow uint32 // required

	// the initial outgoing-window of the sender
	OutgoingWindow uint32 // required

	// the maximum handle value that can be used on the session
	// The handle-max value is the highest handle value that can be
	// used on the session. A peer MUST NOT attempt to attach a link
	// using a handle value outside the range that its partner can handle.
	// A peer that receives a handle outside the supported range MUST
	// close the connection with the framing-error error-code.
	HandleMax uint32 // default 4294967295

	// the extension capabilities the sender supports
	// http://www.amqp.org/specification/1.0/session-capabilities
	OfferedCapabilities []symbol

	// the extension capabilities the sender can use if the receiver supports them
	// The sender MUST NOT attempt to use any capability other than those it
	// has declared in desired-capabilities field.
	DesiredCapabilities []symbol

	// session properties
	// http://www.amqp.org/specification/1.0/session-properties
	Properties map[symbol]interface{}
}

func (b *performBegin) String() string {
	return fmt.Sprintf("Begin{RemoteChannel: %d, NextOutgoingID: %d, IncomingWindow: %d, "+
		"OutgoingWindow: %d, HandleMax: %d, OfferedCapabilities: %v, DesiredCapabilities: %v, "+
		"Properties: %v}",
		b.RemoteChannel,
		b.NextOutgoingID,
		b.IncomingWindow,
		b.OutgoingWindow,
		b.HandleMax,
		b.OfferedCapabilities,
		b.DesiredCapabilities,
		b.Properties,
	)
}

func (b *performBegin) link() (uint32, bool) {
	return 0, false
}

func (b *performBegin) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeBegin, []marshalField{
		{value: b.RemoteChannel, omit: b.RemoteChannel == 0},
		{value: b.NextOutgoingID, omit: false},
		{value: b.IncomingWindow, omit: false},
		{value: b.OutgoingWindow, omit: false},
		{value: b.HandleMax, omit: b.HandleMax == 0},
		{value: b.OfferedCapabilities, omit: len(b.OfferedCapabilities) == 0},
		{value: b.DesiredCapabilities, omit: len(b.DesiredCapabilities) == 0},
		{value: b.Properties, omit: b.Properties == nil},
	}...)
}

func (b *performBegin) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeBegin, []unmarshalField{
		{field: &b.RemoteChannel},
		{field: &b.NextOutgoingID, handleNull: required("Begin.NextOutgoingID")},
		{field: &b.IncomingWindow, handleNull: required("Begin.IncomingWindow")},
		{field: &b.OutgoingWindow, handleNull: required("Begin.OutgoingWindow")},
		{field: &b.HandleMax, handleNull: defaultUint32(&b.HandleMax, 4294967295)},
		{field: &b.OfferedCapabilities},
		{field: &b.DesiredCapabilities},
		{field: &b.Properties},
	}...)
}

/*
<type name="attach" class="composite" source="list" provides="frame">
    <descriptor name="amqp:attach:list" code="0x00000000:0x00000012"/>
    <field name="name" type="string" mandatory="true"/>
    <field name="handle" type="handle" mandatory="true"/>
    <field name="role" type="role" mandatory="true"/>
    <field name="snd-settle-mode" type="sender-settle-mode" default="mixed"/>
    <field name="rcv-settle-mode" type="receiver-settle-mode" default="first"/>
    <field name="source" type="*" requires="source"/>
    <field name="target" type="*" requires="target"/>
    <field name="unsettled" type="map"/>
    <field name="incomplete-unsettled" type="boolean" default="false"/>
    <field name="initial-delivery-count" type="sequence-no"/>
    <field name="max-message-size" type="ulong"/>
    <field name="offered-capabilities" type="symbol" multiple="true"/>
    <field name="desired-capabilities" type="symbol" multiple="true"/>
    <field name="properties" type="fields"/>
</type>
*/
type performAttach struct {
	// the name of the link
	//
	// This name uniquely identifies the link from the container of the source
	// to the container of the target node, e.g., if the container of the source
	// node is A, and the container of the target node is B, the link MAY be
	// globally identified by the (ordered) tuple (A,B,<name>).
	Name string // required

	// the handle for the link while attached
	//
	// The numeric handle assigned by the the peer as a shorthand to refer to the
	// link in all performatives that reference the link until the it is detached.
	//
	// The handle MUST NOT be used for other open links. An attempt to attach using
	// a handle which is already associated with a link MUST be responded to with
	// an immediate close carrying a handle-in-use session-error.
	//
	// To make it easier to monitor AMQP link attach frames, it is RECOMMENDED that
	// implementations always assign the lowest available handle to this field.
	//
	// The two endpoints MAY potentially use different handles to refer to the same link.
	// Link handles MAY be reused once a link is closed for both send and receive.
	Handle uint32 // required

	// role of the link endpoint
	//
	// The role being played by the peer, i.e., whether the peer is the sender or the
	// receiver of messages on the link.
	Role role

	// settlement policy for the sender
	//
	// The delivery settlement policy for the sender. When set at the receiver this
	// indicates the desired value for the settlement mode at the sender. When set
	// at the sender this indicates the actual settlement mode in use. The sender
	// SHOULD respect the receiver's desired settlement mode if the receiver initiates
	// the attach exchange and the sender supports the desired mode.
	//
	// 0: unsettled - The sender will send all deliveries initially unsettled to the receiver.
	// 1: settled - The sender will send all deliveries settled to the receiver.
	// 2: mixed - The sender MAY send a mixture of settled and unsettled deliveries to the receiver.
	SenderSettleMode *SenderSettleMode

	// the settlement policy of the receiver
	//
	// The delivery settlement policy for the receiver. When set at the sender this
	// indicates the desired value for the settlement mode at the receiver.
	// When set at the receiver this indicates the actual settlement mode in use.
	// The receiver SHOULD respect the sender's desired settlement mode if the sender
	// initiates the attach exchange and the receiver supports the desired mode.
	//
	// 0: first - The receiver will spontaneously settle all incoming transfers.
	// 1: second - The receiver will only settle after sending the disposition to
	//             the sender and receiving a disposition indicating settlement of
	//             the delivery from the sender.
	ReceiverSettleMode *ReceiverSettleMode

	// the source for messages
	//
	// If no source is specified on an outgoing link, then there is no source currently
	// attached to the link. A link with no source will never produce outgoing messages.
	Source *source

	// the target for messages
	//
	// If no target is specified on an incoming link, then there is no target currently
	// attached to the link. A link with no target will never permit incoming messages.
	Target *target

	// unsettled delivery state
	//
	// This is used to indicate any unsettled delivery states when a suspended link is
	// resumed. The map is keyed by delivery-tag with values indicating the delivery state.
	// The local and remote delivery states for a given delivery-tag MUST be compared to
	// resolve any in-doubt deliveries. If necessary, deliveries MAY be resent, or resumed
	// based on the outcome of this comparison. See subsection 2.6.13.
	//
	// If the local unsettled map is too large to be encoded within a frame of the agreed
	// maximum frame size then the session MAY be ended with the frame-size-too-small error.
	// The endpoint SHOULD make use of the ability to send an incomplete unsettled map
	// (see below) to avoid sending an error.
	//
	// The unsettled map MUST NOT contain null valued keys.
	//
	// When reattaching (as opposed to resuming), the unsettled map MUST be null.
	Unsettled unsettled

	// If set to true this field indicates that the unsettled map provided is not complete.
	// When the map is incomplete the recipient of the map cannot take the absence of a
	// delivery tag from the map as evidence of settlement. On receipt of an incomplete
	// unsettled map a sending endpoint MUST NOT send any new deliveries (i.e. deliveries
	// where resume is not set to true) to its partner (and a receiving endpoint which sent
	// an incomplete unsettled map MUST detach with an error on receiving a transfer which
	// does not have the resume flag set to true).
	//
	// Note that if this flag is set to true then the endpoints MUST detach and reattach at
	// least once in order to send new deliveries. This flag can be useful when there are
	// too many entries in the unsettled map to fit within a single frame. An endpoint can
	// attach, resume, settle, and detach until enough unsettled state has been cleared for
	// an attach where this flag is set to false.
	IncompleteUnsettled bool // default: false

	// the sender's initial value for delivery-count
	//
	// This MUST NOT be null if role is sender, and it is ignored if the role is receiver.
	InitialDeliveryCount uint32 // sequence number

	// the maximum message size supported by the link endpoint
	//
	// This field indicates the maximum message size supported by the link endpoint.
	// Any attempt to deliver a message larger than this results in a message-size-exceeded
	// link-error. If this field is zero or unset, there is no maximum size imposed by the
	// link endpoint.
	MaxMessageSize uint64

	// the extension capabilities the sender supports
	// http://www.amqp.org/specification/1.0/link-capabilities
	OfferedCapabilities []symbol

	// the extension capabilities the sender can use if the receiver supports them
	//
	// The sender MUST NOT attempt to use any capability other than those it
	// has declared in desired-capabilities field.
	DesiredCapabilities []symbol

	// link properties
	// http://www.amqp.org/specification/1.0/link-properties
	Properties map[symbol]interface{}
}

func (a performAttach) String() string {
	return fmt.Sprintf("Attach{Name: %s, Handle: %d, Role: %s, SenderSettleMode: %s, ReceiverSettleMode: %s, "+
		"Source: %v, Target: %v, Unsettled: %v, IncompleteUnsettled: %t, InitialDeliveryCount: %d, MaxMessageSize: %d, "+
		"OfferedCapabilities: %v, DesiredCapabilities: %v, Properties: %v}",
		a.Name,
		a.Handle,
		a.Role,
		a.SenderSettleMode,
		a.ReceiverSettleMode,
		a.Source,
		a.Target,
		a.Unsettled,
		a.IncompleteUnsettled,
		a.InitialDeliveryCount,
		a.MaxMessageSize,
		a.OfferedCapabilities,
		a.DesiredCapabilities,
		a.Properties,
	)
}

func (a *performAttach) link() (uint32, bool) {
	return a.Handle, true
}

func (a *performAttach) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeAttach, []marshalField{
		{value: a.Name, omit: false},
		{value: a.Handle, omit: false},
		{value: a.Role, omit: false},
		{value: a.SenderSettleMode, omit: a.SenderSettleMode == nil},
		{value: a.ReceiverSettleMode, omit: a.ReceiverSettleMode == nil},
		{value: a.Source, omit: a.Source == nil},
		{value: a.Target, omit: a.Target == nil},
		{value: a.Unsettled, omit: len(a.Unsettled) == 0},
		{value: a.IncompleteUnsettled, omit: !a.IncompleteUnsettled},
		{value: a.InitialDeliveryCount, omit: a.Role == roleReceiver},
		{value: a.MaxMessageSize, omit: a.MaxMessageSize == 0},
		{value: a.OfferedCapabilities, omit: len(a.OfferedCapabilities) == 0},
		{value: a.DesiredCapabilities, omit: len(a.DesiredCapabilities) == 0},
		{value: a.Properties, omit: len(a.Properties) == 0},
	}...)
}

func (a *performAttach) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeAttach, []unmarshalField{
		{field: &a.Name, handleNull: required("Attach.Name")},
		{field: &a.Handle, handleNull: required("Attach.Handle")},
		{field: &a.Role, handleNull: required("Attach.Role")},
		{field: &a.SenderSettleMode},
		{field: &a.ReceiverSettleMode},
		{field: &a.Source},
		{field: &a.Target},
		{field: &a.Unsettled},
		{field: &a.IncompleteUnsettled},
		{field: &a.InitialDeliveryCount},
		{field: &a.MaxMessageSize},
		{field: &a.OfferedCapabilities},
		{field: &a.DesiredCapabilities},
		{field: &a.Properties},
	}...)
}

type role bool

const (
	roleSender   role = false
	roleReceiver role = true
)

func (rl role) String() string {
	if rl {
		return "Receiver"
	}
	return "Sender"
}

func (rl *role) unmarshal(r reader) error {
	_, err := unmarshal(r, (*bool)(rl))
	return err
}

func (rl role) marshal(wr writer) error {
	return marshal(wr, (bool)(rl))
}

type deliveryState interface{} // TODO: http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html#type-declared

type unsettled map[string]deliveryState

func (u unsettled) marshal(wr writer) error {
	return writeMap(wr, u)
}

func (u *unsettled) unmarshal(r reader) error {
	mr, err := newMapReader(r)
	if err != nil {
		return err
	}

	mm := make(unsettled, mr.pairs())
	for mr.more() {
		var key string
		var value deliveryState
		err = mr.next(&key, &value)
		if err != nil {
			return err
		}
		mm[key] = value
	}
	*u = mm
	return nil
}

/*
<type name="source" class="composite" source="list" provides="source">
    <descriptor name="amqp:source:list" code="0x00000000:0x00000028"/>
    <field name="address" type="*" requires="address"/>
    <field name="durable" type="terminus-durability" default="none"/>
    <field name="expiry-policy" type="terminus-expiry-policy" default="session-end"/>
    <field name="timeout" type="seconds" default="0"/>
    <field name="dynamic" type="boolean" default="false"/>
    <field name="dynamic-node-properties" type="node-properties"/>
    <field name="distribution-mode" type="symbol" requires="distribution-mode"/>
    <field name="filter" type="filter-set"/>
    <field name="default-outcome" type="*" requires="outcome"/>
    <field name="outcomes" type="symbol" multiple="true"/>
    <field name="capabilities" type="symbol" multiple="true"/>
</type>
*/
type source struct {
	// the address of the source
	//
	// The address of the source MUST NOT be set when sent on a attach frame sent by
	// the receiving link endpoint where the dynamic flag is set to true (that is where
	// the receiver is requesting the sender to create an addressable node).
	//
	// The address of the source MUST be set when sent on a attach frame sent by the
	// sending link endpoint where the dynamic flag is set to true (that is where the
	// sender has created an addressable node at the request of the receiver and is now
	// communicating the address of that created node). The generated name of the address
	// SHOULD include the link name and the container-id of the remote container to allow
	// for ease of identification.
	Address string

	// indicates the durability of the terminus
	//
	// Indicates what state of the terminus will be retained durably: the state of durable
	// messages, only existence and configuration of the terminus, or no state at all.
	//
	// 0: none
	// 1: configuration
	// 2: unsettled-state
	Durable uint32

	// the expiry policy of the source
	//
	// link-detach: The expiry timer starts when terminus is detached.
	// session-end: The expiry timer starts when the most recently associated session is
	//              ended.
	// connection-close: The expiry timer starts when most recently associated connection
	//                   is closed.
	// never: The terminus never expires.
	ExpiryPolicy symbol

	// duration that an expiring source will be retained
	//
	// The source starts expiring as indicated by the expiry-policy.
	Timeout uint32 // seconds

	// request dynamic creation of a remote node
	//
	// When set to true by the receiving link endpoint, this field constitutes a request
	// for the sending peer to dynamically create a node at the source. In this case the
	// address field MUST NOT be set.
	//
	// When set to true by the sending link endpoint this field indicates creation of a
	// dynamically created node. In this case the address field will contain the address
	// of the created node. The generated address SHOULD include the link name and other
	// available information on the initiator of the request (such as the remote
	// container-id) in some recognizable form for ease of traceability.
	Dynamic bool

	// properties of the dynamically created node
	//
	// If the dynamic field is not set to true this field MUST be left unset.
	//
	// When set by the receiving link endpoint, this field contains the desired
	// properties of the node the receiver wishes to be created. When set by the
	// sending link endpoint this field contains the actual properties of the
	// dynamically created node. See subsection 3.5.9 for standard node properties.
	// http://www.amqp.org/specification/1.0/node-properties
	//
	// lifetime-policy: The lifetime of a dynamically generated node.
	//					Definitionally, the lifetime will never be less than the lifetime
	//					of the link which caused its creation, however it is possible to
	//					extend the lifetime of dynamically created node using a lifetime
	//					policy. The value of this entry MUST be of a type which provides
	//					the lifetime-policy archetype. The following standard
	//					lifetime-policies are defined below: delete-on-close,
	//					delete-on-no-links, delete-on-no-messages or
	//					delete-on-no-links-or-messages.
	// supported-dist-modes: The distribution modes that the node supports.
	//					The value of this entry MUST be one or more symbols which are valid
	//					distribution-modes. That is, the value MUST be of the same type as
	//					would be valid in a field defined with the following attributes:
	//						type="symbol" multiple="true" requires="distribution-mode"
	DynamicNodeProperties map[symbol]interface{} // TODO: implement custom type with validation

	// the distribution mode of the link
	//
	// This field MUST be set by the sending end of the link if the endpoint supports more
	// than one distribution-mode. This field MAY be set by the receiving end of the link
	// to indicate a preference when a node supports multiple distribution modes.
	DistributionMode symbol

	// a set of predicates to filter the messages admitted onto the link
	//
	// The receiving endpoint sets its desired filter, the sending endpoint sets the filter
	// actually in place (including any filters defaulted at the node). The receiving
	// endpoint MUST check that the filter in place meets its needs and take responsibility
	// for detaching if it does not.
	Filter map[symbol]interface{} // TODO: implement custom type with validation

	// default outcome for unsettled transfers
	//
	// Indicates the outcome to be used for transfers that have not reached a terminal
	// state at the receiver when the transfer is settled, including when the source
	// is destroyed. The value MUST be a valid outcome (e.g., released or rejected).
	DefaultOutcome interface{}

	// descriptors for the outcomes that can be chosen on this link
	//
	// The values in this field are the symbolic descriptors of the outcomes that can
	// be chosen on this link. This field MAY be empty, indicating that the default-outcome
	// will be assumed for all message transfers (if the default-outcome is not set, and no
	// outcomes are provided, then the accepted outcome MUST be supported by the source).
	//
	// When present, the values MUST be a symbolic descriptor of a valid outcome,
	// e.g., "amqp:accepted:list".
	Outcomes []symbol

	// the extension capabilities the sender supports/desires
	//
	// http://www.amqp.org/specification/1.0/source-capabilities
	Capabilities []symbol
}

func (s *source) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeSource, []marshalField{
		{value: s.Address, omit: s.Address == ""},
		{value: s.Durable, omit: s.Durable == 0},
		{value: s.ExpiryPolicy, omit: s.ExpiryPolicy == ""},
		{value: s.Timeout, omit: s.Timeout == 0},
		{value: s.Dynamic, omit: !s.Dynamic},
		{value: s.DynamicNodeProperties, omit: len(s.DynamicNodeProperties) == 0},
		{value: s.DistributionMode, omit: s.DistributionMode == ""},
		{value: s.Filter, omit: len(s.Filter) == 0},
		{value: s.DefaultOutcome, omit: s.DefaultOutcome == nil},
		{value: s.Outcomes, omit: len(s.Outcomes) == 0},
		{value: s.Capabilities, omit: len(s.Capabilities) == 0},
	}...)
}

func (s *source) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeSource, []unmarshalField{
		{field: &s.Address},
		{field: &s.Durable},
		{field: &s.ExpiryPolicy, handleNull: defaultSymbol(&s.ExpiryPolicy, "session-end")},
		{field: &s.Timeout},
		{field: &s.Dynamic},
		{field: &s.DynamicNodeProperties},
		{field: &s.DistributionMode},
		{field: &s.Filter},
		{field: &s.DefaultOutcome},
		{field: &s.Outcomes},
		{field: &s.Capabilities},
	}...)
}

func (s source) String() string {
	return fmt.Sprintf("source{Address: %s, Durable: %d, ExpiryPolicy: %s, Timeout: %d, "+
		"Dynamic: %t, DynamicNodeProperties: %v, DistributionMode: %s, Filter: %v, DefaultOutcome: %v"+
		"Outcomes: %v, Capabilities: %v}",
		s.Address,
		s.Durable,
		s.ExpiryPolicy,
		s.Timeout,
		s.Dynamic,
		s.DynamicNodeProperties,
		s.DistributionMode,
		s.Filter,
		s.DefaultOutcome,
		s.Outcomes,
		s.Capabilities,
	)
}

/*
<type name="target" class="composite" source="list" provides="target">
    <descriptor name="amqp:target:list" code="0x00000000:0x00000029"/>
    <field name="address" type="*" requires="address"/>
    <field name="durable" type="terminus-durability" default="none"/>
    <field name="expiry-policy" type="terminus-expiry-policy" default="session-end"/>
    <field name="timeout" type="seconds" default="0"/>
    <field name="dynamic" type="boolean" default="false"/>
    <field name="dynamic-node-properties" type="node-properties"/>
    <field name="capabilities" type="symbol" multiple="true"/>
</type>
*/
type target struct {
	// the address of the target
	//
	// The address of the target MUST NOT be set when sent on a attach frame sent by
	// the sending link endpoint where the dynamic flag is set to true (that is where
	// the sender is requesting the receiver to create an addressable node).
	//
	// The address of the source MUST be set when sent on a attach frame sent by the
	// receiving link endpoint where the dynamic flag is set to true (that is where
	// the receiver has created an addressable node at the request of the sender and
	// is now communicating the address of that created node). The generated name of
	// the address SHOULD include the link name and the container-id of the remote
	// container to allow for ease of identification.
	Address string

	// indicates the durability of the terminus
	//
	// Indicates what state of the terminus will be retained durably: the state of durable
	// messages, only existence and configuration of the terminus, or no state at all.
	//
	// 0: none
	// 1: configuration
	// 2: unsettled-state
	Durable uint32

	// the expiry policy of the target
	//
	// link-detach: The expiry timer starts when terminus is detached.
	// session-end: The expiry timer starts when the most recently associated session is
	//              ended.
	// connection-close: The expiry timer starts when most recently associated connection
	//                   is closed.
	// never: The terminus never expires.
	ExpiryPolicy symbol

	// duration that an expiring target will be retained
	//
	// The target starts expiring as indicated by the expiry-policy.
	Timeout uint32 // seconds

	// request dynamic creation of a remote node
	//
	// When set to true by the sending link endpoint, this field constitutes a request
	// for the receiving peer to dynamically create a node at the target. In this case
	// the address field MUST NOT be set.
	//
	// When set to true by the receiving link endpoint this field indicates creation of
	// a dynamically created node. In this case the address field will contain the
	// address of the created node. The generated address SHOULD include the link name
	// and other available information on the initiator of the request (such as the
	// remote container-id) in some recognizable form for ease of traceability.
	Dynamic bool

	// properties of the dynamically created node
	//
	// If the dynamic field is not set to true this field MUST be left unset.
	//
	// When set by the sending link endpoint, this field contains the desired
	// properties of the node the sender wishes to be created. When set by the
	// receiving link endpoint this field contains the actual properties of the
	// dynamically created node. See subsection 3.5.9 for standard node properties.
	// http://www.amqp.org/specification/1.0/node-properties
	//
	// lifetime-policy: The lifetime of a dynamically generated node.
	//					Definitionally, the lifetime will never be less than the lifetime
	//					of the link which caused its creation, however it is possible to
	//					extend the lifetime of dynamically created node using a lifetime
	//					policy. The value of this entry MUST be of a type which provides
	//					the lifetime-policy archetype. The following standard
	//					lifetime-policies are defined below: delete-on-close,
	//					delete-on-no-links, delete-on-no-messages or
	//					delete-on-no-links-or-messages.
	// supported-dist-modes: The distribution modes that the node supports.
	//					The value of this entry MUST be one or more symbols which are valid
	//					distribution-modes. That is, the value MUST be of the same type as
	//					would be valid in a field defined with the following attributes:
	//						type="symbol" multiple="true" requires="distribution-mode"
	DynamicNodeProperties map[symbol]interface{} // TODO: implement custom type with validation

	// the extension capabilities the sender supports/desires
	//
	// http://www.amqp.org/specification/1.0/target-capabilities
	Capabilities []symbol
}

func (t *target) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeTarget, []marshalField{
		{value: t.Address, omit: t.Address == ""},
		{value: t.Durable, omit: t.Durable == 0},
		{value: t.ExpiryPolicy, omit: t.ExpiryPolicy == ""},
		{value: t.Timeout, omit: t.Timeout == 0},
		{value: t.Dynamic, omit: !t.Dynamic},
		{value: t.DynamicNodeProperties, omit: len(t.DynamicNodeProperties) == 0},
		{value: t.Capabilities, omit: len(t.Capabilities) == 0},
	}...)
}

func (t *target) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeTarget, []unmarshalField{
		{field: &t.Address},
		{field: &t.Durable},
		{field: &t.ExpiryPolicy, handleNull: defaultSymbol(&t.ExpiryPolicy, "session-end")},
		{field: &t.Timeout},
		{field: &t.Dynamic},
		{field: &t.DynamicNodeProperties},
		{field: &t.Capabilities},
	}...)
}

func (t target) String() string {
	return fmt.Sprintf("source{Address: %s, Durable: %d, ExpiryPolicy: %s, Timeout: %d, "+
		"Dynamic: %t, DynamicNodeProperties: %v, Capabilities: %v}",
		t.Address,
		t.Durable,
		t.ExpiryPolicy,
		t.Timeout,
		t.Dynamic,
		t.DynamicNodeProperties,
		t.Capabilities,
	)
}

/*
<type name="flow" class="composite" source="list" provides="frame">
    <descriptor name="amqp:flow:list" code="0x00000000:0x00000013"/>
    <field name="next-incoming-id" type="transfer-number"/>
    <field name="incoming-window" type="uint" mandatory="true"/>
    <field name="next-outgoing-id" type="transfer-number" mandatory="true"/>
    <field name="outgoing-window" type="uint" mandatory="true"/>
    <field name="handle" type="handle"/>
    <field name="delivery-count" type="sequence-no"/>
    <field name="link-credit" type="uint"/>
    <field name="available" type="uint"/>
    <field name="drain" type="boolean" default="false"/>
    <field name="echo" type="boolean" default="false"/>
    <field name="properties" type="fields"/>
</type>
*/
type performFlow struct {
	// Identifies the expected transfer-id of the next incoming transfer frame.
	// This value MUST be set if the peer has received the begin frame for the
	// session, and MUST NOT be set if it has not. See subsection 2.5.6 for more details.
	NextIncomingID *uint32 // sequence number

	// Defines the maximum number of incoming transfer frames that the endpoint
	// can currently receive. See subsection 2.5.6 for more details.
	IncomingWindow uint32 // required

	// The transfer-id that will be assigned to the next outgoing transfer frame.
	// See subsection 2.5.6 for more details.
	NextOutgoingID uint32 // sequence number

	// Defines the maximum number of outgoing transfer frames that the endpoint
	// could potentially currently send, if it was not constrained by restrictions
	// imposed by its peer's incoming-window. See subsection 2.5.6 for more details.
	OutgoingWindow uint32

	// If set, indicates that the flow frame carries flow state information for the local
	// link endpoint associated with the given handle. If not set, the flow frame is
	// carrying only information pertaining to the session endpoint.
	//
	// If set to a handle that is not currently associated with an attached link,
	// the recipient MUST respond by ending the session with an unattached-handle
	// session error.
	Handle *uint32

	// The delivery-count is initialized by the sender when a link endpoint is created,
	// and is incremented whenever a message is sent. Only the sender MAY independently
	// modify this field. The receiver's value is calculated based on the last known
	// value from the sender and any subsequent messages received on the link. Note that,
	// despite its name, the delivery-count is not a count but a sequence number
	// initialized at an arbitrary point by the sender.
	//
	// When the handle field is not set, this field MUST NOT be set.
	//
	// When the handle identifies that the flow state is being sent from the sender link
	// endpoint to receiver link endpoint this field MUST be set to the current
	// delivery-count of the link endpoint.
	//
	// When the flow state is being sent from the receiver endpoint to the sender endpoint
	// this field MUST be set to the last known value of the corresponding sending endpoint.
	// In the event that the receiving link endpoint has not yet seen the initial attach
	// frame from the sender this field MUST NOT be set.
	DeliveryCount *uint32 // sequence number

	// the current maximum number of messages that can be received
	//
	// The current maximum number of messages that can be handled at the receiver endpoint
	// of the link. Only the receiver endpoint can independently set this value. The sender
	// endpoint sets this to the last known value seen from the receiver.
	// See subsection 2.6.7 for more details.
	//
	// When the handle field is not set, this field MUST NOT be set.
	LinkCredit *uint32

	// the number of available messages
	//
	// The number of messages awaiting credit at the link sender endpoint. Only the sender
	// can independently set this value. The receiver sets this to the last known value seen
	// from the sender. See subsection 2.6.7 for more details.
	//
	// When the handle field is not set, this field MUST NOT be set.
	Available *uint32

	// indicates drain mode
	//
	// When flow state is sent from the sender to the receiver, this field contains the
	// actual drain mode of the sender. When flow state is sent from the receiver to the
	// sender, this field contains the desired drain mode of the receiver.
	// See subsection 2.6.7 for more details.
	//
	// When the handle field is not set, this field MUST NOT be set.
	Drain bool

	// request state from partner
	//
	// If set to true then the receiver SHOULD send its state at the earliest convenient
	// opportunity.
	//
	// If set to true, and the handle field is not set, then the sender only requires
	// session endpoint state to be echoed, however, the receiver MAY fulfil this requirement
	// by sending a flow performative carrying link-specific state (since any such flow also
	// carries session state).
	//
	// If a sender makes multiple requests for the same state before the receiver can reply,
	// the receiver MAY send only one flow in return.
	//
	// Note that if a peer responds to echo requests with flows which themselves have the
	// echo field set to true, an infinite loop could result if its partner adopts the same
	// policy (therefore such a policy SHOULD be avoided).
	Echo bool

	// link state properties
	// http://www.amqp.org/specification/1.0/link-state-properties
	Properties map[symbol]interface{}
}

func (f *performFlow) String() string {
	return fmt.Sprintf("Flow{NextIncomingID: %s, IncomingWindow: %d, NextOutgoingID: %d, OutgoingWindow: %d, "+
		"Handle: %s, DeliveryCount: %s, LinkCredit: %s, Available: %s, Drain: %t, Echo: %t, Properties: %+v}",
		formatUint32Ptr(f.NextIncomingID),
		f.IncomingWindow,
		f.NextOutgoingID,
		f.OutgoingWindow,
		formatUint32Ptr(f.Handle),
		formatUint32Ptr(f.DeliveryCount),
		formatUint32Ptr(f.LinkCredit),
		formatUint32Ptr(f.Available),
		f.Drain,
		f.Echo,
		f.Properties,
	)
}

func formatUint32Ptr(p *uint32) string {
	if p == nil {
		return "<nil>"
	}
	return strconv.FormatUint(uint64(*p), 10)
}

func (f *performFlow) link() (uint32, bool) {
	if f.Handle == nil {
		return 0, false
	}
	return *f.Handle, true
}

func (f *performFlow) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeFlow, []marshalField{
		{value: f.NextIncomingID, omit: f.NextIncomingID == nil},
		{value: f.IncomingWindow, omit: false},
		{value: f.NextOutgoingID, omit: false},
		{value: f.OutgoingWindow, omit: false},
		{value: f.Handle, omit: f.Handle == nil},
		{value: f.DeliveryCount, omit: f.DeliveryCount == nil},
		{value: f.LinkCredit, omit: f.LinkCredit == nil},
		{value: f.Available, omit: f.Available == nil},
		{value: f.Drain, omit: !f.Drain},
		{value: f.Echo, omit: !f.Echo},
		{value: f.Properties, omit: len(f.Properties) == 0},
	}...)
}

func (f *performFlow) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeFlow, []unmarshalField{
		{field: &f.NextIncomingID},
		{field: &f.IncomingWindow, handleNull: required("Flow.IncomingWindow")},
		{field: &f.NextOutgoingID, handleNull: required("Flow.NextOutgoingID")},
		{field: &f.OutgoingWindow, handleNull: required("Flow.OutgoingWindow")},
		{field: &f.Handle},
		{field: &f.DeliveryCount},
		{field: &f.LinkCredit},
		{field: &f.Available},
		{field: &f.Drain},
		{field: &f.Echo},
		{field: &f.Properties},
	}...)
}

/*
<type name="transfer" class="composite" source="list" provides="frame">
    <descriptor name="amqp:transfer:list" code="0x00000000:0x00000014"/>
    <field name="handle" type="handle" mandatory="true"/>
    <field name="delivery-id" type="delivery-number"/>
    <field name="delivery-tag" type="delivery-tag"/>
    <field name="message-format" type="message-format"/>
    <field name="settled" type="boolean"/>
    <field name="more" type="boolean" default="false"/>
    <field name="rcv-settle-mode" type="receiver-settle-mode"/>
    <field name="state" type="*" requires="delivery-state"/>
    <field name="resume" type="boolean" default="false"/>
    <field name="aborted" type="boolean" default="false"/>
    <field name="batchable" type="boolean" default="false"/>
</type>
*/
type performTransfer struct {
	// Specifies the link on which the message is transferred.
	Handle uint32 // required

	// The delivery-id MUST be supplied on the first transfer of a multi-transfer
	// delivery. On continuation transfers the delivery-id MAY be omitted. It is
	// an error if the delivery-id on a continuation transfer differs from the
	// delivery-id on the first transfer of a delivery.
	DeliveryID *uint32 // sequence number

	// Uniquely identifies the delivery attempt for a given message on this link.
	// This field MUST be specified for the first transfer of a multi-transfer
	// message and can only be omitted for continuation transfers. It is an error
	// if the delivery-tag on a continuation transfer differs from the delivery-tag
	// on the first transfer of a delivery.
	DeliveryTag []byte // up to 32 bytes

	// This field MUST be specified for the first transfer of a multi-transfer message
	// and can only be omitted for continuation transfers. It is an error if the
	// message-format on a continuation transfer differs from the message-format on
	// the first transfer of a delivery.
	//
	// The upper three octets of a message format code identify a particular message
	// format. The lowest octet indicates the version of said message format. Any given
	// version of a format is forwards compatible with all higher versions.
	MessageFormat *uint32

	// If not set on the first (or only) transfer for a (multi-transfer) delivery,
	// then the settled flag MUST be interpreted as being false. For subsequent
	// transfers in a multi-transfer delivery if the settled flag is left unset then
	// it MUST be interpreted as true if and only if the value of the settled flag on
	// any of the preceding transfers was true; if no preceding transfer was sent with
	// settled being true then the value when unset MUST be taken as false.
	//
	// If the negotiated value for snd-settle-mode at attachment is settled, then this
	// field MUST be true on at least one transfer frame for a delivery (i.e., the
	// delivery MUST be settled at the sender at the point the delivery has been
	// completely transferred).
	//
	// If the negotiated value for snd-settle-mode at attachment is unsettled, then this
	// field MUST be false (or unset) on every transfer frame for a delivery (unless the
	// delivery is aborted).
	Settled bool

	// indicates that the message has more content
	//
	// Note that if both the more and aborted fields are set to true, the aborted flag
	// takes precedence. That is, a receiver SHOULD ignore the value of the more field
	// if the transfer is marked as aborted. A sender SHOULD NOT set the more flag to
	// true if it also sets the aborted flag to true.
	More bool

	// If first, this indicates that the receiver MUST settle the delivery once it has
	// arrived without waiting for the sender to settle first.
	//
	// If second, this indicates that the receiver MUST NOT settle until sending its
	// disposition to the sender and receiving a settled disposition from the sender.
	//
	// If not set, this value is defaulted to the value negotiated on link attach.
	//
	// If the negotiated link value is first, then it is illegal to set this field
	// to second.
	//
	// If the message is being sent settled by the sender, the value of this field
	// is ignored.
	//
	// The (implicit or explicit) value of this field does not form part of the
	// transfer state, and is not retained if a link is suspended and subsequently resumed.
	//
	// 0: first - The receiver will spontaneously settle all incoming transfers.
	// 1: second - The receiver will only settle after sending the disposition to
	//             the sender and receiving a disposition indicating settlement of
	//             the delivery from the sender.
	ReceiverSettleMode *ReceiverSettleMode

	// the state of the delivery at the sender
	//
	// When set this informs the receiver of the state of the delivery at the sender.
	// This is particularly useful when transfers of unsettled deliveries are resumed
	// after resuming a link. Setting the state on the transfer can be thought of as
	// being equivalent to sending a disposition immediately before the transfer
	// performative, i.e., it is the state of the delivery (not the transfer) that
	// existed at the point the frame was sent.
	//
	// Note that if the transfer performative (or an earlier disposition performative
	// referring to the delivery) indicates that the delivery has attained a terminal
	// state, then no future transfer or disposition sent by the sender can alter that
	// terminal state.
	State deliveryState

	// indicates a resumed delivery
	//
	// If true, the resume flag indicates that the transfer is being used to reassociate
	// an unsettled delivery from a dissociated link endpoint. See subsection 2.6.13
	// for more details.
	//
	// The receiver MUST ignore resumed deliveries that are not in its local unsettled map.
	// The sender MUST NOT send resumed transfers for deliveries not in its local
	// unsettled map.
	//
	// If a resumed delivery spans more than one transfer performative, then the resume
	// flag MUST be set to true on the first transfer of the resumed delivery. For
	// subsequent transfers for the same delivery the resume flag MAY be set to true,
	// or MAY be omitted.
	//
	// In the case where the exchange of unsettled maps makes clear that all message
	// data has been successfully transferred to the receiver, and that only the final
	// state (and potentially settlement) at the sender needs to be conveyed, then a
	// resumed delivery MAY carry no payload and instead act solely as a vehicle for
	// carrying the terminal state of the delivery at the sender.
	Resume bool

	// indicates that the message is aborted
	//
	// Aborted messages SHOULD be discarded by the recipient (any payload within the
	// frame carrying the performative MUST be ignored). An aborted message is
	// implicitly settled.
	Aborted bool

	// batchable hint
	//
	// If true, then the issuer is hinting that there is no need for the peer to urgently
	// communicate updated delivery state. This hint MAY be used to artificially increase
	// the amount of batching an implementation uses when communicating delivery states,
	// and thereby save bandwidth.
	//
	// If the message being delivered is too large to fit within a single frame, then the
	// setting of batchable to true on any of the transfer performatives for the delivery
	// is equivalent to setting batchable to true for all the transfer performatives for
	// the delivery.
	//
	// The batchable value does not form part of the transfer state, and is not retained
	// if a link is suspended and subsequently resumed.
	Batchable bool

	Payload []byte

	// optional channel to indicate to sender that transfer has completed
	done chan struct{}
	// complete when receiver has responded with dispostion (ReceiverSettleMode = second)
	// instead of when this message has been sent on network
	confirmSettlement bool
}

func (t performTransfer) String() string {
	deliveryTag := "<nil>"
	if t.DeliveryID != nil {
		deliveryTag = string(t.DeliveryTag)
	}

	return fmt.Sprintf("Transfer{Handle: %d, DeliveryID: %s, DeliveryTag: %q, MessageFormat: %s, "+
		"Settled: %t, More: %t, ReceiverSettleMode: %s, State: %v, Resume: %t, Aborted: %t, "+
		"Batchable: %t, Payload [size]: %d}",
		t.Handle,
		formatUint32Ptr(t.DeliveryID),
		deliveryTag,
		formatUint32Ptr(t.MessageFormat),
		t.Settled,
		t.More,
		t.ReceiverSettleMode,
		t.State,
		t.Resume,
		t.Aborted,
		t.Batchable,
		len(t.Payload),
	)
}

func (t *performTransfer) link() (uint32, bool) {
	return t.Handle, true
}

func (t *performTransfer) marshal(wr writer) error {
	err := marshalComposite(wr, typeCodeTransfer, []marshalField{
		{value: t.Handle},
		{value: t.DeliveryID, omit: t.DeliveryID == nil},
		{value: t.DeliveryTag, omit: len(t.DeliveryTag) == 0},
		{value: t.MessageFormat, omit: t.MessageFormat == nil},
		{value: t.Settled, omit: !t.Settled},
		{value: t.More, omit: !t.More},
		{value: t.ReceiverSettleMode, omit: t.ReceiverSettleMode == nil},
		{value: t.State, omit: t.State == nil},
		{value: t.Resume, omit: !t.Resume},
		{value: t.Aborted, omit: !t.Aborted},
		{value: t.Batchable, omit: !t.Batchable},
	}...)
	if err != nil {
		return err
	}

	_, err = wr.Write(t.Payload)
	return err
}

func (t *performTransfer) unmarshal(r reader) error {
	err := unmarshalComposite(r, typeCodeTransfer, []unmarshalField{
		{field: &t.Handle, handleNull: required("Transfer.Handle")},
		{field: &t.DeliveryID},
		{field: &t.DeliveryTag},
		{field: &t.MessageFormat},
		{field: &t.Settled},
		{field: &t.More},
		{field: &t.ReceiverSettleMode},
		{field: &t.State},
		{field: &t.Resume},
		{field: &t.Aborted},
		{field: &t.Batchable},
	}...)
	if err != nil {
		return err
	}

	t.Payload = append([]byte(nil), r.Bytes()...)

	return err
}

/*
<type name="disposition" class="composite" source="list" provides="frame">
    <descriptor name="amqp:disposition:list" code="0x00000000:0x00000015"/>
    <field name="role" type="role" mandatory="true"/>
    <field name="first" type="delivery-number" mandatory="true"/>
    <field name="last" type="delivery-number"/>
    <field name="settled" type="boolean" default="false"/>
    <field name="state" type="*" requires="delivery-state"/>
    <field name="batchable" type="boolean" default="false"/>
</type>
*/
type performDisposition struct {
	// directionality of disposition
	//
	// The role identifies whether the disposition frame contains information about
	// sending link endpoints or receiving link endpoints.
	Role role

	// lower bound of deliveries
	//
	// Identifies the lower bound of delivery-ids for the deliveries in this set.
	First uint32 // required, sequence number

	// upper bound of deliveries
	//
	// Identifies the upper bound of delivery-ids for the deliveries in this set.
	// If not set, this is taken to be the same as first.
	Last *uint32 // sequence number

	// indicates deliveries are settled
	//
	// If true, indicates that the referenced deliveries are considered settled by
	// the issuing endpoint.
	Settled bool

	// indicates state of deliveries
	//
	// Communicates the state of all the deliveries referenced by this disposition.
	State deliveryState

	// batchable hint
	//
	// If true, then the issuer is hinting that there is no need for the peer to
	// urgently communicate the impact of the updated delivery states. This hint
	// MAY be used to artificially increase the amount of batching an implementation
	// uses when communicating delivery states, and thereby save bandwidth.
	Batchable bool
}

func (d performDisposition) String() string {
	return fmt.Sprintf("Disposition{Role: %s, First: %d, Last: %s, Settled: %t, State: %s, Batchable: %t}",
		d.Role,
		d.First,
		formatUint32Ptr(d.Last),
		d.Settled,
		d.State,
		d.Batchable,
	)
}

func (*performDisposition) link() (uint32, bool) {
	return 0, false
}

func (d *performDisposition) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeDisposition, []marshalField{
		{value: d.Role, omit: false},
		{value: d.First, omit: false},
		{value: d.Last, omit: d.Last == nil},
		{value: d.Settled, omit: !d.Settled},
		{value: d.State, omit: d.State == nil},
		{value: d.Batchable, omit: !d.Batchable},
	}...)
}

func (d *performDisposition) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeDisposition, []unmarshalField{
		{field: &d.Role, handleNull: required("Disposition.Role")},
		{field: &d.First, handleNull: required("Disposition.First")},
		{field: &d.Last},
		{field: &d.Settled},
		{field: &d.State},
		{field: &d.Batchable},
	}...)
}

/*
<type name="detach" class="composite" source="list" provides="frame">
    <descriptor name="amqp:detach:list" code="0x00000000:0x00000016"/>
    <field name="handle" type="handle" mandatory="true"/>
    <field name="closed" type="boolean" default="false"/>
    <field name="error" type="error"/>
</type>
*/
type performDetach struct {
	// the local handle of the link to be detached
	Handle uint32 //required

	// if true then the sender has closed the link
	Closed bool

	// error causing the detach
	//
	// If set, this field indicates that the link is being detached due to an error
	// condition. The value of the field SHOULD contain details on the cause of the error.
	Error *Error
}

func (d performDetach) String() string {
	return fmt.Sprintf("Detach{Handle: %d, Closed: %t, Error: %v}",
		d.Handle,
		d.Closed,
		d.Error,
	)
}

func (d *performDetach) link() (uint32, bool) {
	return d.Handle, true
}

func (d *performDetach) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeDetach, []marshalField{
		{value: d.Handle, omit: false},
		{value: d.Closed, omit: !d.Closed},
		{value: d.Error, omit: d.Error == nil},
	}...)
}

func (d *performDetach) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeDetach, []unmarshalField{
		{field: &d.Handle, handleNull: required("Detach.Handle")},
		{field: &d.Closed},
		{field: &d.Error},
	}...)
}

// ErrorCondition is one of the error conditions defined in the AMQP spec.
type ErrorCondition string

func (ec ErrorCondition) marshal(wr writer) error {
	return marshal(wr, (symbol)(ec))
}

func (ec *ErrorCondition) unmarshal(r reader) error {
	_, err := unmarshal(r, (*symbol)(ec))
	return err
}

// Error Conditions
const (
	// AMQP Errors
	ErrorInternalError         ErrorCondition = "amqp:internal-error"
	ErrorNotFound              ErrorCondition = "amqp:not-found"
	ErrorUnauthorizedAccess    ErrorCondition = "amqp:unauthorized-access"
	ErrorDecodeError           ErrorCondition = "amqp:decode-error"
	ErrorResourceLimitExceeded ErrorCondition = "amqp:resource-limit-exceeded"
	ErrorNotAllowed            ErrorCondition = "amqp:not-allowed"
	ErrorInvalidField          ErrorCondition = "amqp:invalid-field"
	ErrorNotImplemented        ErrorCondition = "amqp:not-implemented"
	ErrorResourceLocked        ErrorCondition = "amqp:resource-locked"
	ErrorPreconditionFailed    ErrorCondition = "amqp:precondition-failed"
	ErrorResourceDeleted       ErrorCondition = "amqp:resource-deleted"
	ErrorIllegalState          ErrorCondition = "amqp:illegal-state"
	ErrorFrameSizeTooSmall     ErrorCondition = "amqp:frame-size-too-small"

	// Connection Errors
	ErrorConnectionForced   ErrorCondition = "amqp:connection:forced"
	ErrorFramingError       ErrorCondition = "amqp:connection:framing-error"
	ErrorConnectionRedirect ErrorCondition = "amqp:connection:redirect"

	// Session Errors
	ErrorWindowViolation  ErrorCondition = "amqp:session:window-violation"
	ErrorErrantLink       ErrorCondition = "amqp:session:errant-link"
	ErrorHandleInUse      ErrorCondition = "amqp:session:handle-in-use"
	ErrorUnattachedHandle ErrorCondition = "amqp:session:unattached-handle"

	// Link Errors
	ErrorDetachForced          ErrorCondition = "amqp:link:detach-forced"
	ErrorTransferLimitExceeded ErrorCondition = "amqp:link:transfer-limit-exceeded"
	ErrorMessageSizeExceeded   ErrorCondition = "amqp:link:message-size-exceeded"
	ErrorLinkRedirect          ErrorCondition = "amqp:link:redirect"
	ErrorStolen                ErrorCondition = "amqp:link:stolen"
)

/*
<type name="error" class="composite" source="list">
    <descriptor name="amqp:error:list" code="0x00000000:0x0000001d"/>
    <field name="condition" type="symbol" requires="error-condition" mandatory="true"/>
    <field name="description" type="string"/>
    <field name="info" type="fields"/>
</type>
*/

// Error is an AMQP error.
type Error struct {
	// TODO: should this implement the error interface?

	// A symbolic value indicating the error condition.
	Condition ErrorCondition
	// TODO: make enum

	// descriptive text about the error condition
	//
	// This text supplies any supplementary details not indicated by the condition field.
	// This text can be logged as an aid to resolving issues.
	Description string

	// map carrying information about the error condition
	Info map[string]interface{}
	// TODO: make more user friendly
}

func (e *Error) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeError, []marshalField{
		{value: e.Condition, omit: false},
		{value: e.Description, omit: e.Description == ""},
		{value: e.Info, omit: len(e.Info) == 0},
	}...)
}

func (e *Error) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeError, []unmarshalField{
		{field: &e.Condition, handleNull: required("Error.Condition")},
		{field: &e.Description},
		{field: &e.Info},
	}...)
}

func (e *Error) String() string {
	if e == nil {
		return "*Error(nil)"
	}
	return fmt.Sprintf("*Error{Condition: %s, Description: %s, Info: %v}",
		e.Condition,
		e.Description,
		e.Info,
	)
}

/*
<type name="end" class="composite" source="list" provides="frame">
    <descriptor name="amqp:end:list" code="0x00000000:0x00000017"/>
    <field name="error" type="error"/>
</type>
*/
type performEnd struct {
	// error causing the end
	//
	// If set, this field indicates that the session is being ended due to an error
	// condition. The value of the field SHOULD contain details on the cause of the error.
	Error *Error
}

func (*performEnd) link() (uint32, bool) {
	return 0, false
}

func (e *performEnd) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeEnd,
		marshalField{value: e.Error, omit: e.Error == nil},
	)
}

func (e *performEnd) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeEnd,
		unmarshalField{field: &e.Error},
	)
}

/*
<type name="close" class="composite" source="list" provides="frame">
    <descriptor name="amqp:close:list" code="0x00000000:0x00000018"/>
    <field name="error" type="error"/>
</type>
*/
type performClose struct {
	// error causing the close
	//
	// If set, this field indicates that the session is being closed due to an error
	// condition. The value of the field SHOULD contain details on the cause of the error.
	Error *Error
}

func (*performClose) link() (uint32, bool) {
	return 0, false
}

func (c *performClose) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeClose,
		marshalField{value: c.Error, omit: c.Error == nil},
	)
}

func (c *performClose) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeClose,
		unmarshalField{field: &c.Error},
	)
}

func (c *performClose) String() string {
	return fmt.Sprintf("*performClose{Error: %s}", c.Error)
}

// Message is an AMQP message.
type Message struct {
	// The header section carries standard delivery details about the transfer
	// of a message through the AMQP network.
	Header *MessageHeader
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
	Properties *MessageProperties
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

	// Value payload.
	Value interface{}
	// An amqp-value section contains a single AMQP value.

	// The footer section is used for details about the message or delivery which
	// can only be calculated or evaluated once the whole bare message has been
	// constructed or seen (for example message hashes, HMACs, signatures and
	// encryption details).
	Footer map[interface{}]interface{}
	// TODO: implement custom type with validation

	receiver *Receiver  // Receiver the message was received from
	id       deliveryID // used when sending disposition
	settled  bool       // whether transfer was settled by sender
}

// Accept notifies the server that the message has been
// accepted and does not require redelivery.
func (m *Message) Accept() {
	if m.shouldSendDisposition() {
		m.receiver.acceptMessage(m.id)
	}
}

// Reject notifies the server that the message is invalid.
func (m *Message) Reject() {
	if m.shouldSendDisposition() {
		m.receiver.rejectMessage(m.id)
	}
}

// Release releases the message back to the server. The message
// may be redelivered to this or another consumer.
func (m *Message) Release() {
	if m.shouldSendDisposition() {
		m.receiver.releaseMessage(m.id)
	}
}

func (m *Message) shouldSendDisposition() bool {
	return !m.settled || (m.receiver.link.receiverSettleMode != nil && *m.receiver.link.receiverSettleMode == ModeSecond)
}

// TODO: add support for sending Modified disposition

func (m *Message) marshal(wr writer) error {
	if m.Header != nil {
		err := m.Header.marshal(wr)
		if err != nil {
			return err
		}
	}

	if m.DeliveryAnnotations != nil {
		err := writeDescriptor(wr, typeCodeDeliveryAnnotations)
		if err != nil {
			return err
		}
		err = marshal(wr, m.DeliveryAnnotations)
		if err != nil {
			return err
		}
	}

	if m.Annotations != nil {
		err := writeDescriptor(wr, typeCodeMessageAnnotations)
		if err != nil {
			return err
		}
		err = marshal(wr, m.Annotations)
		if err != nil {
			return err
		}
	}

	if m.Properties != nil {
		err := marshal(wr, m.Properties)
		if err != nil {
			return err
		}
	}

	if m.ApplicationProperties != nil {
		err := writeDescriptor(wr, typeCodeApplicationProperties)
		if err != nil {
			return err
		}
		err = marshal(wr, m.ApplicationProperties)
		if err != nil {
			return err
		}
	}

	if m.Data != nil {
		err := writeDescriptor(wr, typeCodeApplicationData)
		if err != nil {
			return err
		}
		err = marshal(wr, m.Data)
		if err != nil {
			return err
		}
	}

	if m.Value != nil {
		err := writeDescriptor(wr, typeCodeAMQPValue)
		if err != nil {
			return err
		}
		err = marshal(wr, m.Value)
		if err != nil {
			return err
		}
	}

	if m.Footer != nil {
		err := writeDescriptor(wr, typeCodeFooter)
		if err != nil {
			return err
		}
		err = marshal(wr, m.Footer)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Message) unmarshal(r reader) error {
	// loop, decoding sections until bytes have been consumed
	for r.Len() > 0 {
		// determine type
		typ, err := peekMessageType(r.Bytes())
		if err != nil {
			return err
		}

		var (
			section interface{}
			// section header is read from r before
			// unmarshaling section is set to true
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

		case typeCodeAMQPValue:
			section = &m.Value

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
		if checkNull(buf) {
			return 0, errNull
		}
		return 0, errorNew("invalid message")
	}

	if buf[0] != 0 {
		if checkNull(buf) {
			return 0, errNull
		}
		return 0, errorErrorf("invalid composite header %0x", buf[0])
	}

	v, err := readInt(bytes.NewBuffer(buf[1:]))
	if err != nil {
		return 0, err
	}
	return uint8(v), err
}

func checkNull(buf []byte) bool {
	return len(buf) > 0 && amqpType(buf[0]) == typeCodeNull
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

func (h *MessageHeader) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeMessageHeader, []marshalField{
		{value: h.Durable, omit: !h.Durable},
		{value: h.Priority, omit: h.Priority == 4},
		{value: milliseconds(h.TTL), omit: h.TTL == 0},
		{value: h.FirstAcquirer, omit: !h.FirstAcquirer},
		{value: h.DeliveryCount, omit: h.DeliveryCount == 0},
	}...)
}

func (h *MessageHeader) unmarshal(r reader) error {
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
	ContentType        string
	ContentEncoding    string
	AbsoluteExpiryTime time.Time
	CreationTime       time.Time
	GroupID            string
	GroupSequence      uint32 // RFC-1982 sequence number
	ReplyToGroupID     string
}

func (p *MessageProperties) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeMessageProperties, []marshalField{
		{value: p.MessageID, omit: p.MessageID == nil},
		{value: p.UserID, omit: len(p.UserID) == 0},
		{value: p.To, omit: p.To == ""},
		{value: p.Subject, omit: p.Subject == ""},
		{value: p.ReplyTo, omit: p.ReplyTo == ""},
		{value: p.CorrelationID, omit: p.CorrelationID == nil},
		{value: symbol(p.ContentType), omit: p.ContentType == ""},
		{value: symbol(p.ContentEncoding), omit: p.ContentEncoding == ""},
		{value: p.AbsoluteExpiryTime, omit: p.AbsoluteExpiryTime.IsZero()},
		{value: p.CreationTime, omit: p.CreationTime.IsZero()},
		{value: p.GroupID, omit: p.GroupID == ""},
		{value: p.GroupSequence},
		{value: p.ReplyToGroupID, omit: p.ReplyToGroupID == ""},
	}...)
}

func (p *MessageProperties) unmarshal(r reader) error {
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

func (sr *stateReceived) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeStateReceived, []marshalField{
		{value: sr.SectionNumber, omit: false},
		{value: sr.SectionOffset, omit: false},
	}...)
}

func (sr *stateReceived) unmarshal(r reader) error {
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

func (sa *stateAccepted) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeStateAccepted)
}

func (sa *stateAccepted) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeStateAccepted)
}

func (sa *stateAccepted) String() string {
	return "Accepted"
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

func (sr *stateRejected) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeStateRejected,
		marshalField{value: sr.Error, omit: sr.Error == nil},
	)
}

func (sr *stateRejected) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeStateRejected,
		unmarshalField{field: &sr.Error},
	)
}

func (sr *stateRejected) String() string {
	return fmt.Sprintf("Rejected{Error: %v}", sr.Error)
}

/*
<type name="released" class="composite" source="list" provides="delivery-state, outcome">
    <descriptor name="amqp:released:list" code="0x00000000:0x00000026"/>
</type>
*/

type stateReleased struct{}

func (sr *stateReleased) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeStateReleased)
}

func (sr *stateReleased) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeStateReleased)
}

func (sr *stateReleased) String() string {
	return "Released"
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
	MessageAnnotations map[symbol]interface{}
}

func (sm *stateModified) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeStateModified, []marshalField{
		{value: sm.DeliveryFailed, omit: !sm.DeliveryFailed},
		{value: sm.UndeliverableHere, omit: !sm.UndeliverableHere},
		{value: sm.MessageAnnotations, omit: sm.MessageAnnotations == nil},
	}...)
}

func (sm *stateModified) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeStateModified, []unmarshalField{
		{field: &sm.DeliveryFailed},
		{field: &sm.UndeliverableHere},
		{field: &sm.MessageAnnotations},
	}...)
}

func (sm *stateModified) String() string {
	return fmt.Sprintf("Modified{DeliveryFailed: %t, UndeliverableHere: %t, MessageAnnotations: %v}", sm.DeliveryFailed, sm.UndeliverableHere, sm.MessageAnnotations)
}

/*
<type name="sasl-init" class="composite" source="list" provides="sasl-frame">
    <descriptor name="amqp:sasl-init:list" code="0x00000000:0x00000041"/>
    <field name="mechanism" type="symbol" mandatory="true"/>
    <field name="initial-response" type="binary"/>
    <field name="hostname" type="string"/>
</type>
*/

type saslInit struct {
	Mechanism       symbol
	InitialResponse []byte
	Hostname        string
}

func (si *saslInit) link() (uint32, bool) {
	return 0, false
}

func (si *saslInit) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeSASLInit, []marshalField{
		{value: si.Mechanism, omit: false},
		{value: si.InitialResponse, omit: len(si.InitialResponse) == 0},
		{value: si.Hostname, omit: len(si.Hostname) == 0},
	}...)
}

func (si *saslInit) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeSASLInit, []unmarshalField{
		{field: &si.Mechanism, handleNull: required("saslInit.Mechanism")},
		{field: &si.InitialResponse},
		{field: &si.Hostname},
	}...)
}

/*
<type name="sasl-mechanisms" class="composite" source="list" provides="sasl-frame">
    <descriptor name="amqp:sasl-mechanisms:list" code="0x00000000:0x00000040"/>
    <field name="sasl-server-mechanisms" type="symbol" multiple="true" mandatory="true"/>
</type>
*/

type saslMechanisms struct {
	Mechanisms []symbol
}

func (sm saslMechanisms) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeSASLMechanism, []marshalField{
		{value: sm.Mechanisms, omit: false},
	}...)
}

func (sm *saslMechanisms) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeSASLMechanism,
		unmarshalField{field: &sm.Mechanisms, handleNull: required("SASLMechanisms.Mechanisms")},
	)
}

func (*saslMechanisms) link() (uint32, bool) {
	return 0, false
}

/*
<type name="sasl-outcome" class="composite" source="list" provides="sasl-frame">
    <descriptor name="amqp:sasl-outcome:list" code="0x00000000:0x00000044"/>
    <field name="code" type="sasl-code" mandatory="true"/>
    <field name="additional-data" type="binary"/>
</type>
*/

type saslOutcome struct {
	Code           saslCode
	AdditionalData []byte
}

func (so saslOutcome) marshal(wr writer) error {
	return marshalComposite(wr, typeCodeSASLOutcome, []marshalField{
		{value: so.Code, omit: false},
		{value: so.AdditionalData, omit: len(so.AdditionalData) == 0},
	}...)
}

func (so *saslOutcome) unmarshal(r reader) error {
	return unmarshalComposite(r, typeCodeSASLOutcome, []unmarshalField{
		{field: &so.Code, handleNull: required("SASLOutcome.Code")},
		{field: &so.AdditionalData},
	}...)
}

func (*saslOutcome) link() (uint32, bool) {
	return 0, false
}

// symbol is an AMQP symbolic string.
type symbol string

func (s symbol) marshal(wr writer) error {
	l := len(s)

	var err error
	switch {
	// Sym8
	case l < 256:
		_, err = wr.Write(append([]byte{byte(typeCodeSym8), byte(l)}, []byte(s)...))

	// Sym32
	case l < math.MaxUint32:
		err = wr.WriteByte(uint8(typeCodeSym32))
		if err != nil {
			return err
		}
		err = binary.Write(wr, binary.BigEndian, uint32(l))
		if err != nil {
			return err
		}
		_, err = wr.Write([]byte(s))
	default:
		return errorNew("too long")
	}

	return err
}

type milliseconds time.Duration

func (m milliseconds) marshal(wr writer) error {
	return marshal(wr, uint32((time.Duration)(m).Nanoseconds()/1000000))
}

func (m *milliseconds) unmarshal(r reader) error {
	var n uint32
	_, err := unmarshal(r, &n)
	*m = milliseconds(time.Duration(n) * time.Millisecond)
	return err
}

// mapAnyAny is used to decode AMQP maps who's keys are undefined or
// inconsistently typed.
type mapAnyAny map[interface{}]interface{}

func (m mapAnyAny) marshal(wr writer) error {
	return writeMap(wr, map[interface{}]interface{}(m))
}

func (m *mapAnyAny) unmarshal(r reader) error {
	mr, err := newMapReader(r)
	if err != nil {
		return err
	}

	mm := make(mapAnyAny, mr.pairs())
	for mr.more() {
		var key interface{}
		var value interface{}
		err = mr.next(&key, &value)
		if err != nil {
			return err
		}

		// https://golang.org/ref/spec#Map_types:
		// The comparison operators == and != must be fully defined
		// for operands of the key type; thus the key type must not
		// be a function, map, or slice.
		switch reflect.ValueOf(key).Kind() {
		case reflect.Slice, reflect.Func, reflect.Map:
			return errorNew("invalid map key")
		}

		mm[key] = value
	}
	*m = mm

	return nil
}

// mapStringAny is used to decode AMQP maps that have string keys
type mapStringAny map[string]interface{}

func (m mapStringAny) marshal(wr writer) error {
	return writeMap(wr, map[string]interface{}(m))
}

func (m *mapStringAny) unmarshal(r reader) error {
	mr, err := newMapReader(r)
	if err != nil {
		return err
	}

	mm := make(mapStringAny, mr.pairs())
	for mr.more() {
		var key string
		var value interface{}
		err = mr.next(&key, &value)
		if err != nil {
			return err
		}
		mm[key] = value
	}
	*m = mm
	return nil
}

// mapStringAny is used to decode AMQP maps that have Symbol keys
type mapSymbolAny map[symbol]interface{}

func (m mapSymbolAny) marshal(wr writer) error {
	return writeMap(wr, map[symbol]interface{}(m))
}

func (f *mapSymbolAny) unmarshal(r reader) error {
	mr, err := newMapReader(r)
	if err != nil {
		return err
	}

	m := make(mapSymbolAny, mr.pairs())
	for mr.more() {
		var key symbol
		var value interface{}
		err = mr.next(&key, &value)
		if err != nil {
			return err
		}
		m[key] = value
	}
	*f = m
	return nil
}

// UUID is a 128 bit identifier as defined in RFC 4122.
type UUID [16]byte

// String returns the hex encoded representation described in RFC 4122, Section 3.
func (u UUID) String() string {
	var buf [36]byte
	hex.Encode(buf[:8], u[:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], u[10:])
	return string(buf[:])
}

func (u UUID) marshal(wr writer) error {
	err := wr.WriteByte(byte(typeCodeUUID))
	if err != nil {
		return err
	}
	_, err = wr.Write(u[:])
	return err
}

func (u *UUID) unmarshal(r reader) error {
	un, err := readUUID(r)
	*u = un
	return err
}

type lifetimePolicy uint8

const (
	deleteOnClose             = lifetimePolicy(typeCodeDeleteOnClose)
	deleteOnNoLinks           = lifetimePolicy(typeCodeDeleteOnNoLinks)
	deleteOnNoMessages        = lifetimePolicy(typeCodeDeleteOnNoMessages)
	deleteOnNoLinksOrMessages = lifetimePolicy(typeCodeDeleteOnNoLinksOrMessages)
)

func (p lifetimePolicy) marshal(wr writer) error {
	_, err := wr.Write([]byte{0x0, byte(typeCodeSmallUlong), byte(p), byte(typeCodeList0)})
	return err
}

func (p *lifetimePolicy) unmarshal(r reader) error {
	typ, fields, err := readCompositeHeader(r)
	if err != nil {
		return err
	}
	if fields != 0 {
		return errorErrorf("invalid size %d for lifetime-policy")
	}
	*p = lifetimePolicy(typ)
	return nil
}

func (p lifetimePolicy) unmarshalConstant(r reader) error {
	var tmp [4]byte
	n, err := r.Read(tmp[:])
	if err != nil {
		return err
	}
	if n != 4 {
		return errorErrorf("invalid size %d for lifetime-policy")
	}
	return nil
}

const (
	// ModeMixed specifies the sender will send all deliveries initially
	// unsettled to the receiver.
	ModeUnsettled SenderSettleMode = 0

	// ModeSettled specifies the sender will send all deliveries settled
	// to the receiver.
	ModeSettled SenderSettleMode = 1

	// ModeMixed specifies the sender MAY send a mixture of settled and
	// unsettled deliveries to the receiver.
	ModeMixed SenderSettleMode = 2
)

type SenderSettleMode uint8

func (m *SenderSettleMode) String() string {
	if m == nil {
		return "<nil>"
	}

	switch *m {
	case ModeUnsettled:
		return "unsettled"

	case ModeSettled:
		return "settled"

	case ModeMixed:
		return "mixed"

	default:
		return fmt.Sprintf("unknown sender mode %d", uint8(*m))
	}
}

func (m SenderSettleMode) marshal(wr writer) error {
	return marshal(wr, uint8(m))
}

func (m *SenderSettleMode) unmarshal(r reader) error {
	_, err := unmarshal(r, (*uint8)(m))
	return err
}

const (
	// ModeFirst specifies the receiver will spontaneously settle all
	// incoming transfers.
	ModeFirst ReceiverSettleMode = 0

	// ModeSecond specifies receiver will only settle after sending the
	// disposition to the sender and receiving a disposition indicating
	// settlement of the delivery from the sender.
	//
	// BUG: When receiving messages, accepting/rejecting/releasing a
	//      received message does not block and wait for the sender's
	//      confirmation disposition.
	ModeSecond ReceiverSettleMode = 1
)

type ReceiverSettleMode uint8

func (m *ReceiverSettleMode) String() string {
	if m == nil {
		return "<nil>"
	}

	switch *m {
	case ModeFirst:
		return "first"

	case ModeSecond:
		return "second"

	default:
		return fmt.Sprintf("unknown receiver mode %d", uint8(*m))
	}
}

func (m ReceiverSettleMode) marshal(wr writer) error {
	return marshal(wr, uint8(m))
}

func (m *ReceiverSettleMode) unmarshal(r reader) error {
	_, err := unmarshal(r, (*uint8)(m))
	return err
}

type describedType struct {
	descriptor interface{}
	value      interface{}
}

func (t describedType) marshal(wr writer) error {
	err := wr.WriteByte(0x0) // descriptor constructor
	if err != nil {
		return err
	}
	err = marshal(wr, t.descriptor)
	if err != nil {
		return err
	}
	return marshal(wr, t.value)
}

func (t *describedType) unmarshal(r reader) error {
	b, err := r.ReadByte()
	if b != 0x0 {
		return errorErrorf("invalid described type header %0x", b)
	}
	_, err = unmarshal(r, &t.descriptor)
	if err != nil {
		return err
	}
	_, err = unmarshal(r, &t.value)
	return err
}

func (t describedType) String() string {
	return fmt.Sprintf("describedType{descriptor: %v, value: %v}",
		t.descriptor,
		t.value,
	)
}
