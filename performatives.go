package amqp

import (
	"bytes"
	"errors"
	"time"
)

const (
	PerformativeOpen  = 0x10
	PreformativeBegin = 0x11
)

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
type Open struct {
	ContainerID         string // required
	Hostname            string
	MaxFrameSize        uint32        // default: 4294967295
	ChannelMax          uint16        // default: 65535
	IdleTimeout         time.Duration // from milliseconds
	OutgoingLocales     []Symbol
	IncomingLocales     []Symbol
	OfferedCapabilities []Symbol
	DesiredCapabilities []Symbol
	Properties          map[string]string // TODO: implement marshal/unmarshal
}

func (o *Open) MarshalBinary() ([]byte, error) {
	containerID, err := Marshal(o.ContainerID)
	if err != nil {
		return nil, err
	}

	fields := [][]byte{
		containerID,
	}

	var (
		hostname            []byte
		maxFrameSize        []byte
		channelMax          []byte
		idleTimeout         []byte
		outgoingLocales     []byte
		incomingLocales     []byte
		offeredCapabilities []byte
		desiredCapabilities []byte
	)

	if o.Hostname != "" {
		hostname, err = Marshal(o.Hostname)
		if err != nil {
			return nil, err
		}
	}

	if o.MaxFrameSize != 0 {
		maxFrameSize, err = Marshal(o.MaxFrameSize)
		if err != nil {
			return nil, err
		}
	}

	if o.ChannelMax != 0 {
		channelMax, err = Marshal(o.ChannelMax)
		if err != nil {
			return nil, err
		}
	}

	if o.IdleTimeout != 0 {
		idleTimeout, err = Marshal(o.IdleTimeout)
		if err != nil {
			return nil, err
		}
	}

	if len(o.OutgoingLocales) > 0 {
		outgoingLocales, err = Marshal(o.OutgoingLocales)
		if err != nil {
			return nil, err
		}
	}

	if len(o.IncomingLocales) > 0 {
		incomingLocales, err = Marshal(o.IncomingLocales)
		if err != nil {
			return nil, err
		}
	}

	if len(o.OfferedCapabilities) > 0 {
		offeredCapabilities, err = Marshal(o.OfferedCapabilities)
		if err != nil {
			return nil, err
		}
	}

	if len(o.DesiredCapabilities) > 0 {
		desiredCapabilities, err = Marshal(o.DesiredCapabilities)
		if err != nil {
			return nil, err
		}
	}

	optFields := [][]byte{hostname, maxFrameSize, channelMax, idleTimeout, outgoingLocales, incomingLocales, offeredCapabilities, desiredCapabilities}

	var lastSetIdx int
	for i, field := range optFields {
		if field != nil {
			lastSetIdx = i
		}
	}

	for _, field := range optFields[:lastSetIdx+1] {
		if field == nil {
			fields = append(fields, []byte{Null})
			continue
		}
		fields = append(fields, field)
	}

	buf := bufPool.New().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	err = writeComposite(buf, PerformativeOpen, fields...)
	if err != nil {
		return nil, err
	}

	return append([]byte(nil), buf.Bytes()...), nil
}

func (o *Open) UnmarshalBinary(r byteReader) error {
	t, fields, err := readCompositeHeader(r)
	if err != nil {
		return err
	}

	if t != PerformativeOpen {
		return errors.New("invalid header for Open")
	}

	err = Unmarshal(r, &o.ContainerID)
	if err != nil {
		return err
	}

	if fields > 1 {
		err = Unmarshal(r, &o.Hostname)
		if err != nil {
			return err
		}
	}

	o.MaxFrameSize = 4294967295 //default
	if fields > 2 {
		err = Unmarshal(r, &o.MaxFrameSize)
		if err != nil {
			return err
		}
	}

	o.ChannelMax = 65535 // default
	if fields > 3 {
		err = Unmarshal(r, &o.ChannelMax)
		if err != nil {
			return err
		}
	}

	if fields > 4 {
		err = Unmarshal(r, (*Milliseconds)(&o.IdleTimeout))
		if err != nil {
			return err
		}
	}

	if fields > 5 {
		err = Unmarshal(r, &o.OutgoingLocales)
		if err != nil {
			return err
		}
	}
	if fields > 6 {
		err = Unmarshal(r, &o.IncomingLocales)
		if err != nil {
			return err
		}
	}
	if fields > 7 {
		err = Unmarshal(r, &o.OfferedCapabilities)
		if err != nil {
			return err
		}
	}
	if fields > 8 {
		err = Unmarshal(r, &o.DesiredCapabilities)
		if err != nil {
			return err
		}
	}

	return nil
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
type Begin struct {
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
	OfferedCapabilities []Symbol

	// the extension capabilities the sender can use if the receiver supports them
	// The sender MUST NOT attempt to use any capability other than those it
	// has declared in desired-capabilities field.
	DesiredCapabilities []Symbol

	// session properties
	// http://www.amqp.org/specification/1.0/session-properties
	Properties map[string]interface{}
}

func (b *Begin) MarshalBinary() ([]byte, error) {
	var (
		remoteChannel       []byte
		nextOutgoingID      []byte
		incomingWindow      []byte
		outgoingWindow      []byte
		handleMax           []byte
		offeredCapabilities []byte
		desiredCapabilities []byte

		err error
	)

	if b.RemoteChannel != 0 {
		remoteChannel, err = Marshal(b.RemoteChannel)
		if err != nil {
			return nil, err
		}
	} else {
		remoteChannel = []byte{Null}
	}

	nextOutgoingID, err = Marshal(b.NextOutgoingID)
	if err != nil {
		return nil, err
	}

	incomingWindow, err = Marshal(b.IncomingWindow)
	if err != nil {
		return nil, err
	}

	outgoingWindow, err = Marshal(b.OutgoingWindow)
	if err != nil {
		return nil, err
	}

	fields := [][]byte{
		remoteChannel,
		nextOutgoingID,
		incomingWindow,
		outgoingWindow,
	}

	if b.HandleMax != 0 {
		handleMax, err = Marshal(b.HandleMax)
		if err != nil {
			return nil, err
		}
	}

	if len(b.OfferedCapabilities) > 0 {
		offeredCapabilities, err = Marshal(b.OfferedCapabilities)
		if err != nil {
			return nil, err
		}
	}

	if len(b.DesiredCapabilities) > 0 {
		desiredCapabilities, err = Marshal(b.DesiredCapabilities)
		if err != nil {
			return nil, err
		}
	}

	optFields := [][]byte{handleMax, offeredCapabilities, desiredCapabilities}

	var lastSetIdx int
	for i, field := range optFields {
		if field != nil {
			lastSetIdx = i
		}
	}

	for _, field := range optFields[:lastSetIdx+1] {
		if field == nil {
			fields = append(fields, []byte{Null})
			continue
		}
		fields = append(fields, field)
	}

	buf := bufPool.New().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	err = writeComposite(buf, PreformativeBegin, fields...)
	if err != nil {
		return nil, err
	}

	return append([]byte(nil), buf.Bytes()...), nil
}

func (b *Begin) UnmarshalBinary(r byteReader) error {
	t, fields, err := readCompositeHeader(r)
	if err != nil {
		return err
	}

	if t != PreformativeBegin {
		return errors.New("invalid header for Begin")
	}

	err = Unmarshal(r, &b.RemoteChannel)
	if err != nil {
		return err
	}

	err = Unmarshal(r, &b.NextOutgoingID)
	if err != nil {
		return err
	}

	err = Unmarshal(r, &b.IncomingWindow)
	if err != nil {
		return err
	}

	err = Unmarshal(r, &b.OutgoingWindow)
	if err != nil {
		return err
	}

	b.HandleMax = 4294967295 //default
	if fields > 4 {
		err = Unmarshal(r, &b.HandleMax)
		if err != nil {
			return err
		}
	}

	if fields > 5 {
		err = Unmarshal(r, &b.OfferedCapabilities)
		if err != nil {
			return err
		}
	}

	if fields > 6 {
		err = Unmarshal(r, &b.DesiredCapabilities)
		if err != nil {
			return err
		}
	}

	return nil
}
