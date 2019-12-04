package amqp

// FIXME(alanconway) INCOMPLETE: see panic("FIXME") and FIXME(alaconway) comments.

import (
	"context"
	"net"
	"time"
)

// ConnAllowIncoming allows incoming sessions and links to be accepted.
//
// If you pass this option you MUST start goroutines to service
// Conn.NextIncoming() and Session.NextIncoming(), see the example.
//
// The Server example in this package shows how to use NextIncoming().  Note
// however that AMQP is symmetric, and ConnAllowIncoming can be used with client
// or server connections.
func ConnAllowIncoming() ConnOption {
	return func(c *conn) error { c.incomingSession = make(chan *IncomingSession); return nil }
}

// IncomingConn represents an incoming OPEN request.
type IncomingConn struct {
	// ContainerID uniquely identifies the container for this connection.
	// A container usually corresponds to a process.
	ContainerID string
	// FIXME(alanconway) doc.
	Hostname            string
	MaxFrameSize        uint32        // default: 4294967295
	ChannelMax          uint16        // default: 65535
	IdleTimeout         time.Duration // from milliseconds
	OutgoingLocales     Symbols
	IncomingLocales     Symbols
	OfferedCapabilities Symbols
	DesiredCapabilities Symbols
	Properties          map[Symbol]interface{}

	conn *conn
}

// NewIncoming treats c as an incoming server connection (e.g. from
// net.Listener.Accept()) and reads the initial OPEN frame.
// You can examine connection properties in IncomingConn before
// deciding whether to Accept() or Close() the connection.
func NewIncoming(nc net.Conn) (ic *IncomingConn, err error) {
	defer func() {
		if err != nil {
			nc.Close()
		}
	}()
	ic = &IncomingConn{}
	if ic.conn, err = newConn(nc); err != nil {
		return nil, err
	}
	// FIXME(alanconway) data race:
	// start() starts mux goroutine but then Accept() applies connection options concurrently.
	err = ic.conn.start(ic.negotiateProto)
	if err != nil {
		return nil, err
	}
	return ic, err
}

// negotiateProto does server side protocol negotiation.
func (ic *IncomingConn) negotiateProto() stateFunc {
	c := ic.conn // Alias for brevity
	var p protoHeader
	if p, c.err = c.readProtoHeader(); c.err != nil {
		return nil
	}
	switch p.ProtoID {
	case protoTLS:
		panic("FIXME")
	case protoSASL:
		panic("FIXME")
	case protoAMQP:
		// FIXME(alanconway) MUST verify AMQP version and close if not supported.
		if c.err = c.writeProtoHeader(p.ProtoID); c.err != nil {
			return nil
		}
		// Read incoming open frame, but don't respond until Accept()
		var o *performOpen
		if o, c.err = c.recvOpen(); c.err != nil {
			return nil
		}
		ic.copyFrom(o)
		return nil
	default:
		c.err = errorErrorf("unknown protocol ID %#02x", p.ProtoID)
		return nil
	}
}

func (ic *IncomingConn) copyFrom(p *performOpen) {
	ic.ContainerID = p.ContainerID
	ic.Hostname = p.Hostname
	ic.MaxFrameSize = p.MaxFrameSize
	ic.ChannelMax = p.ChannelMax
	ic.IdleTimeout = p.IdleTimeout
	ic.OutgoingLocales = p.OutgoingLocales
	ic.IncomingLocales = p.IncomingLocales
	ic.OfferedCapabilities = p.OfferedCapabilities
	ic.DesiredCapabilities = p.DesiredCapabilities
	ic.Properties = p.Properties
}

// Accept sends an OPEN response and creates a Conn.
//
// Pass the ConnAllowIncoming() options to accept incoming sessions and links on
// this connection.
func (ic *IncomingConn) Accept(opts ...ConnOption) (*Conn, error) {
	c := ic.conn // Alias for brevity
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	if c.err = c.sendOpen(); c.err != nil {
		return nil, c.err
	}
	return &Conn{conn: c}, nil
}

// Close sends a CLOSE response to reject the OPEN request.
func (ic *IncomingConn) Close(err *Error) error {
	// FIXME(alanconway) not sending the error
	return ic.conn.Close()
}

// IncomingSession represents an incoming BEGIN request.
type IncomingSession struct {
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
	OfferedCapabilities Symbols

	// the extension capabilities the sender can use if the receiver supports them
	// The sender MUST NOT attempt to use any capability other than those it
	// has declared in desired-capabilities field.
	DesiredCapabilities Symbols

	// session properties
	// http://www.amqp.org/specification/1.0/session-properties
	Properties map[Symbol]interface{}

	session *Session
	begin   *performBegin // Remote begin
}

func newIncomingSession(s *Session, p *performBegin) *IncomingSession {
	return &IncomingSession{
		IncomingWindow:      p.IncomingWindow,
		OutgoingWindow:      p.OutgoingWindow,
		HandleMax:           p.HandleMax,
		OfferedCapabilities: p.OfferedCapabilities,
		DesiredCapabilities: p.DesiredCapabilities,
		Properties:          p.Properties,
		session:             s,
		begin:               p,
	}
}

// Accept sends a BEGIN response and creates a Session.
func (is *IncomingSession) Accept(opts ...SessionOption) (*Session, error) {
	// FIXME(alanconway) verify that we are correctly sending/using settings
	// from begin frames in both directions.
	s := is.session
	err := s.sendBegin(opts, true)
	if err != nil {
		return nil, err
	}
	// start Session multiplexor
	go s.mux(is.begin)
	return s, nil
}

// Close sends an END response to reject the BEGIN request.
func (*IncomingSession) Close(*Error) error { panic("FIXME") }

// IncomingLink is the common interface for *IncomingSender and *IncomingReceiver
type IncomingLink interface {
	Address() string
	Close(context.Context) error
}

// IncomingLinkAttach is the common data for an incoming link attach request,
// embedded in IncomingSender and IncomingReceiver
type IncomingLinkAttach struct {
	// Name uniquely identifies the link from the container of the source
	// to the container of the target node, e.g., if the container of the source
	// node is A, and the container of the target node is B, the link is
	// globally identified by the (ordered) tuple (A,B,<name>).
	Name string

	// SenderSettleMode is the settlement policy for the sender
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

	// ReceiverSettleMode is the settlement policy of the receiver
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
	Source *Source

	// the target for messages
	//
	// If no target is specified on an incoming link, then there is no target currently
	// attached to the link. A link with no target will never permit incoming messages.
	Target *Target

	// the maximum message size supported by the link endpoint
	//
	// This field indicates the maximum message size supported by the link endpoint.
	// Any attempt to deliver a message larger than this results in a message-size-exceeded
	// link-error. If this field is zero or unset, there is no maximum size imposed by the
	// link endpoint.
	MaxMessageSize uint64

	// the extension capabilities the sender supports
	// http://www.amqp.org/specification/1.0/link-capabilities
	OfferedCapabilities Symbols

	// the extension capabilities the sender can use if the receiver supports them
	//
	// The sender MUST NOT attempt to use any capability other than those it
	// has declared in desired-capabilities field.
	DesiredCapabilities Symbols

	// link properties
	// http://www.amqp.org/specification/1.0/link-properties
	Properties map[Symbol]interface{}

	// attach frame from incoming request.
	request *performAttach
}

func (i *IncomingLinkAttach) accept(l *link, opts ...LinkOption) error {
	err := l.apply(opts...)
	if err != nil {
		return err
	}
	// FIXME(alanconway) check spec to copy/override settings correctly.
	l.name = i.request.Name
	err = l.remoteAttach(i.request)
	if err != nil {
		return err
	}
	err = l.sendAttach()
	if err != nil {
		return err
	}
	go l.mux()
	return nil
}

type IncomingSender struct {
	IncomingLinkAttach
	sender *Sender
}

func (i *IncomingSender) Close(ctx context.Context) error { return i.sender.Close(ctx) }
func (i *IncomingSender) Address() string                 { return i.sender.Address() }
func (i *IncomingSender) Accept(opts ...LinkOption) (*Sender, error) {
	err := i.accept(i.sender.link, opts...)
	return i.sender, err
}

type IncomingReceiver struct {
	IncomingLinkAttach
	receiver *Receiver
}

func (i *IncomingReceiver) Close(ctx context.Context) error { return i.receiver.Close(ctx) }
func (i *IncomingReceiver) Address() string                 { return i.receiver.Address() }
func (i *IncomingReceiver) Accept(opts ...LinkOption) (*Receiver, error) {
	err := i.accept(i.receiver.link, opts...)
	return i.receiver, err
}

func newIncomingLink(ssn *Session, request *performAttach) IncomingLink {
	a := IncomingLinkAttach{
		Name:                request.Name,
		SenderSettleMode:    request.SenderSettleMode,
		ReceiverSettleMode:  request.ReceiverSettleMode,
		Source:              request.Source,
		Target:              request.Target,
		MaxMessageSize:      request.MaxMessageSize,
		OfferedCapabilities: request.OfferedCapabilities,
		DesiredCapabilities: request.DesiredCapabilities,
		Properties:          request.Properties,
		request:             request,
	}
	isReceiver := (request.Role == roleSender) // Local role opposite to remote.
	if isReceiver {
		r := &IncomingReceiver{IncomingLinkAttach: a, receiver: ssn.newReceiver()}
		r.receiver.link = newLink(ssn, r.receiver)
		return r
	} else {
		return &IncomingSender{IncomingLinkAttach: a, sender: &Sender{link: newLink(ssn, nil)}}
	}
}

func linkFrom(il IncomingLink) *link {
	switch il := il.(type) {
	case *IncomingSender:
		return il.sender.link
	case *IncomingReceiver:
		return il.receiver.link
	default:
		return nil
	}
}
