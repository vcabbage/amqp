package amqp

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/url"
)

// Client is an AMQP client connection.
type Client struct {
	conn *conn
}

// Dial connects to an AMQP server.
//
// If the addr includes a scheme, it must be "amqp" or "amqps".
// TLS will be negotiated when the scheme is "amqps".
//
// If no port is provided, 5672 will be used.
func Dial(addr string, opts ...ConnOption) (*Client, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		host = u.Host
		port = "5672" // use default AMQP if parse fails
	}

	switch u.Scheme {
	case "amqp", "amqps", "":
	default:
		return nil, errorErrorf("unsupported scheme %q", u.Scheme)
	}

	conn, err := net.Dial("tcp", host+":"+port)
	if err != nil {
		return nil, err
	}

	// append default options so user specified can overwrite
	opts = append([]ConnOption{
		ConnServerHostname(host),
		ConnTLS(u.Scheme == "amqps"),
	}, opts...)

	c, err := New(conn, opts...)
	if err != nil {
		return nil, err
	}

	return c, err
}

// New establishes an AMQP client connection on a pre-established
// net.Conn.
func New(netConn net.Conn, opts ...ConnOption) (*Client, error) {
	c := &conn{
		net:              netConn,
		maxFrameSize:     defaultMaxFrameSize,
		peerMaxFrameSize: defaultMaxFrameSize,
		channelMax:       defaultChannelMax,
		idleTimeout:      defaultIdleTimeout,
		done:             make(chan struct{}),
		readErr:          make(chan error, 1), // buffered to ensure connReader doesn't leak
		rxProto:          make(chan protoHeader),
		rxFrame:          make(chan frame),
		newSession:       make(chan *Session),
		delSession:       make(chan *Session),
	}

	// apply options
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	// start connReader
	go c.connReader()

	// run connection establishment state machine
	for state := c.negotiateProto; state != nil; {
		state = state()
	}

	// check if err occurred
	if c.err != nil {
		c.close()
		return nil, c.err
	}

	// start multiplexor
	go c.mux()

	return &Client{conn: c}, nil
}

// Close disconnects the connection.
func (c *Client) Close() error {
	return c.conn.close()
}

// NewSession opens a new AMQP session to the server.
func (c *Client) NewSession() (*Session, error) {
	// get a session allocated by Client.mux
	var s *Session
	select {
	case <-c.conn.done:
		return nil, c.conn.err
	case s = <-c.conn.newSession:
	}

	// send Begin to server
	err := s.txFrame(&performBegin{
		NextOutgoingID: 0,
		IncomingWindow: 1,
	})
	if err != nil {
		s.Close()
		return nil, err
	}

	// wait for response
	var fr frame
	select {
	case <-c.conn.done:
		return nil, c.conn.err
	case fr = <-s.rx:
	}

	begin, ok := fr.body.(*performBegin)
	if !ok {
		s.Close() // deallocate session on error
		return nil, errorErrorf("unexpected begin response: %+v", fr)
	}

	// TODO: record negotiated settings
	s.remoteChannel = begin.RemoteChannel

	// start Session multiplexor
	go s.mux()

	return s, nil
}

// Session is an AMQP session.
//
// A session multiplexes Receivers.
type Session struct {
	channel       uint16
	remoteChannel uint16
	conn          *conn
	rx            chan frame

	allocateHandle   chan *link
	deallocateHandle chan *link
}

func newSession(c *conn, channel uint16) *Session {
	return &Session{
		conn:             c,
		channel:          channel,
		rx:               make(chan frame),
		allocateHandle:   make(chan *link),
		deallocateHandle: make(chan *link),
	}
}

// Close closes the session.
func (s *Session) Close() error {
	// TODO: send end preformative (if Begin has been exchanged)
	select {
	case <-s.conn.done:
		return s.conn.err
	case s.conn.delSession <- s:
		return nil
	}
}

func (s *Session) txFrame(p frameBody) error {
	return s.conn.txFrame(frame{
		typ:     frameTypeAMQP,
		channel: s.channel,
		body:    p,
	})
}

func randString() string { // TODO: random string gen off SO, replace
	var letterBytes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]byte, 40)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

// NewReceiver opens a new receiver link on the session.
func (s *Session) NewReceiver(opts ...LinkOption) (*Receiver, error) {
	l := newLink(s)

	// configure options
	for _, o := range opts {
		err := o(l)
		if err != nil {
			return nil, err
		}
	}
	l.creditUsed = l.linkCredit
	l.rx = make(chan frameBody, l.linkCredit)

	// request handle from Session.mux
	select {
	case <-s.conn.done:
		return nil, s.conn.err
	case s.allocateHandle <- l:
	}

	// wait for handle allocation
	select {
	case <-s.conn.done:
		return nil, s.conn.err
	case <-l.rx:
	}

	s.txFrame(&performAttach{
		Name:   randString(),
		Handle: l.handle,
		Role:   true,
		Source: &source{
			Address: l.sourceAddr,
		},
	})

	var fr frameBody
	select {
	case <-s.conn.done:
		return nil, s.conn.err
	case fr = <-l.rx:
	}
	resp, ok := fr.(*performAttach)
	if !ok {
		return nil, errorErrorf("unexpected attach response: %+v", fr)
	}

	l.senderDeliveryCount = resp.InitialDeliveryCount

	return &Receiver{
		link: l,
		buf:  bufPool.New().(*bytes.Buffer),
	}, nil
}

func (s *Session) mux() {
	links := make(map[uint32]*link)
	var nextHandle uint32

	for {
		select {
		case <-s.conn.done:
			return
		case l := <-s.allocateHandle:
			l.handle = nextHandle
			links[nextHandle] = l
			nextHandle++ // TODO: handle max session/wrapping
			l.rx <- nil

		case l := <-s.deallocateHandle:
			delete(links, l.handle)
			close(l.rx)

		case fr := <-s.rx:
			handle, ok := fr.body.link()
			if !ok {
				log.Printf("unexpected frame: %+v (%T)", fr, fr.body)
				continue
			}

			link, ok := links[handle]
			if !ok {
				log.Printf("frame with unknown handle %d: %+v", handle, fr)
				continue
			}

			select {
			case <-s.conn.done:
			case link.rx <- fr.body:
			}
		}
	}
}

// ErrDetach is returned by a link (Receiver) when a detach frame is received.
//
// RemoteError will be nil if the link was detached gracefully.
type ErrDetach struct {
	RemoteError *Error
}

func (e ErrDetach) Error() string {
	return fmt.Sprintf("link detached, reason: %+v", e.RemoteError)
}

// link is a unidirectional route.
//
// May be used for sending or receiving, currently only receive implemented.
type link struct {
	handle     uint32         // our handle
	sourceAddr string         // address sent during attach
	linkCredit uint32         // maximum number of messages allowed between flow updates
	rx         chan frameBody // sessions sends frames for this link on this channel
	session    *Session       // parent session

	creditUsed          uint32 // currently used credits
	senderDeliveryCount uint32 // number of messages sent/received
	detachSent          bool   // we've sent a detach frame
	detachReceived      bool
	err                 error // err returned on Close()
}

// newLink is used by Session.mux to create new links
func newLink(s *Session) *link {
	return &link{
		linkCredit: 1,
		session:    s,
	}
}

// close closes and requests deletion of the link.
//
// No operations on link are valid after close.
func (l *link) close() {
	if l.detachSent {
		return
	}

	l.session.txFrame(&performDetach{
		Handle: l.handle,
		Closed: true,
	})
	l.detachSent = true

	if !l.detachReceived {
	outer:
		for {
			// TODO: timeout
			select {
			case <-l.session.conn.done:
				l.err = l.session.conn.err
			case fr := <-l.rx:
				if fr, ok := fr.(*performDetach); ok && fr.Closed {
					break outer
				}
			}
		}
	}

	l.session.deallocateHandle <- l
}

// LinkOption is an function for configuring an AMQP links.
//
// A link may be a Sender or a Receiver. Only Receiver is currently implemented.
type LinkOption func(*link) error

// LinkSource sets the source address.
func LinkSource(source string) LinkOption {
	return func(l *link) error {
		l.sourceAddr = source
		return nil
	}
}

// LinkCredit specifies the maximum number of unacknowledged messages
// the sender can transmit.
func LinkCredit(credit uint32) LinkOption { // TODO: make receiver specific?
	return func(l *link) error {
		l.linkCredit = credit
		return nil
	}
}

// Receiver receives messages on a single AMQP link.
type Receiver struct {
	link *link

	buf *bytes.Buffer
}

// sendFlow transmits a flow frame with enough credits to bring the sender's
// link credits up to l.link.linkCredit.
func (r *Receiver) sendFlow() error {
	newLinkCredit := r.link.linkCredit - (r.link.linkCredit - r.link.creditUsed)
	r.link.senderDeliveryCount += r.link.creditUsed
	err := r.link.session.txFrame(&performFlow{
		IncomingWindow: 2147483647,
		NextOutgoingID: 0,
		OutgoingWindow: 0,
		Handle:         &r.link.handle,
		DeliveryCount:  &r.link.senderDeliveryCount,
		LinkCredit:     &newLinkCredit,
	})
	r.link.creditUsed = 0
	return err
}

// Receive returns the next message from the sender.
//
// Blocks until a message is received, ctx completes, or an error occurs.
func (r *Receiver) Receive(ctx context.Context) (*Message, error) {
	r.buf.Reset()

	msg := &Message{link: r.link}

	first := true
outer:
	for {
		if r.link.creditUsed > r.link.linkCredit/2 {
			err := r.sendFlow()
			if err != nil {
				return nil, err
			}
		}

		var fr frameBody
		select {
		case <-r.link.session.conn.done:
			return nil, r.link.session.conn.err
		case fr = <-r.link.rx:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		switch fr := fr.(type) {
		case *performTransfer:
			r.link.creditUsed++

			if first && fr.DeliveryID != nil {
				msg.deliveryID = *fr.DeliveryID
				first = false
			}

			r.buf.Write(fr.Payload)
			if !fr.More {
				break outer
			}
		case *performDetach:
			if !fr.Closed {
				log.Panicf("non-closing detach not supported: %+v", fr)
			}

			r.link.detachReceived = true
			r.link.close()

			return nil, ErrDetach{fr.Error}
		}
	}

	_, err := unmarshal(r.buf, msg)
	return msg, err
}

// Close closes the Receiver and AMQP link.
func (r *Receiver) Close() error {
	r.link.close()
	bufPool.Put(r.buf)
	r.buf = nil
	return r.link.err
}
