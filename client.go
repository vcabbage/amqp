package amqp

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/url"
	"time"
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

// New establishes an AMQP client connection over conn.
func New(conn net.Conn, opts ...ConnOption) (*Client, error) {
	c, err := newConn(conn, opts...)
	return &Client{conn: c}, err
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
	s.txFrame(&performBegin{
		NextOutgoingID: 0,
		IncomingWindow: 1,
	})

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
	channel       uint16     // session's local channel
	remoteChannel uint16     // session's remote channel
	conn          *conn      // underlying conn
	rx            chan frame // frames destined for this session are sent on this chan by conn.mux

	allocateHandle   chan *link // link handles are allocated by sending a link on this channel, nil is sent on link.rx once allocated
	deallocateHandle chan *link // link handles are deallocated by sending a link on this channel
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

// txFrame sends a frame to the connWriter
func (s *Session) txFrame(p frameBody) {
	s.conn.wantWriteFrame(frame{
		typ:     frameTypeAMQP,
		channel: s.remoteChannel,
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

	r := &Receiver{
		link:        l,
		batching:    DefaultLinkBatching,
		batchMaxAge: DefaultLinkBatchMaxAge,
	}

	// add receiver to link so it can be configured via
	// link options.
	l.receiver = r

	// configure options
	for _, o := range opts {
		err := o(l)
		if err != nil {
			return nil, err
		}
	}

	// set creditUsed to linkCredit since no credit has been issued
	// to remote yet
	l.creditUsed = l.linkCredit

	// buffer rx to linkCredit so that conn.mux won't block
	// attempting to send to a slow reader
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

	// send Attach frame
	s.txFrame(&performAttach{
		Name:   randString(),
		Handle: l.handle,
		Role:   roleReceiver,
		Source: &source{
			Address: l.sourceAddr,
		},
	})

	// wait for response
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

	// deliveryCount is a sequence number, must initialize to sender's initial sequence number
	l.deliveryCount = resp.InitialDeliveryCount

	// create dispositions channel and start dispositionBatcher if batching enabled
	if r.batching {
		// buffer dispositions chan to prevent disposition sends from blocking
		r.dispositions = make(chan messageDisposition, l.linkCredit)
		go r.dispositionBatcher()
	}

	return r, nil
}

func (s *Session) mux() {
	links := make(map[uint32]*link) // mapping of handles to links
	var nextHandle uint32           // next handle # to be allocated

	for {
		select {
		// conn has completed, exit
		case <-s.conn.done:
			return

		// handle allocation request
		case l := <-s.allocateHandle:
			// TODO: handle max session/wrapping
			l.handle = nextHandle // allocate handle to the link
			links[nextHandle] = l // add to mapping
			nextHandle++          // increment the next handle
			l.rx <- nil           // send nil on channel to indicate allocation complete

		// handle deallocation request
		case l := <-s.deallocateHandle:
			delete(links, l.handle)
			close(l.rx) // close channel to indicate deallocation

		// incoming frame for link
		case fr := <-s.rx:
			// TODO: how should the two cases below be handled?
			//       proto error or alright to ignore?
			handle, ok := fr.body.link()
			if !ok {
				continue
			}

			link, ok := links[handle]
			if !ok {
				continue
			}

			select {
			case <-s.conn.done:
			case link.rx <- fr.body:
			}
		}
	}
}

// DetachError is returned by a link (Receiver) when a detach frame is received.
//
// RemoteError will be nil if the link was detached gracefully.
type DetachError struct {
	RemoteError *Error
}

func (e DetachError) Error() string {
	return fmt.Sprintf("link detached, reason: %+v", e.RemoteError)
}

// Default link options
const (
	DefaultLinkCredit      = 1
	DefaultLinkBatching    = true
	DefaultLinkBatchMaxAge = 5 * time.Second
)

// link is a unidirectional route.
//
// May be used for sending or receiving, currently only receive implemented.
type link struct {
	handle     uint32         // our handle
	sourceAddr string         // address sent during attach
	linkCredit uint32         // maximum number of messages allowed between flow updates
	rx         chan frameBody // sessions sends frames for this link on this channel
	session    *Session       // parent session
	receiver   *Receiver      // allows link options to modify Receiver

	creditUsed uint32 // currently used credits

	// "The delivery-count is initialized by the sender when a link endpoint is created,
	// and is incremented whenever a message is sent. Only the sender MAY independently
	// modify this field. The receiver's value is calculated based on the last known
	// value from the sender and any subsequent messages received on the link. Note that,
	// despite its name, the delivery-count is not a count but a sequence number
	// initialized at an arbitrary point by the sender."
	deliveryCount  uint32
	detachSent     bool // detach frame has been sent
	detachReceived bool
	err            error // err returned on Close()
}

// newLink is used by Receiver to create new links
func newLink(s *Session) *link {
	return &link{
		linkCredit: DefaultLinkCredit,
		session:    s,
	}
}

// close closes and requests deletion of the link.
//
// No operations on link are valid after close.
func (l *link) close(ctx context.Context) {
	// "A peer closes a link by sending the detach frame with the
	// handle for the specified link, and the closed flag set to
	// true. The partner will destroy the corresponding link
	// endpoint, and reply with its own detach frame with the
	// closed flag set to true.
	//
	// Note that one peer MAY send a closing detach while its
	// partner is sending a non-closing detach. In this case,
	// the partner MUST signal that it has closed the link by
	// reattaching and then sending a closing detach."
	if l.detachSent {
		return
	}

	l.session.txFrame(&performDetach{
		Handle: l.handle,
		Closed: true,
	})
	l.detachSent = true

	// already received detach from remote
	if l.detachReceived {
		select {
		case l.session.deallocateHandle <- l:
		case <-l.session.conn.done:
			l.err = l.session.conn.err
		}
		return
	}

	// wait for remote to detach
outer:
	for {
		// TODO: timeout
		select {
		// read from link until detach with Close == true is received,
		// other frames are discarded.
		case fr := <-l.rx:
			if fr, ok := fr.(*performDetach); ok && fr.Closed {
				break outer
			}

		// connection has ended
		case <-l.session.conn.done:
			l.err = l.session.conn.err
		}
	}

	// deallocate handle
	select {
	case l.session.deallocateHandle <- l:
	case <-l.session.conn.done:
		l.err = l.session.conn.err
	}
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
func LinkCredit(credit uint32) LinkOption {
	return func(l *link) error {
		l.linkCredit = credit
		return nil
	}
}

// LinkBatching toggles batching of message disposition.
//
// When enabled, accepting a message does not send the disposition
// to the server until the batch is equal to link credit or the
// batch max age expires.
func LinkBatching(enable bool) LinkOption {
	return func(l *link) error {
		l.receiver.batching = enable
		return nil
	}
}

// LinkBatchMaxAge sets the maximum time between the start
// of a disposition batch and sending the batch to the server.
func LinkBatchMaxAge(d time.Duration) LinkOption {
	return func(l *link) error {
		l.receiver.batchMaxAge = d
		return nil
	}
}

// Receiver receives messages on a single AMQP link.
type Receiver struct {
	link         *link                   // underlying link
	buf          bytes.Buffer            // resable buffer for decoding multi frame messages
	batching     bool                    // enable batching of message dispositions
	batchMaxAge  time.Duration           // maximum time between the start n batch and sending the batch to the server
	dispositions chan messageDisposition // message dispositions are sent on this channel when batching is enabled
}

// sendFlow transmits a flow frame with enough credits to bring the sender's
// link credits up to l.link.linkCredit.
func (r *Receiver) sendFlow() {
	// increment delivery count
	r.link.deliveryCount += r.link.creditUsed

	// send flow
	r.link.session.txFrame(&performFlow{
		IncomingWindow: math.MaxUint32, // max number of transfer frames
		NextOutgoingID: 0,
		OutgoingWindow: 0,
		Handle:         &r.link.handle,
		DeliveryCount:  &r.link.deliveryCount,
		LinkCredit:     &r.link.linkCredit, // max number of messages
	})

	// reset credit used
	r.link.creditUsed = 0
}

// Receive returns the next message from the sender.
//
// Blocks until a message is received, ctx completes, or an error occurs.
func (r *Receiver) Receive(ctx context.Context) (*Message, error) {
	r.buf.Reset()

	msg := &Message{receiver: r} // message to be decoded into

	first := true // receiving the first frame of the message
outer:
	for {
		// if linkCredit is half used, send more
		if r.link.creditUsed > r.link.linkCredit/2 {
			r.sendFlow()
		}

		// wait for the next frame
		var fr frameBody
		select {
		case <-r.link.session.conn.done:
			return nil, r.link.session.conn.err
		case fr = <-r.link.rx:
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		switch fr := fr.(type) {
		// message frame
		case *performTransfer:
			r.link.creditUsed++

			// record the delivery ID if this is the first frame of the message
			if first && fr.DeliveryID != nil {
				msg.id = (deliveryID)(*fr.DeliveryID)
				first = false
			}

			// add the payload the the buffer
			r.buf.Write(fr.Payload)

			// break out of loop if message is complete
			if !fr.More {
				break outer
			}

		// remote side is closing links
		case *performDetach:
			// don't currently support link detach and reattach
			if !fr.Closed {
				return nil, errorErrorf("non-closing detach not supported: %+v", fr)
			}

			// set detach received and close link
			r.link.detachReceived = true
			r.link.close(ctx)

			return nil, DetachError{fr.Error}
		}
	}

	// unmarshal message
	_, err := unmarshal(&r.buf, msg)
	return msg, err
}

// Close closes the Receiver and AMQP link.
func (r *Receiver) Close() error {
	// TODO: Should this timeout? Close() take a context? Use one of the
	// other timeouts?
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	r.link.close(ctx)
	cancel()
	return r.link.err
}

type messageDisposition struct {
	id          deliveryID
	disposition disposition
}

type deliveryID uint32

type disposition int

const (
	dispositionAccept disposition = iota
	dispositionReject
	dispositionRelease
)

func (r *Receiver) dispositionBatcher() {
	// batch operations:
	// Keep track of the first and last delivery ID, incrementing as
	// Accept() is called. After last-first == linkCredit, send disposition.
	// If Reject()/Release() is called, send one disposition for previously
	// accepted, and one for the rejected/released message. If messages are
	// accepted out of order, send any existing batch and the current message.
	var (
		batchSize    = r.link.linkCredit
		batchStarted bool
		first        deliveryID
		last         deliveryID
	)

	// disposition should be sent at least every batchMaxAge
	batchTimer := time.NewTimer(r.batchMaxAge)
	defer batchTimer.Stop()

	for {
		select {
		case msgDis := <-r.dispositions:

			// not accepted or batch out of order
			if msgDis.disposition != dispositionAccept || (batchStarted && last+1 != msgDis.id) {
				// send the current batch, if any
				if batchStarted {
					r.sendDisposition(first, &last, dispositionAccept)
					batchStarted = false
				}

				// send the current message
				r.sendDisposition(msgDis.id, nil, msgDis.disposition)
				continue
			}

			if batchStarted {
				// increment last
				last++
			} else {
				// start new batch
				batchStarted = true
				first = msgDis.id
				last = msgDis.id
				batchTimer.Reset(r.batchMaxAge)
			}

			// send batch if current size == batchSize
			if uint32(last-first+1) >= batchSize {
				r.sendDisposition(first, &last, dispositionAccept)
				batchStarted = false
				if !batchTimer.Stop() {
					<-batchTimer.C // batch timer must be drained if stop returns false
				}
			}

		// maxBatchAge elapsed, send batch
		case <-batchTimer.C:
			r.sendDisposition(first, &last, dispositionAccept)
			batchStarted = false
			batchTimer.Stop()

		case <-r.link.session.conn.done: // TODO: this should exit if link or session is closed
			return
		}
	}
}

// sendDisposition sends a disposition frame to the peer
func (r *Receiver) sendDisposition(first deliveryID, last *deliveryID, disp disposition) {
	fr := &performDisposition{
		Role:    roleReceiver,
		First:   uint32(first),
		Last:    (*uint32)(last),
		Settled: true,
	}

	switch disp {
	case dispositionAccept:
		fr.State = new(stateAccepted)
	case dispositionReject:
		fr.State = new(stateRejected)
	case dispositionRelease:
		fr.State = new(stateReleased)
	}

	r.link.session.txFrame(fr)
}

func (r *Receiver) acceptMessage(id deliveryID) {
	if r.batching {
		r.dispositions <- messageDisposition{id: id, disposition: dispositionAccept}
		return
	}
	r.sendDisposition(id, nil, dispositionAccept)
}

func (r *Receiver) rejectMessage(id deliveryID) {
	if r.batching {
		r.dispositions <- messageDisposition{id: id, disposition: dispositionReject}
		return
	}
	r.sendDisposition(id, nil, dispositionReject)
}

func (r *Receiver) releaseMessage(id deliveryID) {
	if r.batching {
		r.dispositions <- messageDisposition{id: id, disposition: dispositionRelease}
		return
	}
	r.sendDisposition(id, nil, dispositionRelease)
}
