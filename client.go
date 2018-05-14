package amqp

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrSessionClosed is propagated to Sender/Receivers
	// when Session.Close() is called.
	ErrSessionClosed = errors.New("amqp: session closed")

	// ErrLinkClosed returned by send and receive operations when
	// Sender.Close() or Receiver.Close() are called.
	ErrLinkClosed = errors.New("amqp: link closed")
)

// Client is an AMQP client connection.
type Client struct {
	conn *conn
}

// Dial connects to an AMQP server.
//
// If the addr includes a scheme, it must be "amqp" or "amqps".
// If no port is provided, 5672 will be used for "amqp" and 5671 for "amqps".
//
// If username and password information is not empty it's used as SASL PLAIN
// credentials, equal to passing ConnSASLPlain option.
func Dial(addr string, opts ...ConnOption) (*Client, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		host = u.Host
		port = "5672" // use default port values if parse fails
		if u.Scheme == "amqps" {
			port = "5671"
		}
	}

	// prepend SASL credentials when the user/pass segment is not empty
	if u.User != nil {
		pass, _ := u.User.Password()
		opts = append([]ConnOption{
			ConnSASLPlain(u.User.Username(), pass),
		}, opts...)
	}

	// append default options so user specified can overwrite
	opts = append([]ConnOption{
		ConnServerHostname(host),
	}, opts...)

	c, err := newConn(nil, opts...)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "amqp", "":
		c.net, err = net.Dial("tcp", host+":"+port)
	case "amqps":
		c.initTLSConfig()
		c.tlsNegotiation = false
		c.net, err = tls.Dial("tcp", host+":"+port, c.tlsConfig)
	default:
		return nil, errorErrorf("unsupported scheme %q", u.Scheme)
	}
	if err != nil {
		return nil, err
	}
	err = c.start()
	return &Client{conn: c}, err
}

// New establishes an AMQP client connection over conn.
func New(conn net.Conn, opts ...ConnOption) (*Client, error) {
	c, err := newConn(conn, opts...)
	if err != nil {
		return nil, err
	}
	err = c.start()
	return &Client{conn: c}, err
}

// Close disconnects the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// NewSession opens a new AMQP session to the server.
func (c *Client) NewSession(opts ...SessionOption) (*Session, error) {
	// get a session allocated by Client.mux
	var sResp newSessionResp
	select {
	case <-c.conn.done:
		return nil, c.conn.getErr()
	case sResp = <-c.conn.newSession:
	}

	if sResp.err != nil {
		return nil, sResp.err
	}
	s := sResp.session

	for _, opt := range opts {
		err := opt(s)
		if err != nil {
			_ = s.Close(context.Background()) // deallocate session on error
			return nil, err
		}
	}

	// send Begin to server
	begin := &performBegin{
		NextOutgoingID: 0,
		IncomingWindow: s.incomingWindow,
		OutgoingWindow: s.outgoingWindow,
		HandleMax:      s.handleMax,
	}
	debug(1, "TX: %s", begin)
	s.txFrame(begin, nil)

	// wait for response
	var fr frame
	select {
	case <-c.conn.done:
		return nil, c.conn.getErr()
	case fr = <-s.rx:
	}
	debug(1, "RX: %s", fr.body)

	begin, ok := fr.body.(*performBegin)
	if !ok {
		_ = s.Close(context.Background()) // deallocate session on error
		return nil, errorErrorf("unexpected begin response: %+v", fr.body)
	}

	// start Session multiplexor
	go s.mux(begin)

	return s, nil
}

// Default session options
const (
	DefaultMaxLinks = 4294967296
	DefaultWindow   = 100
)

// SessionOption is an function for configuring an AMQP session.
type SessionOption func(*Session) error

// SessionIncomingWindow sets the maximum number of unacknowledged
// transfer frames the server can send.
func SessionIncomingWindow(window uint32) SessionOption {
	return func(s *Session) error {
		s.incomingWindow = window
		return nil
	}
}

// SessionOutgoingWindow sets the maximum number of unacknowledged
// transfer frames the client can send.
func SessionOutgoingWindow(window uint32) SessionOption {
	return func(s *Session) error {
		s.outgoingWindow = window
		return nil
	}
}

// SessionMaxLinks sets the maximum number of links (Senders/Receivers)
// allowed on the session.
//
// n must be in the range 1 to 4294967296.
//
// Default: 4294967296.
func SessionMaxLinks(n int) SessionOption {
	return func(s *Session) error {
		if n < 1 {
			return errorNew("max sessions cannot be less than 1")
		}
		if n > 4294967296 {
			return errorNew("max sessions cannot be greater than 4294967296")
		}
		s.handleMax = uint32(n - 1)
		return nil
	}
}

// Session is an AMQP session.
//
// A session multiplexes Receivers.
type Session struct {
	channel       uint16                // session's local channel
	remoteChannel uint16                // session's remote channel, owned by conn.mux
	conn          *conn                 // underlying conn
	rx            chan frame            // frames destined for this session are sent on this chan by conn.mux
	tx            chan frameBody        // non-transfer frames to be sent; session must track disposition
	txTransfer    chan *performTransfer // transfer frames to be sent; session must track disposition

	// flow control
	incomingWindow uint32
	outgoingWindow uint32

	handleMax        uint32
	allocateHandle   chan *link // link handles are allocated by sending a link on this channel, nil is sent on link.rx once allocated
	deallocateHandle chan *link // link handles are deallocated by sending a link on this channel

	nextDeliveryID uint32 // atomically accessed sequence for deliveryIDs

	// used for gracefully closing link
	close     chan struct{}
	closeOnce sync.Once
	done      chan struct{}
	err       error
}

func newSession(c *conn, channel uint16) *Session {
	return &Session{
		conn:             c,
		channel:          channel,
		rx:               make(chan frame),
		tx:               make(chan frameBody),
		txTransfer:       make(chan *performTransfer),
		incomingWindow:   DefaultWindow,
		outgoingWindow:   DefaultWindow,
		handleMax:        DefaultMaxLinks - 1,
		allocateHandle:   make(chan *link),
		deallocateHandle: make(chan *link),
		close:            make(chan struct{}),
		done:             make(chan struct{}),
	}
}

// Close gracefully closes the session.
//
// If ctx expires while waiting for servers response, ctx.Err() will be returned.
// The session will continue to wait for the response until the Client is closed.
func (s *Session) Close(ctx context.Context) error {
	s.closeOnce.Do(func() { close(s.close) })
	select {
	case <-s.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if s.err == ErrSessionClosed {
		return nil
	}
	return s.err
}

// txFrame sends a frame to the connWriter
func (s *Session) txFrame(p frameBody, done chan deliveryState) {
	s.conn.wantWriteFrame(frame{
		type_:   frameTypeAMQP,
		channel: s.channel,
		body:    p,
		done:    done,
	})
}

// randBytes returns a base64 encoded string of n bytes.
//
// A new random source is created to avoid any issues with seeding
// of the global source.
func randString(n int) string {
	localRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, n)
	localRand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// NewReceiver opens a new receiver link on the session.
func (s *Session) NewReceiver(opts ...LinkOption) (*Receiver, error) {
	r := &Receiver{
		batching:    DefaultLinkBatching,
		batchMaxAge: DefaultLinkBatchMaxAge,
		maxCredit:   DefaultLinkCredit,
	}

	l, err := attachLink(s, r, opts)
	if err != nil {
		return nil, err
	}

	r.link = l

	// batching is just extra overhead when maxCredits == 1
	if r.maxCredit == 1 {
		r.batching = false
	}

	// create dispositions channel and start dispositionBatcher if batching enabled
	if r.batching {
		// buffer dispositions chan to prevent disposition sends from blocking
		r.dispositions = make(chan messageDisposition, r.maxCredit)
		go r.dispositionBatcher()
	}

	return r, nil
}

// Sender sends messages on a single AMQP link.
type Sender struct {
	link *link

	mu              sync.Mutex // protects buf and nextDeliveryTag
	buf             buffer
	nextDeliveryTag uint64
}

// Send sends a Message.
//
// Blocks until the message is sent, ctx completes, or an error occurs.
//
// Send is safe for concurrent use. Since only a single message can be
// sent on a link at a time, this is most useful when settlement confirmation
// has been requested (receiver settle mode is "Second"). In this case,
// additional messages can be sent while the current goroutine is waiting
// for the confirmation.
func (s *Sender) Send(ctx context.Context, msg *Message) error {
	done, err := s.send(ctx, msg)
	if err != nil {
		return err
	}

	// wait for transfer to be confirmed
	select {
	case state := <-done:
		if state, ok := state.(*stateRejected); ok {
			return state.Error
		}
		return nil
	case <-s.link.done:
		return s.link.err
	case <-ctx.Done():
		return errorWrapf(ctx.Err(), "awaiting send")
	}
}

// send is separated from Send so that the mutex unlock can be deferred without
// locking the transfer confirmation that happens in Send.
func (s *Sender) send(ctx context.Context, msg *Message) (chan deliveryState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.buf.reset()
	err := msg.marshal(&s.buf)
	if err != nil {
		return nil, err
	}

	if s.link.maxMessageSize != 0 && uint64(s.buf.len()) > s.link.maxMessageSize {
		return nil, errorErrorf("encoded message size exceeds max of %d", s.link.maxMessageSize)
	}

	var (
		maxPayloadSize = int(s.link.session.conn.peerMaxFrameSize) - maxTransferFrameHeader
		sndSettleMode  = s.link.senderSettleMode
		rcvSettleMode  = s.link.receiverSettleMode
		senderSettled  = sndSettleMode != nil && *sndSettleMode == ModeSettled
		deliveryID     = atomic.AddUint32(&s.link.session.nextDeliveryID, 1)
	)

	// use uint64 encoded as []byte as deliveryTag
	deliveryTag := make([]byte, 8)
	binary.BigEndian.PutUint64(deliveryTag, s.nextDeliveryTag)
	s.nextDeliveryTag++

	fr := performTransfer{
		Handle:        s.link.handle,
		DeliveryID:    &deliveryID,
		DeliveryTag:   deliveryTag,
		MessageFormat: &msg.Format,
		More:          s.buf.len() > 0,
	}

	for fr.More {
		buf, _ := s.buf.next(maxPayloadSize)
		fr.Payload = append([]byte(nil), buf...)
		fr.More = s.buf.len() > 0
		if !fr.More {
			// mark final transfer as settled when sender mode is settled
			fr.Settled = senderSettled

			// set done on last frame to be closed after network transmission
			//
			// If confirmSettlement is true (ReceiverSettleMode == "second"),
			// Session.mux will intercept the done channel and close it when the
			// receiver has confirmed settlement instead of on net transmit.
			fr.done = make(chan deliveryState, 1)
			fr.confirmSettlement = rcvSettleMode != nil && *rcvSettleMode == ModeSecond
		}

		select {
		case s.link.transfers <- fr:
		case <-s.link.done:
			return nil, s.link.err
		case <-ctx.Done():
			return nil, errorWrapf(ctx.Err(), "awaiting send")
		}

		// clear values that are only required on first message
		fr.DeliveryID = nil
		fr.DeliveryTag = nil
		fr.MessageFormat = nil
	}

	return fr.done, nil
}

// Address returns the link's address.
func (s *Sender) Address() string {
	if s.link.target == nil {
		return ""
	}
	return s.link.target.Address
}

// Close closes the Sender and AMQP link.
func (s *Sender) Close(ctx context.Context) error {
	return s.link.Close(ctx)
}

// NewSender opens a new sender link on the session.
func (s *Session) NewSender(opts ...LinkOption) (*Sender, error) {
	l, err := attachLink(s, nil, opts)
	if err != nil {
		return nil, err
	}

	return &Sender{link: l}, nil
}

func (s *Session) mux(remoteBegin *performBegin) {
	defer close(s.done)

	var (
		links       = make(map[uint32]*link)    // mapping of remote handles to links
		linksByName = make(map[string]*link)    // maping of names to links
		handles     = &bitmap{max: s.handleMax} // allocated handles

		handlesByDeliveryID = make(map[uint32]uint32) // mapping of deliveryIDs to handles
		deliveryIDByHandle  = make(map[uint32]uint32) // mapping of handles to latest deliveryID

		settlementByDeliveryID = make(map[uint32]chan deliveryState)

		// flow control values
		nextOutgoingID       uint32
		nextIncomingID       = remoteBegin.NextOutgoingID
		remoteIncomingWindow = remoteBegin.IncomingWindow
		remoteOutgoingWindow = remoteBegin.OutgoingWindow
	)

	for {
		txTransfer := s.txTransfer
		// disable txTransfer if flow control windows have been exceeded
		if remoteIncomingWindow == 0 || s.outgoingWindow == 0 {
			txTransfer = nil
		}

		select {
		// conn has completed, exit
		case <-s.conn.done:
			s.err = s.conn.getErr()
			return

		// session is being closed by user
		case <-s.close:
			s.txFrame(&performEnd{}, nil)

			// discard frames until End is received or conn closed
		EndLoop:
			for {
				select {
				case fr := <-s.rx:
					_, ok := fr.body.(*performEnd)
					if ok {
						break EndLoop
					}
				case <-s.conn.done:
					s.err = s.conn.getErr()
					return
				}
			}

			// release session
			select {
			case s.conn.delSession <- s:
				s.err = ErrSessionClosed
			case <-s.conn.done:
				s.err = s.conn.getErr()
			}
			return

		// handle allocation request
		case l := <-s.allocateHandle:
			next, ok := handles.next()
			if !ok {
				l.err = errorErrorf("reached session handle max (%d)", s.handleMax)
				l.rx <- nil
				continue
			}

			l.handle = next         // allocate handle to the link
			linksByName[l.name] = l // add to mapping
			l.rx <- nil             // send nil on channel to indicate allocation complete

		// handle deallocation request
		case l := <-s.deallocateHandle:
			delete(links, l.remoteHandle)
			delete(deliveryIDByHandle, l.handle)
			handles.remove(l.handle)
			close(l.rx) // close channel to indicate deallocation

		// incoming frame for link
		case fr := <-s.rx:
			debug(1, "RX(Session): %s", fr.body)

			switch body := fr.body.(type) {
			// Disposition frames can reference transfers from more than one
			// link. Send this frame to all of them.
			case *performDisposition:
				start := body.First
				end := start
				if body.Last != nil {
					end = *body.Last
				}
				for deliveryID := start; deliveryID <= end; deliveryID++ {
					handle, ok := handlesByDeliveryID[deliveryID]
					if !ok {
						continue
					}
					delete(handlesByDeliveryID, deliveryID)

					if body.Settled {
						// check if settlement confirmation was requested, if so
						// confirm by closing channel
						if done, ok := settlementByDeliveryID[deliveryID]; ok {
							delete(settlementByDeliveryID, deliveryID)
							select {
							case done <- body.State:
							default:
							}
							close(done)
						}
					}

					link, ok := links[handle]
					if !ok {
						continue
					}

					s.muxFrameToLink(link, fr.body)
				}
				continue
			case *performFlow:
				if body.NextIncomingID == nil {
					// This is a protocol error:
					//       "[...] MUST be set if the peer has received
					//        the begin frame for the session"
					s.txFrame(&performEnd{
						Error: &Error{
							Condition:   ErrorNotAllowed,
							Description: "next-incoming-id not set after session established",
						},
					}, nil)
					s.err = errors.New("protocol error: received flow without next-incoming-id after session established")
					return
				}

				// "When the endpoint receives a flow frame from its peer,
				// it MUST update the next-incoming-id directly from the
				// next-outgoing-id of the frame, and it MUST update the
				// remote-outgoing-window directly from the outgoing-window
				// of the frame."
				nextIncomingID = body.NextOutgoingID
				remoteOutgoingWindow = body.OutgoingWindow

				// "The remote-incoming-window is computed as follows:
				//
				// next-incoming-id(flow) + incoming-window(flow) - next-outgoing-id(endpoint)
				//
				// If the next-incoming-id field of the flow frame is not set, then remote-incoming-window is computed as follows:
				//
				// initial-outgoing-id(endpoint) + incoming-window(flow) - next-outgoing-id(endpoint)"
				remoteIncomingWindow = body.IncomingWindow - nextOutgoingID
				remoteIncomingWindow += *body.NextIncomingID

				// Send to link if handle is set
				if body.Handle != nil {
					link, ok := links[*body.Handle]
					if !ok {
						continue
					}

					s.muxFrameToLink(link, fr.body)
					continue
				}

				if body.Echo {
					niID := nextIncomingID
					resp := &performFlow{
						NextIncomingID: &niID,
						IncomingWindow: s.incomingWindow,
						NextOutgoingID: nextOutgoingID,
						OutgoingWindow: s.outgoingWindow,
					}
					debug(1, "TX: %s", resp)
					s.txFrame(resp, nil)
				}

			case *performAttach:
				// On Attach response link should be looked up by name, then added
				// to the links map with the remote's handle contained in this
				// attach frame.
				link, linkOk := linksByName[body.Name]
				if !linkOk {
					break
				}
				delete(linksByName, body.Name) // name no longer needed

				link.remoteHandle = body.Handle
				links[link.remoteHandle] = link

				s.muxFrameToLink(link, fr.body)

			case *performTransfer:
				// "Upon receiving a transfer, the receiving endpoint will
				// increment the next-incoming-id to match the implicit
				// transfer-id of the incoming transfer plus one, as well
				// as decrementing the remote-outgoing-window, and MAY
				// (depending on policy) decrement its incoming-window."
				nextIncomingID++
				remoteOutgoingWindow--
				link, ok := links[body.Handle]
				if !ok {
					continue
				}

				select {
				case <-s.conn.done:
				case link.rx <- fr.body:
				}

				// Update peer's outgoing window if half has been consumed.
				if remoteOutgoingWindow < s.incomingWindow/2 {
					nID := nextIncomingID
					flow := &performFlow{
						NextIncomingID: &nID,
						IncomingWindow: s.incomingWindow,
						NextOutgoingID: nextOutgoingID,
						OutgoingWindow: s.outgoingWindow,
					}
					debug(1, "TX(Session): %s", flow)
					s.txFrame(flow, nil)
					remoteOutgoingWindow = s.incomingWindow
				}

			case *performDetach:
				link, ok := links[body.Handle]
				if !ok {
					continue
				}
				s.muxFrameToLink(link, fr.body)

			case *performEnd:
				s.txFrame(&performEnd{}, nil)
				s.err = errorErrorf("session ended by server: %s", body.Error)
				return

			default:
				fmt.Printf("Unexpected frame: %s\n", body)
			}

		case fr := <-txTransfer:

			// record current delivery ID
			var deliveryID uint32
			if fr.DeliveryID != nil {
				deliveryID = *fr.DeliveryID
				deliveryIDByHandle[fr.Handle] = deliveryID

				// add to handleByDeliveryID if not sender-settled
				if !fr.Settled {
					handlesByDeliveryID[deliveryID] = fr.Handle
				}
			} else {
				// if fr.DeliveryID is nil it must have been added
				// to deliveryIDByHandle already
				deliveryID = deliveryIDByHandle[fr.Handle]
			}

			// frame has been sender-settled, remove from map
			if fr.Settled {
				delete(handlesByDeliveryID, deliveryID)
			}

			// if confirmSettlement requested, add done chan to map
			// and clear from frame so conn doesn't close it.
			if fr.confirmSettlement && fr.done != nil {
				settlementByDeliveryID[deliveryID] = fr.done
				fr.done = nil
			}

			debug(2, "TX(Session): %s", fr)
			s.txFrame(fr, fr.done)

			// "Upon sending a transfer, the sending endpoint will increment
			// its next-outgoing-id, decrement its remote-incoming-window,
			// and MAY (depending on policy) decrement its outgoing-window."
			nextOutgoingID++
			remoteIncomingWindow--

		case fr := <-s.tx:
			switch fr := fr.(type) {
			case *performFlow:
				niID := nextIncomingID
				fr.NextIncomingID = &niID
				fr.IncomingWindow = s.incomingWindow
				fr.NextOutgoingID = nextOutgoingID
				fr.OutgoingWindow = s.outgoingWindow
				debug(1, "TX(Session): %s", fr)
				s.txFrame(fr, nil)
				remoteOutgoingWindow = s.incomingWindow
			case *performTransfer:
				panic("transfer frames must use txTransfer")
			default:
				debug(1, "TX(Session): %s", fr)
				s.txFrame(fr, nil)
			}
		}
	}
}

func (s *Session) muxFrameToLink(l *link, fr frameBody) {
	select {
	case l.rx <- fr:
	case <-l.done:
	case <-s.conn.done:
	}
}

// DetachError is returned by a link (Receiver/Sender) when a detach frame is received.
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
// May be used for sending or receiving.
type link struct {
	name          string               // our name
	handle        uint32               // our handle
	remoteHandle  uint32               // remote's handle
	dynamicAddr   bool                 // request a dynamic link address from the server
	rx            chan frameBody       // sessions sends frames for this link on this channel
	transfers     chan performTransfer // sender uses for send; receiver uses for receive
	closeOnce     sync.Once            // closeOnce protects close from being closed multiple times
	close         chan struct{}        // close signals the mux to shutdown
	done          chan struct{}        // done is closed by mux/muxDetach when the link is fully detached
	detachErrorMu sync.Mutex           // protects detachError
	detachError   *Error               // error to send to remote on detach, set by closeWithError
	session       *Session             // parent session
	receiver      *Receiver            // allows link options to modify Receiver
	source        *source
	target        *target
	properties    map[symbol]interface{} // additional properties sent upon link attach

	// "The delivery-count is initialized by the sender when a link endpoint is created,
	// and is incremented whenever a message is sent. Only the sender MAY independently
	// modify this field. The receiver's value is calculated based on the last known
	// value from the sender and any subsequent messages received on the link. Note that,
	// despite its name, the delivery-count is not a count but a sequence number
	// initialized at an arbitrary point by the sender."
	deliveryCount      uint32
	linkCredit         uint32 // maximum number of messages allowed between flow updates
	senderSettleMode   *SenderSettleMode
	receiverSettleMode *ReceiverSettleMode
	maxMessageSize     uint64
	detachReceived     bool
	err                error // err returned on Close()
}

// attachLink is used by Receiver and Sender to create new links
func attachLink(s *Session, r *Receiver, opts []LinkOption) (*link, error) {
	l, err := newLink(s, r, opts)
	if err != nil {
		return nil, err
	}

	isReceiver := r != nil

	// buffer rx to linkCredit so that conn.mux won't block
	// attempting to send to a slow reader
	if isReceiver {
		l.rx = make(chan frameBody, l.linkCredit)
	} else {
		l.rx = make(chan frameBody, 1)
	}

	// request handle from Session.mux
	select {
	case <-s.done:
		return nil, s.err
	case s.allocateHandle <- l:
	}

	// wait for handle allocation
	select {
	case <-s.done:
		return nil, s.err
	case <-l.rx:
	}

	// check for link request error
	if l.err != nil {
		return nil, l.err
	}

	attach := &performAttach{
		Name:               l.name,
		Handle:             l.handle,
		ReceiverSettleMode: l.receiverSettleMode,
		SenderSettleMode:   l.senderSettleMode,
		MaxMessageSize:     l.maxMessageSize,
		Source:             l.source,
		Target:             l.target,
		Properties:         l.properties,
	}

	if isReceiver {
		attach.Role = roleReceiver
		if attach.Source == nil {
			attach.Source = new(source)
		}
		attach.Source.Dynamic = l.dynamicAddr
	} else {
		attach.Role = roleSender
		if attach.Target == nil {
			attach.Target = new(target)
		}
		attach.Target.Dynamic = l.dynamicAddr
	}

	// send Attach frame
	debug(1, "TX: %s", attach)
	s.txFrame(attach, nil)

	// wait for response
	var fr frameBody
	select {
	case <-s.done:
		return nil, s.err
	case fr = <-l.rx:
	}
	debug(3, "RX: %s", fr)
	resp, ok := fr.(*performAttach)
	if !ok {
		return nil, errorErrorf("unexpected attach response: %#v", fr)
	}

	if l.maxMessageSize == 0 || resp.MaxMessageSize < l.maxMessageSize {
		l.maxMessageSize = resp.MaxMessageSize
	}

	if isReceiver {
		// if dynamic address requested, copy assigned name to address
		if l.dynamicAddr && resp.Source != nil {
			l.source.Address = resp.Source.Address
		}
		// deliveryCount is a sequence number, must initialize to sender's initial sequence number
		l.deliveryCount = resp.InitialDeliveryCount
		// buffer receiver so that link.mux doesn't block
		l.transfers = make(chan performTransfer, l.receiver.maxCredit)
		if resp.SenderSettleMode != nil {
			l.senderSettleMode = resp.SenderSettleMode
		}
	} else {
		// if dynamic address requested, copy assigned name to address
		if l.dynamicAddr && resp.Target != nil {
			l.target.Address = resp.Target.Address
		}
		l.transfers = make(chan performTransfer)
		if resp.ReceiverSettleMode != nil {
			l.receiverSettleMode = resp.ReceiverSettleMode
		}
	}

	go l.mux()

	return l, nil
}

func newLink(s *Session, r *Receiver, opts []LinkOption) (*link, error) {
	l := &link{
		name:     randString(40),
		session:  s,
		receiver: r,
		close:    make(chan struct{}),
		done:     make(chan struct{}),
	}

	// configure options
	for _, o := range opts {
		err := o(l)
		if err != nil {
			return nil, err
		}
	}

	return l, nil
}

func (l *link) mux() {
	defer l.muxDetach()

	var (
		isReceiver = l.receiver != nil
		isSender   = !isReceiver
	)

Loop:
	for {
		var outgoingTransfers chan performTransfer
		switch {
		// enable outgoing transfers case if sender and credits are available
		case isSender && l.linkCredit > 0:
			outgoingTransfers = l.transfers

		// if receiver and linkCredit is half used, send more
		case isReceiver && l.linkCredit <= l.receiver.maxCredit/2:
			l.err = l.muxFlow()
			if l.err != nil {
				return
			}
		}

		select {
		// received frame
		case fr := <-l.rx:
			l.err = l.muxHandleFrame(fr)
			if l.err != nil {
				return
			}

		// send data
		case tr := <-outgoingTransfers:
			debug(3, "TX(link): %s", tr)

			// Ensure the session mux is not blocked
			for {
				select {
				case l.session.txTransfer <- &tr:
					// decrement link-credit after entire message transferred
					if !tr.More {
						l.deliveryCount++
						l.linkCredit--
					}
					continue Loop
				case fr := <-l.rx:
					l.err = l.muxHandleFrame(fr)
					if l.err != nil {
						return
					}
				case <-l.close:
					l.err = ErrLinkClosed
					return
				case <-l.session.done:
					l.err = l.session.err
					return
				}
			}

		case <-l.close:
			l.err = ErrLinkClosed
			return
		case <-l.session.done:
			l.err = l.session.err
			return
		}
	}
}

// muxFlow sends tr to the session mux.
func (l *link) muxFlow() error {
	var (
		// copy because sent by pointer below; prevent race
		linkCredit    = l.receiver.maxCredit
		deliveryCount = l.deliveryCount
	)

	fr := &performFlow{
		Handle:        &l.handle,
		DeliveryCount: &deliveryCount,
		LinkCredit:    &linkCredit, // max number of messages
	}
	debug(3, "TX: %s", fr)

	// Ensure the session mux is not blocked
	for {
		select {
		case l.session.tx <- fr:
			// reset credit
			l.linkCredit = l.receiver.maxCredit
			return nil
		case fr := <-l.rx:
			err := l.muxHandleFrame(fr)
			if err != nil {
				return err
			}
		case <-l.close:
			return ErrLinkClosed
		case <-l.session.done:
			return l.session.err
		}
	}
}

// muxHandleFrame processes fr based on type.
func (l *link) muxHandleFrame(fr frameBody) error {
	var (
		isSender               = l.receiver == nil
		errOnRejectDisposition = isSender && (l.receiverSettleMode == nil || *l.receiverSettleMode == ModeFirst)
	)

	switch fr := fr.(type) {
	// message frame
	case *performTransfer:
		debug(3, "RX: %s", fr)
		if isSender {
			// Senders should never receive transfer frames, but handle it just in case.
			l.closeWithError(&Error{
				Condition:   ErrorNotAllowed,
				Description: "sender cannot process transfer frame",
			})
			return errorErrorf("sender received transfer frame")
		}

		l.transfers <- *fr

		// decrement link-credit after entire message received
		if !fr.More {
			l.deliveryCount++
			l.linkCredit--
		}

	// flow control frame
	case *performFlow:
		debug(3, "RX: %s", fr)
		if isSender {
			l.linkCredit = *fr.LinkCredit - l.deliveryCount
			if fr.DeliveryCount != nil {
				// DeliveryCount can be nil if the receiver hasn't processed
				// the attach. That shouldn't be the case here, but it's
				// what ActiveMQ does.
				l.linkCredit += *fr.DeliveryCount
			}
		} else if fr.DeliveryCount != nil {
			l.deliveryCount = *fr.DeliveryCount
		}

		if !fr.Echo {
			return nil
		}

		var (
			// copy because sent by pointer below; prevent race
			linkCredit    = l.linkCredit
			deliveryCount = l.deliveryCount
		)

		// send flow
		resp := &performFlow{
			Handle:        &l.handle,
			DeliveryCount: &deliveryCount,
			LinkCredit:    &linkCredit, // max number of messages
		}
		debug(1, "TX: %s", resp)
		l.session.txFrame(resp, nil)

	// remote side is closing links
	case *performDetach:
		debug(1, "RX: %s", fr)
		// don't currently support link detach and reattach
		if !fr.Closed {
			return errorErrorf("non-closing detach not supported: %+v", fr)
		}

		// set detach received and close link
		l.detachReceived = true

		return errorWrapf(DetachError{fr.Error}, "received detach frame")

	case *performDisposition:
		debug(3, "RX: %s", fr)

		// If sending async and a message is rejected, cause a link error.
		//
		// This isn't ideal, but there isn't a clear better way to handle it.
		if fr, ok := fr.State.(*stateRejected); ok && errOnRejectDisposition {
			return fr.Error
		}

		if fr.Settled {
			return nil
		}

		resp := &performDisposition{
			Role:    roleSender,
			First:   fr.First,
			Last:    fr.Last,
			Settled: true,
		}
		debug(1, "TX: %s", resp)
		l.session.txFrame(resp, nil)

	default:
		debug(1, "RX: %s", fr)
		fmt.Printf("Unexpected frame: %s\n", fr)
	}

	return nil
}

// close closes and requests deletion of the link.
//
// No operations on link are valid after close.
//
// If ctx expires while waiting for servers response, ctx.Err() will be returned.
// The session will continue to wait for the response until the Session or Client
// is closed.
func (l *link) Close(ctx context.Context) error {
	l.closeOnce.Do(func() { close(l.close) })
	select {
	case <-l.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if l.err == ErrLinkClosed {
		return nil
	}
	return l.err
}

func (l *link) closeWithError(de *Error) {
	l.closeOnce.Do(func() {
		l.detachErrorMu.Lock()
		l.detachError = de
		l.detachErrorMu.Unlock()
		close(l.close)
	})
}

func (l *link) muxDetach() {
	defer func() {
		// final cleanup and signaling

		// deallocate handle
		select {
		case l.session.deallocateHandle <- l:
		case <-l.session.done:
			if l.err == nil {
				l.err = l.session.err
			}
		}

		// signal other goroutines that links is done
		close(l.done)
	}()

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

	l.detachErrorMu.Lock()
	detachError := l.detachError
	l.detachErrorMu.Unlock()

	fr := &performDetach{
		Handle: l.handle,
		Closed: true,
		Error:  detachError,
	}
	select {
	case l.session.tx <- fr:
	case <-l.session.done:
		if l.err == nil {
			l.err = l.session.err
		}
		return
	}

	// don't wait for remote to detach when already
	// received or closing due to error
	if l.detachReceived || detachError != nil {
		return
	}

	for {
		select {
		// read from link until detach with Close == true is received,
		// other frames are discarded.
		case fr := <-l.rx:
			if fr, ok := fr.(*performDetach); ok && fr.Closed {
				return
			}

		// connection has ended
		case <-l.session.done:
			if l.err == nil {
				l.err = l.session.err
			}
			return
		}
	}
}

// LinkOption is a function for configuring an AMQP link.
//
// A link may be a Sender or a Receiver.
type LinkOption func(*link) error

// LinkAddress sets the link address.
//
// For a Receiver this configures the source address.
// For a Sender this configures the target address.
//
// Deprecated: use LinkSourceAddress or LinkTargetAddress instead.
func LinkAddress(source string) LinkOption {
	return func(l *link) error {
		if l.receiver != nil {
			return LinkSourceAddress(source)(l)
		}
		return LinkTargetAddress(source)(l)
	}
}

// LinkProperty sets an entry in the link properties map sent to the server.
//
// This option can be used multiple times.
func LinkProperty(key, value string) LinkOption {
	return linkProperty(key, value)
}

// LinkPropertyInt64 sets an entry in the link properties map sent to the server.
//
// This option can be used multiple times.
func LinkPropertyInt64(key string, value int64) LinkOption {
	return linkProperty(key, value)
}

func linkProperty(key string, value interface{}) LinkOption {
	return func(l *link) error {
		if key == "" {
			return errorNew("link property key must not be empty")
		}
		if l.properties == nil {
			l.properties = make(map[symbol]interface{})
		}
		l.properties[symbol(key)] = value
		return nil
	}
}

// LinkSourceAddress sets the source address.
func LinkSourceAddress(addr string) LinkOption {
	return func(l *link) error {
		if l.source == nil {
			l.source = new(source)
		}
		l.source.Address = addr
		return nil
	}
}

// LinkTargetAddress sets the target address.
func LinkTargetAddress(addr string) LinkOption {
	return func(l *link) error {
		if l.target == nil {
			l.target = new(target)
		}
		l.target.Address = addr
		return nil
	}
}

// LinkAddressDynamic requests a dynamically created address from the server.
func LinkAddressDynamic() LinkOption {
	return func(l *link) error {
		l.dynamicAddr = true
		return nil
	}
}

// LinkCredit specifies the maximum number of unacknowledged messages
// the sender can transmit.
func LinkCredit(credit uint32) LinkOption {
	return func(l *link) error {
		if l.receiver == nil {
			return errorNew("LinkCredit is not valid for Sender")
		}

		l.receiver.maxCredit = credit
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

// LinkSenderSettle sets the sender settlement mode.
//
// When the Link is the Receiver, this is a request to the remote
// server.
//
// When the Link is the Sender, this is the actual settlement mode.
func LinkSenderSettle(mode SenderSettleMode) LinkOption {
	return func(l *link) error {
		if mode > ModeMixed {
			return errorErrorf("invalid SenderSettlementMode %d", mode)
		}
		l.senderSettleMode = &mode
		return nil
	}
}

// LinkReceiverSettle sets the receiver settlement mode.
//
// When the Link is the Sender, this is a request to the remote
// server.
//
// When the Link is the Receiver, this is the actual settlement mode.
func LinkReceiverSettle(mode ReceiverSettleMode) LinkOption {
	return func(l *link) error {
		if mode > ModeSecond {
			return errorErrorf("invalid ReceiverSettlementMode %d", mode)
		}
		l.receiverSettleMode = &mode
		return nil
	}
}

// LinkSessionFilter sets a session filter (com.microsoft:session-filter) on the link source.
// This is used in Azure Service Bus to filter messages by session ID on a receiving link.
func LinkSessionFilter(sessionID string) LinkOption {
	// <descriptor name="com.microsoft:session-filter" code="00000013:7000000C"/>
	return linkSourceFilter("com.microsoft:session-filter", uint64(0x00000137000000C), sessionID)
}

// LinkSelectorFilter sets a selector filter (apache.org:selector-filter:string) on the link source.
func LinkSelectorFilter(filter string) LinkOption {
	// <descriptor name="apache.org:selector-filter:string" code="0x0000468C:0x00000004"/>
	return linkSourceFilter("apache.org:selector-filter:string", uint64(0x0000468C00000004), filter)
}

// linkSourceFilter sets a filter on the link source.
func linkSourceFilter(name string, code uint64, value string) LinkOption {
	nameSym := symbol(name)
	return func(l *link) error {
		if l.source == nil {
			l.source = new(source)
		}
		if l.source.Filter == nil {
			l.source.Filter = make(map[symbol]*describedType)
		}
		l.source.Filter[nameSym] = &describedType{
			descriptor: code,
			value:      value,
		}
		return nil
	}
}

// LinkMaxMessageSize sets the maximum message size that can
// be sent or received on the link.
//
// A size of zero indicates no limit.
//
// Default: 0.
func LinkMaxMessageSize(size uint64) LinkOption {
	return func(l *link) error {
		l.maxMessageSize = size
		return nil
	}
}

// Receiver receives messages on a single AMQP link.
type Receiver struct {
	link         *link                   // underlying link
	buf          buffer                  // reusable buffer for decoding multi frame messages
	batching     bool                    // enable batching of message dispositions
	batchMaxAge  time.Duration           // maximum time between the start n batch and sending the batch to the server
	dispositions chan messageDisposition // message dispositions are sent on this channel when batching is enabled
	maxCredit    uint32                  // maximum allowed inflight messages
}

// Receive returns the next message from the sender.
//
// Blocks until a message is received, ctx completes, or an error occurs.
func (r *Receiver) Receive(ctx context.Context) (*Message, error) {
	r.buf.reset()

	msg := Message{receiver: r} // message to be decoded into

	var (
		maxMessageSize        = r.link.maxMessageSize
		messageSize    uint64 = 0
		first                 = true // receiving the first frame of the message
	)

	for {
		// wait for the next frame
		var fr performTransfer
		select {
		case fr = <-r.link.transfers:
		case <-r.link.done:
			return nil, r.link.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		// record the delivery ID and message format if this is
		// the first frame of the message
		if first {
			if fr.DeliveryID != nil {
				msg.id = (deliveryID)(*fr.DeliveryID)
			}

			if fr.MessageFormat != nil {
				msg.Format = *fr.MessageFormat
			}

			first = false
		}

		// ensure maxMessageSize will not be exceeded
		messageSize += uint64(len(fr.Payload))
		if maxMessageSize != 0 && messageSize > maxMessageSize {
			msg := fmt.Sprintf("received message larger than max size of %d", maxMessageSize)
			r.link.closeWithError(&Error{
				Condition:   ErrorMessageSizeExceeded,
				Description: msg,
			})
			return nil, errorNew(msg)
		}

		// add the payload the the buffer
		r.buf.write(fr.Payload)

		// mark as settled if at least one frame is settled
		msg.settled = msg.settled || fr.Settled

		// break out of loop if message is complete
		if !fr.More {
			break
		}
	}

	// TODO:
	// When rcv-settle-mode == second, don't consider the transfer complete
	// until caller accepts/reject/etc and a confirm from sender.
	//
	// At first glance, this appears to be at odds with batching. A batch can't
	// be built up if the caller is blocked on confirmation. However, while receives
	// must happen synchronously, confirmations do not. While the use is probably
	// limited it may be worth exploring.

	// unmarshal message
	err := msg.unmarshal(&r.buf)
	return &msg, err
}

// Address returns the link's address.
func (r *Receiver) Address() string {
	if r.link.source == nil {
		return ""
	}
	return r.link.source.Address
}

// Close closes the Receiver and AMQP link.
//
// If ctx expires while waiting for servers response, ctx.Err() will be returned.
// The session will continue to wait for the response until the Session or Client
// is closed.
func (r *Receiver) Close(ctx context.Context) error {
	return r.link.Close(ctx)
}

type messageDisposition struct {
	id    deliveryID
	state interface{}
}

type deliveryID uint32

func (r *Receiver) dispositionBatcher() {
	// batch operations:
	// Keep track of the first and last delivery ID, incrementing as
	// Accept() is called. After last-first == linkCredit, send disposition.
	// If Reject()/Release() is called, send one disposition for previously
	// accepted, and one for the rejected/released message. If messages are
	// accepted out of order, send any existing batch and the current message.
	var (
		batchSize    = r.maxCredit
		batchStarted bool
		first        deliveryID
		last         deliveryID
	)

	// create an unstarted timer
	batchTimer := time.NewTimer(1 * time.Minute)
	batchTimer.Stop()
	defer batchTimer.Stop()

	for {
		select {
		case msgDis := <-r.dispositions:

			// not accepted or batch out of order
			_, isAccept := msgDis.state.(*stateAccepted)
			if !isAccept || (batchStarted && last+1 != msgDis.id) {
				// send the current batch, if any
				if batchStarted {
					lastCopy := last
					r.sendDisposition(first, &lastCopy, &stateAccepted{})
					batchStarted = false
				}

				// send the current message
				r.sendDisposition(msgDis.id, nil, msgDis.state)
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
				lastCopy := last
				r.sendDisposition(first, &lastCopy, &stateAccepted{})
				batchStarted = false
				if !batchTimer.Stop() {
					<-batchTimer.C // batch timer must be drained if stop returns false
				}
			}

		// maxBatchAge elapsed, send batch
		case <-batchTimer.C:
			lastCopy := last
			r.sendDisposition(first, &lastCopy, &stateAccepted{})
			batchStarted = false
			batchTimer.Stop()

		case <-r.link.done:
			return
		}
	}
}

// sendDisposition sends a disposition frame to the peer
func (r *Receiver) sendDisposition(first deliveryID, last *deliveryID, state interface{}) {
	fr := &performDisposition{
		Role:    roleReceiver,
		First:   uint32(first),
		Last:    (*uint32)(last),
		Settled: r.link.receiverSettleMode == nil || *r.link.receiverSettleMode == ModeFirst,
		State:   state,
	}

	debug(1, "TX: %s", fr)
	r.link.session.txFrame(fr, nil)
}

func (r *Receiver) messageDisposition(id deliveryID, state interface{}) {
	if r.batching {
		r.dispositions <- messageDisposition{id: id, state: state}
		return
	}
	r.sendDisposition(id, nil, state)
}

const maxTransferFrameHeader = 66 // determined by calcMaxTransferFrameHeader

func calcMaxTransferFrameHeader() int {
	var buf buffer

	maxUint32 := uint32(math.MaxUint32)
	receiverSettleMode := ReceiverSettleMode(0)
	err := writeFrame(&buf, frame{
		type_:   frameTypeAMQP,
		channel: math.MaxUint16,
		body: &performTransfer{
			Handle:             maxUint32,
			DeliveryID:         &maxUint32,
			DeliveryTag:        bytes.Repeat([]byte{'a'}, 32),
			MessageFormat:      &maxUint32,
			Settled:            true,
			More:               true,
			ReceiverSettleMode: &receiverSettleMode,
			State:              nil, // TODO: determine whether state should be included in size
			Resume:             true,
			Aborted:            true,
			Batchable:          true,
			// Payload omitted as it is appended directly without any header
		},
	})
	if err != nil {
		panic(err)
	}

	return buf.len()
}
