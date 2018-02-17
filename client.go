package amqp

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/url"
	"sync"
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

// maxSliceLen is equal to math.MaxInt32 or math.MaxInt64, depending on platform
const maxSliceLen = uint64(^uint(0) >> 1)

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
func (c *Client) NewSession() (*Session, error) {
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

	// send Begin to server
	begin := &performBegin{
		NextOutgoingID: 0,
		IncomingWindow: s.incomingWindow,
		OutgoingWindow: s.outgoingWindow,
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
		_ = s.Close() // deallocate session on error
		return nil, errorErrorf("unexpected begin response: %+v", fr.body)
	}

	// start Session multiplexor
	go s.mux(begin)

	return s, nil
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

	allocateHandle   chan *link // link handles are allocated by sending a link on this channel, nil is sent on link.rx once allocated
	deallocateHandle chan *link // link handles are deallocated by sending a link on this channel

	// used for gracefully closing link
	close     chan struct{}
	closeOnce sync.Once
	done      chan struct{}
	err       error
}

func newSession(c *conn, channel uint16) *Session {
	return &Session{
		conn:       c,
		channel:    channel,
		rx:         make(chan frame),
		tx:         make(chan frameBody),
		txTransfer: make(chan *performTransfer),
		// TODO: make windows configurable
		incomingWindow:   5000,
		outgoingWindow:   math.MaxUint32,
		allocateHandle:   make(chan *link),
		deallocateHandle: make(chan *link),
		close:            make(chan struct{}),
		done:             make(chan struct{}),
	}
}

// Close closes the session.
func (s *Session) Close() error {
	s.closeOnce.Do(func() { close(s.close) })
	<-s.done
	if s.err == ErrSessionClosed {
		return nil
	}
	return s.err
}

// txFrame sends a frame to the connWriter
func (s *Session) txFrame(p frameBody, done chan struct{}) {
	s.conn.wantWriteFrame(frame{
		type_:   frameTypeAMQP,
		channel: s.channel,
		body:    p,
		done:    done,
	})
}

func randBytes(n int) []byte { // TODO: random string gen off SO, replace
	var letterBytes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return b
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
	link            *link
	buf             buffer
	nextDeliveryTag uint64
}

// Send sends a Message.
//
// Blocks until the message is sent, ctx completes, or an error occurs.
func (s *Sender) Send(ctx context.Context, msg *Message) error {
	s.buf.reset()
	err := msg.marshal(&s.buf)
	if err != nil {
		return err
	}

	if uint64(s.buf.len()) > s.link.peerMaxMessageSize {
		return errorErrorf("encoded message size exceeds peer max of %d", s.link.peerMaxMessageSize)
	}

	var (
		maxPayloadSize = int(s.link.session.conn.peerMaxFrameSize) - maxTransferFrameHeader
		sndSettleMode  = s.link.senderSettleMode
		rcvSettleMode  = s.link.receiverSettleMode
	)

	// use uint64 encoded as []byte as deliveryTag
	deliveryTag := make([]byte, 8)
	binary.BigEndian.PutUint64(deliveryTag, s.nextDeliveryTag)
	s.nextDeliveryTag++

	fr := performTransfer{
		Handle:        s.link.handle,
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
			fr.Settled = sndSettleMode != nil && *sndSettleMode == ModeSettled

			// set done on last frame to be closed after network transmission
			//
			// If confirmSettlement is true (ReceiverSettleMode == "second"),
			// Session.mux will intercept the done channel and close it when the
			// receiver has confirmed settlement instead of on net transmit.
			fr.done = make(chan struct{})
			fr.confirmSettlement = rcvSettleMode != nil && *rcvSettleMode == ModeSecond
		}

		select {
		case s.link.transfers <- fr:
		case <-s.link.done:
			return s.link.err
		case <-ctx.Done():
			return errorWrapf(ctx.Err(), "awaiting send")
		}

		// clear values that are only required on first message
		fr.DeliveryTag = nil
		fr.MessageFormat = nil
	}

	// TODO: The blocking below could slow single link sending. This could be
	//       alleviated by making Send() concurrency-safe (lock the section above).

	// wait for transfer to be confirmed
	select {
	case <-fr.done:
	case <-s.link.done:
		return s.link.err
	case <-ctx.Done():
		return errorWrapf(ctx.Err(), "awaiting send")
	}

	return nil
}

// Address returns the link's address.
func (s *Sender) Address() string {
	if s.link.target == nil {
		return ""
	}
	return s.link.target.Address
}

// Close closes the Sender and AMQP link.
func (s *Sender) Close() error {
	// TODO: Should this timeout? Close() take a context? Use one of the
	// other timeouts?
	return s.link.Close()
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
		links       = make(map[uint32]*link) // mapping of remote handles to links
		linksByName = make(map[string]*link) // maping of names to links
		nextHandle  uint32                   // next handle # to be allocated

		handlesByDeliveryID = make(map[uint32]uint32) //mapping of deliveryIDs to handles
		nextDeliveryID      uint32                    // next deliveryID

		settlementByDeliveryID = make(map[uint32]chan struct{})

		// flow control values
		nextOutgoingID       uint32
		nextIncomingID       = remoteBegin.NextOutgoingID
		remoteIncomingWindow = remoteBegin.IncomingWindow
		remoteOutgoingWindow = remoteBegin.OutgoingWindow
	)

	updateFlowControl := func(flow *performFlow) {
		// "When the endpoint receives a flow frame from its peer,
		// it MUST update the next-incoming-id directly from the
		// next-outgoing-id of the frame, and it MUST update the
		// remote-outgoing-window directly from the outgoing-window
		// of the frame."
		nextIncomingID = flow.NextOutgoingID
		remoteOutgoingWindow = flow.OutgoingWindow

		// "The remote-incoming-window is computed as follows:
		//
		// next-incoming-id(flow) + incoming-window(flow) - next-outgoing-id(endpoint)
		//
		// If the next-incoming-id field of the flow frame is not set, then remote-incoming-window is computed as follows:
		//
		// initial-outgoing-id(endpoint) + incoming-window(flow) - next-outgoing-id(endpoint)"
		remoteIncomingWindow = flow.IncomingWindow - nextOutgoingID
		if flow.NextIncomingID != nil {
			remoteIncomingWindow += *flow.NextIncomingID
		} else {
			// TODO: This is a protocol error:
			//       "[...] MUST be set if the peer has received
			//        the begin frame for the session"
		}
	}

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
			// TODO: handle max session/wrapping
			l.handle = nextHandle   // allocate handle to the link
			linksByName[l.name] = l // add to mapping
			nextHandle++            // increment the next handle
			l.rx <- nil             // send nil on channel to indicate allocation complete

		// handle deallocation request
		case l := <-s.deallocateHandle:
			delete(links, l.remoteHandle)
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
							close(done)
							delete(settlementByDeliveryID, deliveryID)
						}
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
				continue
			case *performFlow:
				updateFlowControl(body)

				if body.Handle != nil {
					link, ok := links[*body.Handle]
					if !ok {
						continue
					}

					select {
					case <-s.conn.done:
					case link.rx <- fr.body:
					}
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

				select {
				case <-s.conn.done:
				case link.rx <- fr.body:
				}

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

			case *performDetach:
				link, ok := links[body.Handle]
				if !ok {
					continue
				}

				select {
				case <-s.conn.done:
				case link.rx <- fr.body:
				}

			case *performEnd:
				s.txFrame(&performEnd{}, nil)
				s.err = errorErrorf("session ended by server: %s", body.Error)
				return

			default:
				fmt.Printf("Unexpected frame: %s\n", body)
			}

		case fr := <-txTransfer:
			// nil DeliveryID indicates first message
			if fr.DeliveryID == nil {
				// allocate new DeliveryID
				id := nextDeliveryID
				nextDeliveryID++
				fr.DeliveryID = &id

				// add to handleByDeliveryID if not sender-settled
				if !fr.Settled {
					handlesByDeliveryID[id] = fr.Handle
				}
			}

			// frame has been sender-settled, remove from map
			if fr.Settled {
				delete(handlesByDeliveryID, *fr.DeliveryID)
			}

			// if confirmSettlement requested, add done chan to map
			// and clear from frame so conn doesn't close it.
			if fr.confirmSettlement && fr.done != nil {
				settlementByDeliveryID[*fr.DeliveryID] = fr.done
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
			case *performTransfer:
				panic("transfer frames must use txTransfer")
			default:
				debug(1, "TX(Session): %s", fr)
				s.txFrame(fr, nil)
			}
		}
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
	name         string               // our name
	handle       uint32               // our handle
	remoteHandle uint32               // remote's handle
	dynamicAddr  bool                 // request a dynamic link address from the server
	rx           chan frameBody       // sessions sends frames for this link on this channel
	transfers    chan performTransfer // sender uses for send; receiver uses for receive
	close        chan struct{}
	closeOnce    sync.Once
	done         chan struct{}
	doneOnce     sync.Once
	session      *Session  // parent session
	receiver     *Receiver // allows link options to modify Receiver
	source       *source
	target       *target

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
	peerMaxMessageSize uint64
	detachSent         bool // detach frame has been sent
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
	case <-s.conn.done:
		return nil, s.conn.getErr()
	case s.allocateHandle <- l:
	}

	// wait for handle allocation
	select {
	case <-s.conn.done:
		return nil, s.conn.getErr()
	case <-l.rx:
	}

	attach := &performAttach{
		Name:               l.name,
		Handle:             l.handle,
		ReceiverSettleMode: l.receiverSettleMode,
		SenderSettleMode:   l.senderSettleMode,
		MaxMessageSize:     l.maxMessageSize,
		Source:             l.source,
		Target:             l.target,
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
	case <-s.conn.done:
		return nil, s.conn.getErr()
	case fr = <-l.rx:
	}
	debug(3, "RX: %s", fr)
	resp, ok := fr.(*performAttach)
	if !ok {
		return nil, errorErrorf("unexpected attach response: %#v", fr)
	}

	// TODO: this is excessive, especially on 64-bit platforms
	//       default to a more reasonable max and allow users to
	//       change via LinkOption
	l.peerMaxMessageSize = maxSliceLen
	if resp.MaxMessageSize != 0 && resp.MaxMessageSize < l.peerMaxMessageSize {
		l.peerMaxMessageSize = resp.MaxMessageSize
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
		name:     string(randBytes(40)),
		session:  s,
		receiver: r,
		close:    make(chan struct{}),
		done:     make(chan struct{}),
		// TODO: this is excessive, especially on 64-bit platforms
		//       default to a more reasonable max and allow users to
		//       change via LinkOption
		maxMessageSize: maxSliceLen,
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
	defer l.detach()

	var (
		isReceiver = l.receiver != nil
		isSender   = !isReceiver
	)

	handleRx := func(fr frameBody) bool {
		switch fr := fr.(type) {
		// message frame
		case *performTransfer:
			debug(3, "RX: %s", fr)
			if isSender {
				// TODO: send error to remote
				l.err = errorErrorf("Sender received transfer frame")
				return false
			}

			l.transfers <- *fr

			l.deliveryCount++
			l.linkCredit--

		// flow control frame
		case *performFlow:
			debug(3, "RX: %s", fr)
			if isReceiver {
				if fr.DeliveryCount != nil {
					l.deliveryCount = *fr.DeliveryCount
				}
			} else {
				l.linkCredit = *fr.LinkCredit - l.deliveryCount
				if fr.DeliveryCount != nil {
					// DeliveryCount can be nil if the receiver hasn't processed
					// the attach. That shouldn't be the case here, but it's
					// what ActiveMQ does.
					l.linkCredit += *fr.DeliveryCount
				}
			}

			if fr.Echo {
				var (
					// copy because sent by pointer below; prevent race
					linkCredit    = l.linkCredit
					deliveryCount = l.deliveryCount
				)

				// send flow
				fr := &performFlow{
					Handle:        &l.handle,
					DeliveryCount: &deliveryCount,
					LinkCredit:    &linkCredit, // max number of messages
				}
				debug(1, "TX: %s", fr)
				l.session.txFrame(fr, nil)
			}

		// remote side is closing links
		case *performDetach:
			debug(1, "RX: %s", fr)
			// don't currently support link detach and reattach
			if !fr.Closed {
				l.err = errorErrorf("non-closing detach not supported: %+v", fr)
				return false
			}

			// set detach received and close link
			l.detachReceived = true

			l.err = errorWrapf(DetachError{fr.Error}, "received detach frame")
			return false

		case *performDisposition:
			debug(3, "RX: %s", fr)
			if fr.Settled {
				return true
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
		return true
	}

	for {
		var outgoingTransfers chan performTransfer
		switch {
		// enable outgoing transfers case if sender and credits are available
		case isSender && l.linkCredit > 0:
			outgoingTransfers = l.transfers

		// if receiver and linkCredit is half used, send more
		case isReceiver && l.linkCredit <= l.receiver.maxCredit/2:
			var (
				// copy because sent by pointer below; prevent race
				linkCredit    = l.receiver.maxCredit
				deliveryCount = l.deliveryCount
			)

			// send flow
			fr := &performFlow{
				Handle:        &l.handle,
				DeliveryCount: &deliveryCount,
				LinkCredit:    &linkCredit, // max number of messages
			}
			debug(3, "TX: %s", fr)
		FlowLoop:
			for {
				// Ensure we never block the session mux
				select {
				case l.session.tx <- fr:
					break FlowLoop
				case fr := <-l.rx:
					if !handleRx(fr) {
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

			// reset credit
			l.linkCredit = l.receiver.maxCredit
		}

		// TODO: Look into avoiding the select statement duplication.

		select {
		// send data
		case tr := <-outgoingTransfers:
			debug(3, "TX(link): %s", tr)
		Loop:
			for {
				// Ensure we never block the session mux
				select {
				case l.session.txTransfer <- &tr:
					break Loop
				case fr := <-l.rx:
					if !handleRx(fr) {
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
			l.deliveryCount++
			l.linkCredit--

		// received frame
		case fr := <-l.rx:
			if !handleRx(fr) {
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
}

// close closes and requests deletion of the link.
//
// No operations on link are valid after close.
func (l *link) Close() error {
	l.closeOnce.Do(func() { close(l.close) })
	<-l.done
	if l.err == ErrLinkClosed {
		return nil
	}
	return l.err
}

func (l *link) detach() {
	defer l.doneOnce.Do(func() { close(l.done) })
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

	fr := &performDetach{
		Handle: l.handle,
		Closed: true,
	}
	select {
	case l.session.tx <- fr:
	case <-l.session.done:
		if l.err == nil {
			l.err = l.session.err
		}
		return
	}
	l.detachSent = true

	// already received detach from remote
	if l.detachReceived {
		select {
		case l.session.deallocateHandle <- l:
		case <-l.session.done:
			if l.err == nil {
				l.err = l.session.err
			}
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
		case <-l.session.done:
			if l.err == nil {
				l.err = l.session.err
			}
			return
		}
	}

	// deallocate handle
	select {
	case l.session.deallocateHandle <- l:
	case <-l.session.done:
		if l.err == nil {
			l.err = l.session.err
		}
		return
	}
}

// LinkOption is an function for configuring an AMQP links.
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

// LinkSelectorFilter sets a selector filter (apache.org:selector-filter:string) on the link source.
func LinkSelectorFilter(filter string) LinkOption {
	// <descriptor name="apache.org:selector-filter:string" code="0x0000468C:0x00000004"/>
	const (
		name = symbol("apache.org:selector-filter:string")
		code = uint64(0x0000468C00000004)
	)

	return func(l *link) error {
		if l.source == nil {
			l.source = new(source)
		}
		if l.source.Filter == nil {
			l.source.Filter = make(map[symbol]*describedType)
		}
		l.source.Filter[name] = &describedType{
			descriptor: code,
			value:      filter,
		}
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
		maxMessageSize = int(r.link.maxMessageSize)
		messageSize    = 0
		first          = true // receiving the first frame of the message
	)
	if maxMessageSize == 0 {
		maxMessageSize = int(maxSliceLen)
	}

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
		messageSize += len(fr.Payload)
		if messageSize > maxMessageSize {
			// TODO: send error
			_ = r.Close()
			return nil, errorErrorf("received message larger than max size of ")
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
func (r *Receiver) Close() error {
	// TODO: Should this timeout? Close() take a context? Use one of the
	// other timeouts?
	return r.link.Close()
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
			if msgDis.disposition != dispositionAccept || (batchStarted && last+1 != msgDis.id) {
				// send the current batch, if any
				if batchStarted {
					lastCopy := last
					r.sendDisposition(first, &lastCopy, dispositionAccept)
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
				lastCopy := last
				r.sendDisposition(first, &lastCopy, dispositionAccept)
				batchStarted = false
				if !batchTimer.Stop() {
					<-batchTimer.C // batch timer must be drained if stop returns false
				}
			}

		// maxBatchAge elapsed, send batch
		case <-batchTimer.C:
			lastCopy := last
			r.sendDisposition(first, &lastCopy, dispositionAccept)
			batchStarted = false
			batchTimer.Stop()

		case <-r.link.done:
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
		Settled: r.link.receiverSettleMode == nil || *r.link.receiverSettleMode == ModeFirst,
	}

	switch disp {
	case dispositionAccept:
		fr.State = new(stateAccepted)
	case dispositionReject:
		fr.State = new(stateRejected)
	case dispositionRelease:
		fr.State = new(stateReleased)
	}

	debug(1, "TX: %s", fr)
	r.link.session.txFrame(fr, nil)
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
