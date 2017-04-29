package amqp

import (
	"bytes"
	"crypto/tls"
	"math"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// connection defaults
const (
	defaultMaxFrameSize = 512
	defaultChannelMax   = 1
	defaultIdleTimeout  = 1 * time.Minute
)

// Errors
var (
	ErrTimeout = errorNew("timeout waiting for response")
)

// ConnOption is an function for configuring an AMQP connection.
type ConnOption func(*Conn) error

// ConnHostname sets the hostname of the server sent in the AMQP
// Open frame and TLS ServerName (if not otherwise set).
//
// This is useful when the AMQP connection will be established
// via a pre-established TLS connection as the server may not
// know what hostname the client is attempting to connect to.
func ConnHostname(hostname string) ConnOption {
	return func(c *Conn) error {
		c.hostname = hostname
		return nil
	}
}

// ConnTLS toggles TLS negotiation.
func ConnTLS(enable bool) ConnOption {
	return func(c *Conn) error {
		c.tlsNegotiation = enable
		return nil
	}
}

// ConnTLSConfig sets the tls.Config to be used during
// TLS negotiation.
//
// This option is for advanced usage, in most scenarios
// providing a URL scheme of "amqps://" or ConnTLS(true)
// is sufficient.
func ConnTLSConfig(tc *tls.Config) ConnOption {
	return func(c *Conn) error {
		c.tlsConfig = tc
		c.tlsNegotiation = true
		return nil
	}
}

// ConnIdleTimeout specifies the maximum period between receiving
// frames from the peer.
//
// Resolution is milliseconds. A value of zero indicates no timeout.
// This setting is in addition to TCP keepalives.
func ConnIdleTimeout(d time.Duration) ConnOption {
	return func(c *Conn) error {
		if d < 0 {
			return errorNew("idle timeout cannot be negative")
		}
		c.idleTimeout = d
		return nil
	}
}

// ConnMaxFrameSize sets the maximum frame size that
// the connection will send our receive.
//
// Must be 512 or greater.
//
// Default: 512
func ConnMaxFrameSize(n uint32) ConnOption {
	return func(c *Conn) error {
		if n < 512 {
			return errorNew("max frame size must be 512 or greater")
		}
		c.maxFrameSize = n
		return nil
	}
}

type stateFunc func() stateFunc

// Conn is an AMQP connection.
type Conn struct {
	net net.Conn // underlying connection

	// TLS
	tlsNegotiation bool        // negotiate TLS
	tlsComplete    bool        // TLS negotiation complete
	tlsConfig      *tls.Config // TLS config, default used if nil (ServerName set to Conn.hostname)

	// SASL
	saslHandlers map[Symbol]stateFunc // map of supported handlers keyed by SASL mechanism, SASL not negotiated if nil
	saslComplete bool                 // SASL negotiation complete

	// local settings
	maxFrameSize uint32        // max frame size we accept
	channelMax   uint16        // maximum number of channels we'll create
	hostname     string        // hostname of remote server (set explicitly or parsed from URL)
	idleTimeout  time.Duration // maximum period between receiving frames

	// peer settings
	peerIdleTimeout  time.Duration // maximum period between sending frames
	peerMaxFrameSize uint32        // maximum frame size peer will accept

	// conn state
	errMu      sync.Mutex    // mux holds errMu from start until shutdown completes; operations are sequential before mux is started
	err        error         // error to be returned to client
	doneClosed int32         // atomically read/set; used to prevent double close
	done       chan struct{} // indicates the connection is done

	// mux
	readErr    chan error       // connReader notifications of an error
	rxProto    chan protoHeader // protoHeaders received by connReader
	rxFrame    chan frame       // AMQP frames received by connReader
	newSession chan *Session    // new Sessions are requested from mux by reading off this channel
	delSession chan *Session    // session completion is indicated to mux by sending the Session on this channel
}

// Dial connects to an AMQP server.
//
// If the addr includes a scheme, it must be "amqp" or "amqps".
// TLS will be negotiated when the scheme is "amqps".
//
// If no port is provided, 5672 will be used.
func Dial(addr string, opts ...ConnOption) (*Conn, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		host = u.Host
		port = "5672"
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

	opts = append([]ConnOption{
		ConnHostname(host),
		ConnTLS(u.Scheme == "amqps"),
	}, opts...)

	c, err := New(conn, opts...)
	if err != nil {
		return nil, err
	}

	return c, err
}

// New establishes an AMQP connection on pre-established
// net.Conn.
func New(conn net.Conn, opts ...ConnOption) (*Conn, error) {
	c := &Conn{
		net:              conn,
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

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	go c.connReader()

	for state := c.negotiateProto; state != nil; {
		state = state()
	}

	if c.err != nil {
		c.Close()
		return nil, c.err
	}

	go c.mux()

	return c, nil
}

// Close disconnects the connection.
func (c *Conn) Close() error {
	// TODO: shutdown AMQP
	c.closeDone()

	c.errMu.Lock()
	defer c.errMu.Unlock()
	err := c.net.Close()
	if c.err == nil {
		c.err = err
	}
	return c.err
}

// closeDone closes Conn.done if it has not already been closed
func (c *Conn) closeDone() {
	if atomic.CompareAndSwapInt32(&c.doneClosed, 0, 1) {
		close(c.done)
	}
}

// NewSession opens a new AMQP session to the server.
func (c *Conn) NewSession() (*Session, error) {
	var s *Session
	select {
	case <-c.done:
		return nil, c.err
	case s = <-c.newSession:
	}

	err := s.txFrame(&performBegin{
		NextOutgoingID: 0,
		IncomingWindow: 1,
	})
	if err != nil {
		s.Close()
		return nil, err
	}

	var fr frame
	select {
	case <-c.done:
		return nil, c.err
	case fr = <-s.rx:
	}
	begin, ok := fr.body.(*performBegin)
	if !ok {
		s.Close()
		return nil, errorErrorf("unexpected begin response: %+v", fr)
	}

	// TODO: record negotiated settings
	s.remoteChannel = begin.RemoteChannel

	go s.startMux()

	return s, nil
}

// keepaliveFrame is an AMQP frame with no body, it's used for keepalives
var keepaliveFrame = []byte{0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00}

// mux is called after initial connection establishment. It handles
// muxing of sessions, keepalives, and connection errors.
func (c *Conn) mux() {
	nextSession := newSession(c, 0)

	// map channel to session
	sessions := make(map[uint16]*Session)

	var keepalive <-chan time.Time
	if kaInterval := c.peerIdleTimeout / 2; kaInterval > 0 {
		ticker := time.NewTicker(kaInterval)
		defer ticker.Stop()
		keepalive = ticker.C
	}

	c.errMu.Lock()
	defer c.errMu.Unlock()

outer:
	for {
		if c.err != nil {
			c.closeDone()
			return
		}

		select {
		case c.err = <-c.readErr:

		case fr := <-c.rxFrame:
			ch, ok := sessions[fr.channel]
			if !ok {
				c.err = errorErrorf("unexpected frame: %#v", fr.body)
				continue outer
			}
			ch.rx <- fr

		case c.newSession <- nextSession: // TODO: enforce max session/wrapping
			sessions[nextSession.channel] = nextSession
			nextSession = newSession(c, nextSession.channel+1)

		case s := <-c.delSession:
			delete(sessions, s.channel)

		case <-keepalive:
			_, c.err = c.net.Write(keepaliveFrame)
		case <-c.done:
			return
		}
	}
}

// connReader reads from the net.Conn, decodes frames, and passes them
// up via the Conn.rxFrame and Conn.rxProto channels.
func (c *Conn) connReader() {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	rxBuf := make([]byte, c.maxFrameSize)

	var (
		negotiating     = true
		idleTimeout     = c.idleTimeout
		currentHeader   frameHeader
		frameInProgress = false
		err             error
	)

	for {
		// fmt.Println(frameInProgress, buf.Len())
		if frameInProgress || buf.Len() < 8 { // 8 = min size for header
			c.net.SetReadDeadline(time.Now().Add(idleTimeout))
			n, err := c.net.Read(rxBuf[:c.maxFrameSize]) // TODO: send error on frame too large
			if err != nil {
				c.readErr <- err
				return
			}

			_, err = buf.Write(rxBuf[:n])
			if err != nil {
				c.readErr <- err
				return
			}
		}

		if buf.Len() < 8 {
			continue
		}

		if negotiating && bytes.Equal(buf.Bytes()[:4], []byte{'A', 'M', 'Q', 'P'}) {
			p, err := parseProtoHeader(buf)
			if err != nil {
				c.readErr <- err
				return
			}

			if p.ProtoID == protoAMQP {
				negotiating = false
			}

			select {
			case <-c.done:
				return
			case c.rxProto <- p:
			}

			continue
		}

		if !frameInProgress {
			currentHeader, err = parseFrameHeader(buf)
			if err != nil {
				c.readErr <- err
				return
			}
			frameInProgress = true
		}

		if currentHeader.Size > math.MaxInt32 { // make max size configurable
			c.readErr <- errorNew("payload too large")
			return
		}

		bodySize := int(currentHeader.Size - 8)

		if buf.Len() < bodySize {
			continue
		}
		frameInProgress = false

		if bodySize == 0 {
			continue // empty frame, likely for keepalive
		}

		frameBody := buf.Next(bodySize)

		p, err := parseFrame(bytes.NewBuffer(frameBody))
		if err != nil {
			c.readErr <- err
			return
		}

		if o, ok := p.(*performOpen); ok && o.MaxFrameSize < c.maxFrameSize {
			if o.IdleTimeout > 0 && o.IdleTimeout < idleTimeout {
				idleTimeout = o.IdleTimeout
			}
		}

		select {
		case <-c.done:
			return
		case c.rxFrame <- frame{channel: currentHeader.Channel, body: p}:
		}
	}
}

// negotiateProto determines which proto to negotiate next
func (c *Conn) negotiateProto() stateFunc {
	switch {
	case c.tlsNegotiation && !c.tlsComplete:
		return c.exchangeProtoHeader(protoTLS)
	case c.saslHandlers != nil && !c.saslComplete:
		return c.exchangeProtoHeader(protoSASL)
	default:
		return c.exchangeProtoHeader(protoAMQP)
	}
}

// ProtoIDs
const (
	protoAMQP = 0x0
	protoTLS  = 0x2
	protoSASL = 0x3
)

// exchangeProtoHeader performs the round trip exchange of protocol
// headers, validation, and returns the protoID specific next state.
func (c *Conn) exchangeProtoHeader(protoID uint8) stateFunc {
	_, c.err = c.net.Write([]byte{'A', 'M', 'Q', 'P', protoID, 1, 0, 0})
	if c.err != nil {
		return nil
	}

	var p protoHeader
	select {
	case p = <-c.rxProto:
	case c.err = <-c.readErr:
		return nil
	case fr := <-c.rxFrame:
		c.err = errorErrorf("unexpected frame %#v", fr)
		return nil
	case <-time.After(1 * time.Second):
		c.err = ErrTimeout
		return nil
	}

	if protoID != p.ProtoID {
		c.err = errorErrorf("unexpected protocol header %#00x, expected %#00x", p.ProtoID, protoID)
		return nil
	}

	switch protoID {
	case protoAMQP:
		return c.openAMQP
	case protoTLS:
		return c.startTLS
	case protoSASL:
		return c.negotiateSASL
	default:
		c.err = errorErrorf("unknown protocol ID %#02x", p.ProtoID)
		return nil
	}
}

// startTLS wraps the conn with TLS and returns to Conn.negotiateProto
func (c *Conn) startTLS() stateFunc {
	if c.tlsConfig == nil {
		c.tlsConfig = new(tls.Config)
	}
	if c.tlsConfig.ServerName == "" && !c.tlsConfig.InsecureSkipVerify {
		c.tlsConfig.ServerName = c.hostname
	}
	c.net = tls.Client(c.net, c.tlsConfig)
	c.tlsComplete = true
	return c.negotiateProto
}

// txFrame encodes and transmits a frame on the connection
func (c *Conn) txFrame(fr frame) error {
	// BUG: this should respect c.peerMaxFrameSize
	return writeFrame(c.net, fr) // TODO: buffer?
}

// openAMQP round trips the AMQP open performative
func (c *Conn) openAMQP() stateFunc {
	c.err = c.txFrame(frame{
		typ: frameTypeAMQP,
		body: &performOpen{
			ContainerID:  randString(),
			Hostname:     c.hostname,
			MaxFrameSize: c.maxFrameSize,
			ChannelMax:   c.channelMax,
			IdleTimeout:  c.idleTimeout,
		},
		channel: 0,
	})
	if c.err != nil {
		return nil
	}

	fr, err := c.readFrame()
	if err != nil {
		c.err = err
		return nil
	}

	o, ok := fr.body.(*performOpen)
	if !ok {
		c.err = errorErrorf("unexpected frame type %T", fr.body)
		return nil
	}

	if o.MaxFrameSize > 0 {
		c.peerMaxFrameSize = o.MaxFrameSize // TODO: make writer adhere
	}

	if o.IdleTimeout > 0 {
		c.peerIdleTimeout = o.IdleTimeout
	}

	if o.ChannelMax < c.channelMax {
		c.channelMax = o.ChannelMax
	}

	return nil
}

// negotiateSASL returns the SASL handler for the first matched
// mechanism specified by the server
func (c *Conn) negotiateSASL() stateFunc {
	if c.saslHandlers == nil {
		// we don't support SASL
		c.err = errorErrorf("server request SASL, but not configured")
		return nil
	}

	fr, err := c.readFrame()
	if err != nil {
		c.err = err
		return nil
	}

	sm, ok := fr.body.(*saslMechanisms)
	if !ok {
		c.err = errorErrorf("unexpected frame type %T", fr.body)
		return nil
	}

	for _, mech := range sm.Mechanisms {
		if state, ok := c.saslHandlers[mech]; ok {
			return state
		}
	}

	// TODO: send some sort of "auth not supported" frame?
	c.err = errorErrorf("no supported auth mechanism (%v)", sm.Mechanisms)
	return nil
}

// saslOutcome processes the SASL outcome frame and return Conn.negotiateProto
// on success.
//
// SASL handlers return this stateFunc when the mechanism specific negotiation
// has completed.
func (c *Conn) saslOutcome() stateFunc {
	fr, err := c.readFrame()
	if err != nil {
		c.err = err
		return nil
	}

	so, ok := fr.body.(*saslOutcome)
	if !ok {
		c.err = errorErrorf("unexpected frame type %T", fr.body)
		return nil
	}

	if so.Code != codeSASLOK {
		c.err = errorErrorf("SASL PLAIN auth failed with code %#00x: %s", so.Code, so.AdditionalData)
		return nil
	}

	c.saslComplete = true

	return c.negotiateProto
}

// readFrame is used during connection establishment to read a single frame.
//
// After setup, Conn.mux handles incoming frames.
func (c *Conn) readFrame() (frame, error) {
	var fr frame
	select {
	case fr = <-c.rxFrame:
		return fr, nil
	case err := <-c.readErr:
		return fr, err
	case p := <-c.rxProto:
		return fr, errorErrorf("unexpected protocol header %#v", p)
	case <-time.After(1 * time.Second):
		return fr, ErrTimeout
	}
}
