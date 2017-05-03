package amqp

import (
	"bytes"
	"crypto/tls"
	"io"
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
	net        net.Conn      // underlying connection
	pauseRead  int32         // atomically set to indicate connReader should pause reading from network
	resumeRead chan struct{} // connReader reads from channel while paused, until channel is closed

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
		c.Close()
		return nil, c.err
	}

	// start multiplexor
	go c.mux()

	return c, nil
}

// Close disconnects the connection.
func (c *Conn) Close() error {
	// TODO: shutdown AMQP
	c.closeDone() // notify goroutines and blocked functions to exit

	// Conn.mux holds err lock until shutdown, we block until
	// shutdown completes and we can return the error (if any)
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
	// get a session allocated by Conn.mux
	var s *Session
	select {
	case <-c.done:
		return nil, c.err
	case s = <-c.newSession:
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
	case <-c.done:
		return nil, c.err
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

// keepaliveFrame is an AMQP frame with no body, used for keepalives
var keepaliveFrame = []byte{0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00}

// mux is start in it's own goroutine after initial connection establishment.
//  It handles muxing of sessions, keepalives, and connection errors.
func (c *Conn) mux() {
	// create the next session to allocate
	nextSession := newSession(c, 0)

	// map channel to sessions
	sessions := make(map[uint16]*Session)

	// if Conn.peerIdleTimeout is 0, keepalive will be nil and
	// no keepalives will be sent
	var keepalive <-chan time.Time

	// per spec, keepalives should be sent every 0.5 * idle timeout
	if kaInterval := c.peerIdleTimeout / 2; kaInterval > 0 {
		ticker := time.NewTicker(kaInterval)
		defer ticker.Stop()
		keepalive = ticker.C
	}

	// we hold the errMu lock until error or done
	c.errMu.Lock()
	defer c.errMu.Unlock()

	for {
		// check if last loop returned an error
		if c.err != nil {
			c.closeDone()
			return
		}

		select {
		// error from connReader
		case c.err = <-c.readErr:

		// new frame from connReader
		case fr := <-c.rxFrame:
			// lookup session and send to Session.mux
			ch, ok := sessions[fr.channel]
			if !ok {
				c.err = errorErrorf("unexpected frame: %#v", fr.body)
				continue
			}
			ch.rx <- fr

		// new session request
		//
		// Continually try to send the next session on the channel,
		// then add it to the sessions map. This allows us to control ID
		// allocation and prevents the need to have shared map. Since new
		// sessions are far less frequent than frames being sent to sessions,
		// we can avoid the lock/unlock for session lookup.
		case c.newSession <- nextSession:
			sessions[nextSession.channel] = nextSession

			// create the next session to send
			nextSession = newSession(c, nextSession.channel+1) // TODO: enforce max session/wrapping

		// session deletion
		case s := <-c.delSession:
			delete(sessions, s.channel)

		// keepalive timer
		case <-keepalive:
			// TODO: reset timer on non-keepalive transmit
			_, c.err = c.net.Write(keepaliveFrame)

		// connection is complete
		case <-c.done:
			return
		}
	}
}

// frameReader reads one frame at a time, up to n bytes
type frameReader struct {
	r io.Reader // underlying reader
	n int64     // max bytes per Read call
}

func (f *frameReader) Read(p []byte) (int, error) {
	if f.n < int64(len(p)) {
		p = p[:f.n]
	}
	n, err := f.r.Read(p)
	if err != nil {
		return n, err
	}
	return n, io.EOF
}

// connReader reads from the net.Conn, decodes frames, and passes them
// up via the Conn.rxFrame and Conn.rxProto channels.
func (c *Conn) connReader() {
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()

	var (
		negotiating     = true      // true during conn establishment, we should check for protoHeaders
		currentHeader   frameHeader // keep track of the current header, for frames split across multiple TCP packets
		frameInProgress bool        // true if we're in the middle of receiving data for currentHeader
	)

	// frameReader facilitates reading directly into buf
	fr := &frameReader{r: c.net, n: int64(c.maxFrameSize)}

	for {
		// we need to read more if buf doesn't contain the complete frame
		// or there's not enough in buf to parse the header
		if frameInProgress || buf.Len() < frameHeaderSize {
			c.net.SetReadDeadline(time.Now().Add(c.idleTimeout))
			_, err := buf.ReadFrom(fr) // TODO: send error on frame too large
			if err != nil {
				if atomic.LoadInt32(&c.pauseRead) == 1 {
					// need to stop reading during TLS negotiation,
					// see Conn.startTLS()
					c.pauseRead = 0
					for range c.resumeRead {
						// reads indicate paused, resume on close
					}
					fr.r = c.net // conn wrapped with TLS
					continue
				}

				c.readErr <- err
				return
			}
		}

		// read more if we didn't get enough to parse header
		if buf.Len() < frameHeaderSize {
			continue
		}

		// during negotiation, check for proto frames
		if negotiating && bytes.Equal(buf.Bytes()[:4], []byte{'A', 'M', 'Q', 'P'}) {
			p, err := parseProtoHeader(buf)
			if err != nil {
				c.readErr <- err
				return
			}

			// we know negotiation is complete once an AMQP proto frame
			// is received
			if p.ProtoID == protoAMQP {
				negotiating = false
			}

			// send proto header
			select {
			case <-c.done:
				return
			case c.rxProto <- p:
			}

			continue
		}

		// parse the header if we're not completeing an already
		// parsed frame
		if !frameInProgress {
			var err error
			currentHeader, err = parseFrameHeader(buf)
			if err != nil {
				c.readErr <- err
				return
			}
			frameInProgress = true
		}

		// check size is reasonable
		if currentHeader.Size > math.MaxInt32 { // make max size configurable
			c.readErr <- errorNew("payload too large")
			return
		}

		bodySize := int(currentHeader.Size - frameHeaderSize)

		// check if we have the full frame
		if buf.Len() < bodySize {
			continue
		}
		frameInProgress = false

		// check if body is empty (keepalive)
		if bodySize == 0 {
			continue
		}

		// parse the frame
		payload := bytes.NewBuffer(buf.Next(bodySize))
		parsedBody, err := parseFrame(payload)
		if err != nil {
			c.readErr <- err
			return
		}

		// send to mux
		select {
		case <-c.done:
			return
		case c.rxFrame <- frame{channel: currentHeader.Channel, body: parsedBody}:
		}
	}
}

// negotiateProto determines which proto to negotiate next
func (c *Conn) negotiateProto() stateFunc {
	// in the order each must be negotiated
	switch {
	case c.tlsNegotiation && !c.tlsComplete:
		return c.exchangeProtoHeader(protoTLS)
	case c.saslHandlers != nil && !c.saslComplete:
		return c.exchangeProtoHeader(protoSASL)
	default:
		return c.exchangeProtoHeader(protoAMQP)
	}
}

// protocol IDs received in protoHeaders
const (
	protoAMQP = 0x0
	protoTLS  = 0x2
	protoSASL = 0x3
)

// exchangeProtoHeader performs the round trip exchange of protocol
// headers, validation, and returns the protoID specific next state.
func (c *Conn) exchangeProtoHeader(protoID uint8) stateFunc {
	// write the proto header
	c.net.SetWriteDeadline(time.Now().Add(1 * time.Second)) // TODO: make configurable
	_, c.err = c.net.Write([]byte{'A', 'M', 'Q', 'P', protoID, 1, 0, 0})
	if c.err != nil {
		c.err = errorWrapf(c.err, "writing to network")
		return nil
	}
	c.net.SetWriteDeadline(time.Time{})

	// read response header
	var p protoHeader
	select {
	case p = <-c.rxProto:
	case c.err = <-c.readErr:
		return nil
	case fr := <-c.rxFrame:
		c.err = errorErrorf("unexpected frame %#v", fr)
		return nil
	case <-time.After(1 * time.Second):
		c.err = errorWrapf(ErrTimeout, "timeout")
		return nil
	}

	if protoID != p.ProtoID {
		c.err = errorErrorf("unexpected protocol header %#00x, expected %#00x", p.ProtoID, protoID)
		return nil
	}

	// go to the proto specific state
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
	// create a new config if not already set
	if c.tlsConfig == nil {
		c.tlsConfig = new(tls.Config)
	}

	// TLS config must have ServerName or InsecureSkipVerify set
	if c.tlsConfig.ServerName == "" && !c.tlsConfig.InsecureSkipVerify {
		c.tlsConfig.ServerName = c.hostname
	}

	// convoluted method to pause connReader, explorer simpler alternatives
	c.resumeRead = make(chan struct{})        // 1. create channel
	atomic.StoreInt32(&c.pauseRead, 1)        // 2. indicate should pause
	c.net.SetReadDeadline(time.Time{}.Add(1)) // 3. set deadline to interrupt connReader
	c.resumeRead <- struct{}{}                // 4. wait for connReader to read from chan, indicating paused
	defer close(c.resumeRead)                 // 5. defer connReader resume by closing channel
	c.net.SetReadDeadline(time.Time{})        // 6. clear deadline

	// wrap existing net.Conn and perform TLS handshake
	conn := tls.Client(c.net, c.tlsConfig)
	c.err = conn.Handshake()
	if c.err != nil {
		return nil
	}

	// swap net.Conn
	c.net = conn
	c.tlsComplete = true

	// go to next protocol
	return c.negotiateProto
}

// txFrame encodes and transmits a frame on the connection
func (c *Conn) txFrame(fr frame) error {
	// BUG: This should respect c.peerMaxFrameSize. Should not affect current functionality;
	//      only transfer frames should be larger than min-max frame size (512).
	return writeFrame(c.net, fr) // TODO: buffer?
}

// openAMQP round trips the AMQP open performative
func (c *Conn) openAMQP() stateFunc {
	// send open frame
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

	// get the response
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

	// update peer settings
	if o.MaxFrameSize > 0 {
		c.peerMaxFrameSize = o.MaxFrameSize
	}
	if o.IdleTimeout > 0 {
		c.peerIdleTimeout = o.IdleTimeout
	}
	if o.ChannelMax < c.channelMax {
		c.channelMax = o.ChannelMax
	}

	// connection established, exit state machine
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
		return fr, errorWrapf(ErrTimeout, "timeout")
	}
}
