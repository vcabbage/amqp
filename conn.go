package amqp

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/url"
	"time"

	"github.com/pkg/errors"
)

// connection defaults
const (
	initialMaxFrameSize = 512
	initialChannelMax   = 1
)

type ConnOpt func(*Conn) error

func ConnHostname(hostname string) ConnOpt {
	return func(c *Conn) error {
		c.hostname = hostname
		return nil
	}
}

func ConnNegotiateTLS(enable bool) ConnOpt {
	return func(c *Conn) error {
		c.tlsNegotiation = enable
		return nil
	}
}

func ConnTLSConfig(tc *tls.Config) ConnOpt {
	return func(c *Conn) error {
		c.tlsConfig = tc
		c.tlsNegotiation = true
		return nil
	}
}

type stateFunc func() stateFunc

type Conn struct {
	net net.Conn

	// TLS
	tlsNegotiation bool
	tlsComplete    bool
	tlsConfig      *tls.Config

	maxFrameSize uint32
	channelMax   uint16
	hostname     string
	idleTimeout  time.Duration

	rxBuf []byte
	err   error

	// SASL
	saslHandlers map[Symbol]stateFunc
	saslComplete bool

	// mux
	readErr    chan error
	rxFrame    chan frame
	txFrame    chan frame
	newSession chan *Session
	delSession chan *Session
}

func Dial(addr string, opts ...ConnOpt) (*Conn, error) {
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
		return nil, fmt.Errorf("unsupported scheme %q", u.Scheme)
	}

	conn, err := net.Dial("tcp", host+":"+port)
	if err != nil {
		return nil, err
	}

	opts = append([]ConnOpt{
		ConnHostname(host),
		ConnNegotiateTLS(u.Scheme == "amqps"),
	}, opts...)

	c, err := New(conn, opts...)
	if err != nil {
		return nil, err
	}

	return c, err
}

func New(conn net.Conn, opts ...ConnOpt) (*Conn, error) {
	c := &Conn{
		net:          conn,
		maxFrameSize: initialMaxFrameSize,
		channelMax:   initialChannelMax,
		idleTimeout:  1 * time.Minute,
		readErr:      make(chan error),
		rxFrame:      make(chan frame),
		txFrame:      make(chan frame),
		newSession:   make(chan *Session),
		delSession:   make(chan *Session),
	}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	for state := c.negotiateProto; state != nil; {
		state = state()
	}

	if c.err != nil && c.net != nil {
		c.net.Close()
	}

	return c, c.err
}

func (c *Conn) Close() error {
	// TODO: shutdown AMQP
	return c.net.Close()
}

func (c *Conn) MaxFrameSize() int {
	return int(c.maxFrameSize)
}

func (c *Conn) ChannelMax() int {
	return int(c.channelMax)
}

func (c *Conn) NewSession() (*Session, error) {
	s := <-c.newSession
	if s.err != nil {
		return nil, s.err
	}

	s.txFrame(&performativeBegin{
		NextOutgoingID: 0,
		IncomingWindow: 1,
	})

	fr := <-s.rx
	begin, ok := fr.preformative.(*performativeBegin)
	if !ok {
		s.Close()
		return nil, fmt.Errorf("unexpected begin response: %+v", fr)
	}

	fmt.Printf("Begin Resp: %+v", begin)
	// TODO: record negotiated settings
	s.remoteChannel = fr.channel
	s.newLink = make(chan *link)
	s.delLink = make(chan *link)

	go s.startMux()

	return s, nil
}

func parseFrame(payload []byte) (preformative, error) {
	pType, err := preformativeType(payload)
	if err != nil {
		return nil, err
	}

	var t preformative
	switch pType {
	case preformativeOpen:
		t = &performativeOpen{}
	case preformativeBegin:
		t = &performativeBegin{}
	case preformativeAttach:
		t = &performativeAttach{}
	case preformativeFlow:
		t = &flow{}
	case preformativeTransfer:
		t = &performativeTransfer{}
	case preformativeDisposition:
		t = &performativeDisposition{}
	case preformativeDetach:
		t = &performativeDetach{}
	case preformativeEnd:
		t = &performativeEnd{}
	case preformativeClose:
		t = &performativeClose{}
	default:
		return nil, errors.Errorf("unknown preformative type %0x", pType)
	}

	err = unmarshal(bytes.NewReader(payload), t)
	return t, err
}

type frame struct {
	channel      uint16
	preformative preformative
}

func (c *Conn) startMux() {
	go c.connReader()

	nextSession := &Session{conn: c, rx: make(chan frame)}

	// map channel to session
	sessions := make(map[uint16]*Session)

	keepalive := time.NewTicker(c.idleTimeout / 2)
	var buf bytes.Buffer
	writeFrame(&buf, frameTypeAMQP, 0, nil)
	keepaliveFrame := buf.Bytes()
	buf.Reset()

	fmt.Println("Starting mux")

outer:
	for {
		if c.err != nil {
			panic(c.err) // TODO: graceful close
		}

		select {
		case err := <-c.readErr:
			fmt.Println("Got read error")
			c.err = err

		case fr := <-c.rxFrame:
			ch, ok := sessions[fr.channel]
			if !ok {
				c.err = errors.Errorf("unexpected frame: %+v", fr)
				continue outer
			}
			ch.rx <- fr

		case c.newSession <- nextSession:
			fmt.Println("Got new session request")
			sessions[nextSession.channel] = nextSession
			// TODO: handle max session/wrapping
			nextSession = &Session{conn: c, channel: nextSession.channel + 1, rx: make(chan frame)}

		case s := <-c.delSession:
			fmt.Println("Got delete session request")
			delete(sessions, s.channel)

		case fr := <-c.txFrame:
			fmt.Printf("Writing: %d; %+v\n", fr.channel, fr.preformative)
			c.err = c.txPreformative(fr)

		case <-keepalive.C:
			fmt.Printf("Writing: %# 02x\n", keepaliveFrame)
			_, c.err = c.net.Write(keepaliveFrame)
		}
	}
}

func (c *Conn) connReader() {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()

outer:
	for {
		n, err := c.net.Read(c.rxBuf[:c.maxFrameSize]) // TODO: send error on frame too large
		if err != nil {
			c.readErr <- err
			return
		}

		_, err = buf.Write(c.rxBuf[:n])
		if err != nil {
			c.readErr <- err
			return
		}

		for buf.Len() > 8 { // 8 = min size for header
			frameHeader, err := parseFrameHeader(buf.Bytes())
			if err != nil {
				c.err = err
				continue outer
			}

			if buf.Len() < int(frameHeader.size) {
				continue outer
			}

			payload := make([]byte, frameHeader.size)
			_, err = io.ReadFull(buf, payload)
			if err != nil {
				c.err = err
				continue outer
			}

			preformative, err := parseFrame(payload[8:])
			if err != nil {
				c.err = err
				continue outer
			}

			c.rxFrame <- frame{channel: frameHeader.channel, preformative: preformative}
		}
	}
}

/*
On connection open, we'll need to handle 4 possible scenarios:
1. Straight into AMQP.
2. SASL -> AMQP.
3. TLS -> AMQP.
4. TLS -> SASL -> AMQP
*/
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

func (c *Conn) exchangeProtoHeader(proto uint8) stateFunc {
	_, c.err = c.net.Write([]byte{'A', 'M', 'Q', 'P', proto, 1, 0, 0})
	if c.err != nil {
		return nil
	}

	c.rxBuf = make([]byte, c.maxFrameSize)
	n, err := c.net.Read(c.rxBuf)
	if err != nil {
		c.err = err
		return nil
	}

	fmt.Printf("Read %d bytes.\n", n)

	p, err := parseProto(c.rxBuf[:n])
	if err != nil {
		c.err = err
		return nil
	}

	fmt.Printf("Proto: %s; ProtoID: %d; Version: %d.%d.%d\n",
		p.proto,
		p.protoID,
		p.major,
		p.minor,
		p.revision,
	)

	if proto != p.protoID {
		c.err = fmt.Errorf("unexpected protocol header %#00x, expected %#00x", p.protoID, proto)
		return nil
	}

	switch proto {
	case protoAMQP:
		return c.txOpen
	case protoTLS:
		return c.protoTLS
	case protoSASL:
		return c.protoSASL
	default:
		c.err = fmt.Errorf("unknown protocol ID %#02x", p.protoID)
		return nil
	}
}

func (c *Conn) protoTLS() stateFunc {
	if c.tlsConfig == nil {
		c.tlsConfig = &tls.Config{ServerName: c.hostname}
	}
	c.net = tls.Client(c.net, c.tlsConfig)
	c.tlsComplete = true
	return c.negotiateProto
}

func (c *Conn) txPreformative(fr frame) error {
	data, err := marshal(fr.preformative)
	if err != nil {
		return err
	}

	wr := bufPool.New().(*bytes.Buffer)
	defer bufPool.Put(wr)
	wr.Reset()

	err = writeFrame(wr, frameTypeAMQP, fr.channel, data)
	if err != nil {
		return err
	}

	_, err = c.net.Write(wr.Bytes())
	return err
}

func (c *Conn) txOpen() stateFunc {
	c.err = c.txPreformative(frame{
		preformative: &performativeOpen{
			ContainerID:  "gopher",
			Hostname:     c.hostname,
			MaxFrameSize: c.maxFrameSize,
			ChannelMax:   c.channelMax,
		},
		channel: 0,
	})
	if c.err != nil {
		return nil
	}

	return c.rxOpen
}

func (c *Conn) rxOpen() stateFunc {
	n, err := c.net.Read(c.rxBuf)
	if err != nil {
		c.err = errors.Wrapf(err, "reading")
		return nil
	}

	fh, err := parseFrameHeader(c.rxBuf[:n])
	if err != nil {
		c.err = errors.Wrapf(err, "parsing frame header")
		return nil
	}

	if fh.frameType != frameTypeAMQP {
		c.err = fmt.Errorf("unexpected frame type %#02x", fh.frameType)
	}

	var o performativeOpen
	err = unmarshal(bytes.NewBuffer(c.rxBuf[fh.dataOffsetBytes():n]), &o)
	if err != nil {
		c.err = errors.Wrapf(err, "unmarshaling")
		return nil
	}

	fmt.Printf("Rx Open: %#v\n", o)

	if o.IdleTimeout > 0 {
		c.idleTimeout = o.IdleTimeout
	}

	if o.MaxFrameSize < c.maxFrameSize {
		c.maxFrameSize = o.MaxFrameSize
	}
	if o.ChannelMax < c.channelMax {
		c.channelMax = o.ChannelMax
	}

	if uint32(len(c.rxBuf)) < c.maxFrameSize {
		c.rxBuf = make([]byte, c.maxFrameSize)
	}

	go c.startMux()

	return nil
}

func (c *Conn) protoSASL() stateFunc {
	if c.saslHandlers == nil {
		// we don't support SASL
		c.err = fmt.Errorf("server request SASL, but not configured")
		return nil
	}

	n, err := c.net.Read(c.rxBuf)
	if err != nil {
		c.err = err
		return nil
	}

	fh, err := parseFrameHeader(c.rxBuf[:n])
	if err != nil {
		c.err = err
		return nil
	}

	if fh.frameType != frameTypeSASL {
		c.err = fmt.Errorf("unexpected frame type %#02x", fh.frameType)
	}

	var sm saslMechanisms
	err = unmarshal(bytes.NewBuffer(c.rxBuf[fh.dataOffsetBytes():n]), &sm)
	if err != nil {
		c.err = err
		return nil
	}

	for _, mech := range sm.Mechanisms {
		if state, ok := c.saslHandlers[mech]; ok {
			return state
		}
	}

	// TODO: send some sort of "auth not supported" frame?
	c.err = fmt.Errorf("no supported auth mechanism (%v)", sm.Mechanisms)
	return nil
}

func (c *Conn) saslOutcome() stateFunc {
	n, err := c.net.Read(c.rxBuf)
	if err != nil {
		c.err = err
		return nil
	}

	fh, err := parseFrameHeader(c.rxBuf[:n])
	if err != nil {
		c.err = err
		return nil
	}

	if fh.frameType != frameTypeSASL {
		c.err = fmt.Errorf("unexpected frame type %#02x", fh.frameType)
	}

	var so saslOutcome
	c.err = unmarshal(bytes.NewBuffer(c.rxBuf[fh.dataOffsetBytes():n]), &so)
	if c.err != nil {
		return nil
	}

	if so.Code != codeSASLOK {
		c.err = fmt.Errorf("SASL PLAIN auth failed with code %#00x: %s", so.Code, so.AdditionalData)
		return nil
	}

	c.saslComplete = true

	return c.negotiateProto
}
