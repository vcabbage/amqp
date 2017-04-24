package amqp

import (
	"bytes"
	"crypto/tls"
	"fmt"
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

func ConnTLS(enable bool) ConnOpt {
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

	err  error
	done chan struct{}

	// SASL
	saslHandlers map[Symbol]stateFunc
	saslComplete bool

	// mux
	readErr    chan error
	rxProto    chan proto
	rxFrame    chan frame
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
		ConnTLS(u.Scheme == "amqps"),
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
		done:         make(chan struct{}),
		readErr:      make(chan error, 1),
		rxProto:      make(chan proto),
		rxFrame:      make(chan frame),
		newSession:   make(chan *Session),
		delSession:   make(chan *Session),
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
		if c.net != nil {
			c.net.Close()
		}
		return nil, c.err
	}

	go c.startMux()

	return c, nil
}

func (c *Conn) Close() error {
	// TODO: shutdown AMQP
	err := c.net.Close()
	if c.err == nil {
		c.err = err
	}
	return c.err
}

func (c *Conn) MaxFrameSize() int {
	return int(c.maxFrameSize)
}

func (c *Conn) ChannelMax() int {
	return int(c.channelMax)
}

func (c *Conn) NewSession() (*Session, error) {
	var s *Session
	select {
	case <-c.done:
		return nil, c.err
	case s = <-c.newSession:
	}

	err := s.txFrame(&performativeBegin{
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
	begin, ok := fr.preformative.(*performativeBegin)
	if !ok {
		s.Close()
		return nil, fmt.Errorf("unexpected begin response: %+v", fr)
	}

	fmt.Printf("Begin Resp: %+v", begin)
	// TODO: record negotiated settings
	s.remoteChannel = begin.RemoteChannel

	go s.startMux()

	return s, nil
}

var keepaliveFrame = []byte{0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00}

func (c *Conn) startMux() {
	nextSession := newSession(c, 0)

	// map channel to session
	sessions := make(map[uint16]*Session)

	keepalive := time.NewTicker(c.idleTimeout / 2)

	fmt.Println("Starting mux")

outer:
	for {
		if c.err != nil {
			close(c.done)
			return
		}

		select {
		case c.err = <-c.readErr:
			fmt.Println("Got read error")

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
			nextSession = newSession(c, nextSession.channel+1)

		case s := <-c.delSession:
			fmt.Println("Got delete session request")
			delete(sessions, s.channel)

		case <-keepalive.C:
			fmt.Printf("Writing: %# 02x\n", keepaliveFrame)
			_, c.err = c.net.Write(keepaliveFrame)
		}
	}
}

func (c *Conn) connReader() {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	rxBuf := make([]byte, c.maxFrameSize)

	negotiating := true

outer:
	for {
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

		for buf.Len() > 7 { // 8 = min size for header
			if negotiating && bytes.Equal(buf.Bytes()[:4], []byte{'A', 'M', 'Q', 'P'}) {
				p, err := parseProto(buf)
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

			frameHeader, err := parseFrameHeader(buf)
			if err != nil {
				c.readErr <- err
				return
			}

			if buf.Len() < int(frameHeader.Size)-8 {
				continue outer
			}

			p, err := parseFrame(buf)
			if err != nil {
				c.readErr <- err
				return
			}

			if o, ok := p.(*performativeOpen); ok && o.MaxFrameSize < c.maxFrameSize {
				c.maxFrameSize = o.MaxFrameSize
			}

			select {
			case <-c.done:
				return
			case c.rxFrame <- frame{channel: frameHeader.Channel, preformative: p}:
			}
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

func (c *Conn) exchangeProtoHeader(protoID uint8) stateFunc {
	_, c.err = c.net.Write([]byte{'A', 'M', 'Q', 'P', protoID, 1, 0, 0})
	if c.err != nil {
		return nil
	}

	var p proto
	select {
	case p = <-c.rxProto:
	case c.err = <-c.readErr:
		return nil
	case fr := <-c.rxFrame:
		c.err = errors.Errorf("unexpected frame %#v", fr)
	case <-time.After(1 * time.Second):
		c.err = ErrTimeout
		return nil
	}

	fmt.Printf("Proto: %s; ProtoID: %d; Version: %d.%d.%d\n",
		p.Proto,
		p.ProtoID,
		p.Major,
		p.Minor,
		p.Revision,
	)

	if protoID != p.ProtoID {
		c.err = fmt.Errorf("unexpected protocol header %#00x, expected %#00x", p.ProtoID, protoID)
		return nil
	}

	switch protoID {
	case protoAMQP:
		return c.txOpen
	case protoTLS:
		return c.protoTLS
	case protoSASL:
		return c.protoSASL
	default:
		c.err = fmt.Errorf("unknown protocol ID %#02x", p.ProtoID)
		return nil
	}
}

func (c *Conn) protoTLS() stateFunc {
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
			ContainerID:  randString(),
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
	fr, err := c.readFrame()
	if err != nil {
		c.err = err
		return nil
	}

	o, ok := fr.preformative.(*performativeOpen)
	if !ok {
		c.err = fmt.Errorf("unexpected frame type %T", fr.preformative)
	}

	fmt.Printf("Rx Open: %#v\n", o)

	if o.IdleTimeout > 0 {
		c.idleTimeout = o.IdleTimeout
	}

	if o.ChannelMax < c.channelMax {
		c.channelMax = o.ChannelMax
	}

	return nil
}

func (c *Conn) protoSASL() stateFunc {
	if c.saslHandlers == nil {
		// we don't support SASL
		c.err = fmt.Errorf("server request SASL, but not configured")
		return nil
	}

	fr, err := c.readFrame()
	if err != nil {
		c.err = err
		return nil
	}

	sm, ok := fr.preformative.(*saslMechanisms)
	if !ok {
		c.err = fmt.Errorf("unexpected frame type %T", fr.preformative)
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
	fr, err := c.readFrame()
	if err != nil {
		c.err = err
		return nil
	}

	so, ok := fr.preformative.(*saslOutcome)
	if !ok {
		c.err = fmt.Errorf("unexpected frame type %T", fr.preformative)
	}

	if so.Code != codeSASLOK {
		c.err = fmt.Errorf("SASL PLAIN auth failed with code %#00x: %s", so.Code, so.AdditionalData)
		return nil
	}

	c.saslComplete = true

	return c.negotiateProto
}

var ErrTimeout = errors.New("timeout waiting for response")

func (c *Conn) readFrame() (frame, error) {
	var fr frame
	select {
	case fr = <-c.rxFrame:
		return fr, nil
	case err := <-c.readErr:
		return fr, err
	case p := <-c.rxProto:
		return fr, errors.Errorf("unexpected protocol header %#v", p)
	case <-time.After(1 * time.Second):
		return fr, ErrTimeout
	}
}
