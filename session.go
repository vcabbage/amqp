package amqp

import (
	"bytes"
	"log"
	"math/rand"
)

type Session struct {
	channel       uint16
	remoteChannel uint16
	conn          *Conn
	rx            chan frame

	allocateHandle   chan *link
	deallocateHandle chan *link
}

func newSession(c *Conn, channel uint16) *Session {
	return &Session{
		conn:             c,
		channel:          channel,
		rx:               make(chan frame),
		allocateHandle:   make(chan *link),
		deallocateHandle: make(chan *link),
	}
}

func (s *Session) Close() error {
	// TODO: send end preformative
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

	r := &Receiver{
		link: l,
		buf:  bufPool.New().(*bytes.Buffer),
	}

	return r, nil
}

func (s *Session) startMux() {
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
