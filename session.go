package amqp

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
)

type Session struct {
	channel       uint16
	remoteChannel uint16
	conn          *Conn
	rx            chan frame

	newLink chan *link
	delLink chan *link
}

func newSession(c *Conn, channel uint16) *Session {
	return &Session{
		conn:    c,
		channel: channel,
		rx:      make(chan frame),
		newLink: make(chan *link),
		delLink: make(chan *link),
	}
}

func (s *Session) Close() error {
	// s.txFrame(&)
	// TODO: send end preformative
	select {
	case <-s.conn.done:
		return s.conn.err
	case s.conn.delSession <- s:
		return nil
	}
}

func (s *Session) txFrame(p preformative) error {
	return s.conn.txPreformative(frame{preformative: p, channel: s.channel})
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
	var l *link
	select {
	case <-s.conn.done:
		return nil, s.conn.err
	case l = <-s.newLink:
	}

	for _, o := range opts {
		err := o(l)
		if err != nil {
			// TODO: release link
			return nil, err
		}
	}
	l.creditUsed = l.linkCredit

	s.txFrame(&performativeAttach{
		Name:   randString(),
		Handle: l.handle,
		Role:   true,
		Source: &source{
			Address: l.sourceAddr,
		},
	})

	var fr preformative
	select {
	case <-s.conn.done:
		return nil, s.conn.err
	case fr = <-l.rx:
	}
	resp, ok := fr.(*performativeAttach)
	if !ok {
		return nil, fmt.Errorf("unexpected attach response: %+v", fr)
	}

	fmt.Printf("Attach Resp: %+v\n", resp)
	fmt.Printf("Attach Source: %+v\n", resp.Source)
	fmt.Printf("Attach Target: %+v\n", resp.Target)

	l.senderDeliveryCount = resp.InitialDeliveryCount

	r := &Receiver{
		link: l,
		buf:  bufPool.New().(*bytes.Buffer),
	}

	return r, nil
}

func (s *Session) startMux() {
	links := make(map[uint32]*link)
	nextLink := newLink(s, 0)

	for {
		select {
		case <-s.conn.done:
			return
		case s.newLink <- nextLink:
			fmt.Println("Got new link request")
			links[nextLink.handle] = nextLink
			// TODO: handle max session/wrapping
			nextLink = newLink(s, nextLink.handle+1)

		case link := <-s.delLink:
			fmt.Println("Got link deletion request")
			delete(links, link.handle)
			close(link.rx)

		case fr := <-s.rx:
			go func() {
				handle, ok := fr.preformative.link()
				if !ok {
					log.Printf("unexpected frame: %+v (%T)", fr, fr.preformative)
					return
				}

				link, ok := links[handle]
				if !ok {
					log.Printf("frame with unknown handle %d: %+v", handle, fr)
					return
				}
				link.rx <- fr.preformative // TODO: timeout
			}()
		}
	}
}
