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
	err           error
	rx            chan frame

	newLink chan *link
	delLink chan *link
}

func (s *Session) Close() {
	// s.txFrame(&)
	// TODO: send end preformative
	s.conn.delSession <- s
}

func (s *Session) txFrame(p Preformative) {
	s.conn.txFrame <- frame{preformative: p, channel: s.channel}
}

func randString() string { // TODO: random string gen off SO, replace
	var letterBytes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]byte, 40)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func (s *Session) Receiver(opts ...LinkOption) (*Receiver, error) {
	link := <-s.newLink

	for _, o := range opts {
		err := o(link)
		if err != nil {
			// TODO: release link
			return nil, err
		}
	}
	link.creditUsed = link.linkCredit

	s.txFrame(&Attach{
		Name:   randString(),
		Handle: link.handle,
		Role:   true,
		Source: &Source{
			Address: link.sourceAddr,
		},
	})

	fr := <-link.rx
	resp, ok := fr.(*Attach)
	if !ok {
		return nil, fmt.Errorf("unexpected attach response: %+v", fr)
	}

	fmt.Printf("Attach Resp: %+v\n", resp)
	fmt.Printf("Attach Source: %+v\n", resp.Source)
	fmt.Printf("Attach Target: %+v\n", resp.Target)

	link.senderDeliveryCount = resp.InitialDeliveryCount

	r := &Receiver{
		link: link,
		buf:  bufPool.New().(*bytes.Buffer),
	}

	return r, nil
}

func (s *Session) startMux() {
	links := make(map[uint32]*link)
	nextLink := newLink(s, 0)

	for {
		select {
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
				link.rx <- fr.preformative
			}()
		}
	}
}
