package amqp

import (
	"bytes"
	"context"
	"fmt"
	"log"
)

type ErrDetach struct {
	RemoteError *Error
}

func (e ErrDetach) Error() string {
	return fmt.Sprintf("link detached, reason: %+v", e.RemoteError)
}

type link struct {
	handle     uint32
	sourceAddr string
	linkCredit uint32
	rx         chan preformative
	session    *Session

	creditUsed          uint32
	senderDeliveryCount uint32
	closed              bool
	err                 error
	detachRx            bool
}

func (l *link) close() {
	if !l.closed {
		l.session.txFrame(&performDetach{
			Handle: l.handle,
			Closed: true,
		})
		l.closed = true

		if !l.detachRx {
		outer:
			for {
				// TODO: timeout
				select {
				case <-l.session.conn.done:
					l.err = l.session.conn.err
				case fr := <-l.rx:
					if fr, ok := fr.(*performDetach); ok && fr.Closed {
						break outer
					}
				}
			}
		}

		l.session.delLink <- l
	}
}

func newLink(s *Session, handle uint32) *link {
	return &link{
		handle:     handle,
		linkCredit: 1,
		rx:         make(chan preformative),
		session:    s,
	}
}

type LinkOption func(*link) error

func LinkSource(source string) LinkOption {
	return func(l *link) error {
		l.sourceAddr = source
		return nil
	}
}

func LinkCredit(credit uint32) LinkOption {
	return func(l *link) error {
		l.linkCredit = credit
		return nil
	}
}

type Receiver struct {
	link *link

	buf *bytes.Buffer
}

func (r *Receiver) sendFlow() error {
	newLinkCredit := r.link.linkCredit - (r.link.linkCredit - r.link.creditUsed)
	r.link.senderDeliveryCount += r.link.creditUsed
	err := r.link.session.txFrame(&performFlow{
		IncomingWindow: 2147483647,
		NextOutgoingID: 0,
		OutgoingWindow: 0,
		Handle:         &r.link.handle,
		DeliveryCount:  &r.link.senderDeliveryCount,
		LinkCredit:     &newLinkCredit,
	})
	r.link.creditUsed = 0
	return err
}

func (r *Receiver) Receive(ctx context.Context) (*Message, error) {
	r.buf.Reset()

	msg := &Message{link: r.link}

	first := true
outer:
	for {
		if r.link.creditUsed > r.link.linkCredit/2 {
			err := r.sendFlow()
			if err != nil {
				return nil, err
			}
		}

		var fr preformative
		select {
		case <-r.link.session.conn.done:
			return nil, r.link.session.conn.err
		case fr = <-r.link.rx:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		switch fr := fr.(type) {
		case *performTransfer:
			r.link.creditUsed++

			if first && fr.DeliveryID != nil {
				msg.deliveryID = *fr.DeliveryID
				first = false
			}

			r.buf.Write(fr.Payload)
			if !fr.More {
				break outer
			}
		case *performDetach:
			if !fr.Closed {
				log.Panicf("non-closing detach not supported: %+v", fr)
			}

			r.link.detachRx = true
			r.link.close()

			return nil, ErrDetach{fr.Error}
		}
	}

	err := unmarshal(r.buf, msg)
	return msg, err
}

func (r *Receiver) Close() error {
	r.link.close()
	bufPool.Put(r.buf)
	r.buf = nil
	return r.link.err
}
