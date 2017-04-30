package amqp

import (
	"bytes"
	"context"
	"fmt"
	"log"
)

// ErrDetach is returned by a link (Receiver) when a detach frame is received.
//
// RemoteError will be nil if the link was detached gracefully.
type ErrDetach struct {
	RemoteError *Error
}

func (e ErrDetach) Error() string {
	return fmt.Sprintf("link detached, reason: %+v", e.RemoteError)
}

// link is a unidirectional route.
//
// May be used for sending or receiving, currently only receive implemented.
type link struct {
	handle     uint32         // our handle
	sourceAddr string         // address sent during attach
	linkCredit uint32         // maximum number of messages allowed between flow updates
	rx         chan frameBody // sessions sends frames for this link on this channel
	session    *Session       // parent session

	creditUsed          uint32 // currently used credits
	senderDeliveryCount uint32 // number of messages sent/received
	detachSent          bool   // we've sent a detach frame
	detachReceived      bool
	err                 error // err returned on Close()
}

// newLink is used by Session.mux to create new links
func newLink(s *Session) *link {
	return &link{
		linkCredit: 1,
		session:    s,
	}
}

// close closes and requests deletion of the link.
//
// No operations on link are valid after close.
func (l *link) close() {
	if l.detachSent {
		return
	}

	l.session.txFrame(&performDetach{
		Handle: l.handle,
		Closed: true,
	})
	l.detachSent = true

	if !l.detachReceived {
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

	l.session.deallocateHandle <- l
}

// LinkOption is an function for configuring an AMQP links.
//
// A link may be a Sender or a Receiver. Only Receiver is currently implemented.
type LinkOption func(*link) error

// LinkSource sets the source address.
func LinkSource(source string) LinkOption {
	return func(l *link) error {
		l.sourceAddr = source
		return nil
	}
}

// LinkCredit specifies the maximum number of unacknowledged messages
// the sender can transmit.
func LinkCredit(credit uint32) LinkOption { // TODO: make receiver specific?
	return func(l *link) error {
		l.linkCredit = credit
		return nil
	}
}

// Receiver receives messages on a single AMQP link.
type Receiver struct {
	link *link

	buf *bytes.Buffer
}

// sendFlow transmits a flow frame with enough credits to bring the sender's
// link credits up to l.link.linkCredit.
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

// Receive returns the next message from the sender.
//
// Blocks until a message is received, ctx completes, or an error occurs.
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

		var fr frameBody
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

			r.link.detachReceived = true
			r.link.close()

			return nil, ErrDetach{fr.Error}
		}
	}

	_, err := unmarshal(r.buf, msg)
	return msg, err
}

// Close closes the Receiver and AMQP link.
func (r *Receiver) Close() error {
	r.link.close()
	bufPool.Put(r.buf)
	r.buf = nil
	return r.link.err
}
