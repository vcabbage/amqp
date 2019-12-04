package amqp

import (
	"context"
	"net"
)

// ConnAllowIncoming allows incoming sessions and links to be accepted.
//
// If you pass this option you MUST start goroutines to service
// Conn.NextIncoming() and Session.NextIncoming(), see the example.
//
// The Server example in this package shows how to use NextIncoming().  Note
// however that AMQP is symmetric, and ConnAllowIncoming can be used with client
// or server connections.
func ConnAllowIncoming() ConnOption { panic("FIXME") }

// NewIncoming treats c as an incoming server connection (e.g. from
// net.Listener.Accept()) and reads the initial OPEN frame.
func NewIncoming(c net.Conn) (*IncomingConn, error) { panic("FIXME") }

// IncomingConn represents an incoming OPEN request.
type IncomingConn struct {
	// FIXME ...relevant open fields...
}

// Accept sends an OPEN response and creates a Conn.
//
// Pass the ConnAllowIncoming() options to accept incoming sessions and links on
// this connection.
func (*IncomingConn) Accept(opts ...ConnOption) (*Conn, error) { panic("FIXME") }

// Close sends a CLOSE response to reject the OPEN request.
func (*IncomingConn) Close(*Error) error { panic("FIXME") }

// IncomingSession represents an incoming BEGIN request.
type IncomingSession struct {
	// FIXME ...relevant begin fields...
}

// Accept sends a BEGIN response and creates a Session.
func (*IncomingSession) Accept(...SessionOption) (*Session, error) { panic("FIXME") }

// Close sends an END response to reject the BEGIN request.
func (*IncomingSession) Close(*Error) error { panic("FIXME") }

// IncomingLink represents an incoming ATTACH request.
type IncomingLink struct {
	// FIXME ...relevant attach fields...
}

// Link is the common interface for *Sender and *Receiver
type Link interface {
	Address() string
	Close(context.Context) error
}

// Accept returns *Sender or *Receiver.
func (*IncomingLink) Accept(...LinkOption) (Link, error) { panic("FIXME") }

// Close sends a DETACH response to reject the ATTACH request.
func (*IncomingLink) Close(*Error) error { panic("FIXME") }
