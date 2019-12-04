// This example is a trivial AMQP "broker" that contains a single queue and
// accepts sender and receiver links to the queue.

package amqp_test

import (
	"context"
	"fmt"
	"net"
	"sync"

	"pack.ag/amqp"
)

func logErr(err error) bool {
	if err != nil && err != amqp.ErrConnClosed {
		fmt.Println(err)
	}
	return err != nil
}

// Queue receives messages from Receiver links, sends to Sender links.
type Queue chan *amqp.Message

// Receive accepts an incoming receiver link and receives messages on it.
func (q Queue) Receive(r *amqp.Receiver) {
	go func() { // Receive in a new goroutine
		defer r.Close(context.Background())
		for {
			m, err := r.Receive(context.Background())
			if logErr(err) {
				return
			}
			m.Accept()
			q <- m
		}
	}()
}

// Send accepts an incoming sender link and sends messages on it.
func (q Queue) Send(s *amqp.Sender) {
	go func() { // Send in a new goroutine
		defer s.Close(context.Background())
		for m := range q {
			err := s.Send(context.Background(), m)
			if logErr(err) {
				return
			}
		}
	}()
}

// Broker contains a single queue, but accepts multiple connections.
type Broker struct {
	queue    Queue
	listener net.Listener
	closed   bool
}

func NewBroker() (*Broker, error) {
	// Single queue with capacity for 100 messages
	b := &Broker{queue: Queue(make(chan *amqp.Message, 100))}
	var err error
	b.listener, err = net.Listen("tcp", ":0") // Listen on a temporary address for example
	return b, err
}

func (b *Broker) Addr() net.Addr { return b.listener.Addr() }

func (b *Broker) Run() {
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		nc, err := b.listener.Accept() // Accept a net.Conn
		if b.closed || logErr(err) {
			return
		}
		// Create an amqp.IncomingConn. This gives you access to connection
		// properties which you can use to decide to accept or reject the
		// connection.
		ic, err := amqp.NewIncoming(nc)
		if logErr(err) {
			continue
		}
		// Accept the amqp.Conn so that we can start processing it.
		c, err := ic.Accept(amqp.ConnAllowIncoming())
		if logErr(err) {
			continue
		}
		wg.Add(1)
		go func() { // goroutine per connection
			defer wg.Done()
			for {
				issn, err := c.NextIncoming() // Get next amqp.IncomingSession
				if logErr(err) {
					return
				}
				ssn, err := issn.Accept() // Accept the amqp.Session
				if logErr(err) {
					return
				}
				go func() { // goroutine per session
					for {
						ilink, err := ssn.NextIncoming()
						if logErr(err) {
							return
						}
						switch ilink := ilink.(type) {
						case *amqp.IncomingSender:
							s, err := ilink.Accept()
							if logErr(err) {
								return
							}
							b.queue.Send(s)
						case *amqp.IncomingReceiver:
							r, err := ilink.Accept(amqp.LinkCredit(10))
							if logErr(err) {
								return
							}
							b.queue.Receive(r) // Accept and process a receiver link
						}
					}
				}()
			}
		}()
	}
}

func (b *Broker) Close() {
	b.closed = true
	b.listener.Close()
}

func Example_server() {
	b, err := NewBroker()
	if logErr(err) {
		return
	}
	defer b.Close()
	go b.Run()

	// Open 2 client connections, one to send one to receive.
	c, err := amqp.Dial("amqp://" + b.Addr().String())
	if logErr(err) {
		return
	}
	defer c.Close()
	ssn, err := c.NewSession()
	if logErr(err) {
		return
	}
	s, err := ssn.NewSender()
	if logErr(err) {
		return
	}

	c, err = amqp.Dial("amqp://" + b.Addr().String())
	if logErr(err) {
		return
	}
	defer c.Close()
	ssn, err = c.NewSession()
	if logErr(err) {
		return
	}
	r, err := ssn.NewReceiver()
	if logErr(err) {
		return
	}

	for _, str := range []string{"a", "b", "c", "d"} {
		err := s.Send(context.Background(), &amqp.Message{Value: str})
		if logErr(err) {
			return
		}
		m, err := r.Receive(context.Background())
		if logErr(err) {
			return
		}
		err = m.Accept()
		if logErr(err) {
			return
		}
		fmt.Println(m.Value)
	}
	// OUTPUT:
	// a
	// b
	// c
	// d
}
