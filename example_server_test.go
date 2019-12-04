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

// Trivial error handling for the example: exit on error.
func check(err error) {
	if err != nil {
		panic(err)
	}
}

// Queue receives messages from Receiver links, sends to Sender links.
type Queue chan *amqp.Message

// Receive accepts an incoming receiver link and receives messages on it.
func (q Queue) Receive(r *amqp.Receiver) {
	go func() { // Receive in a new goroutine
		defer r.Close(context.Background())
		for {
			m, err := r.Receive(context.Background())
			check(err)
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
			check(err)
		}
	}()
}

// Broker contains a single queue, but accepts multiple connections.
type Broker struct {
	queue    Queue
	listener net.Listener
}

func NewBroker() *Broker {
	// Single queue with capacity for 100 messages
	b := &Broker{queue: Queue(make(chan *amqp.Message, 100))}
	var err error
	b.listener, err = net.Listen("tcp", ":0") // Listen on a temporary address for example
	check(err)
	return b
}

func (b *Broker) Addr() net.Addr { return b.listener.Addr() }

func (b *Broker) Run() {
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		nc, err := b.listener.Accept() // Accept a net.Conn
		check(err)
		// Create an amqp.IncomingConn. This gives you access to connection
		// properties which you can use to decide to accept or reject the
		// connection.
		ic, err := amqp.NewIncoming(nc)
		check(err)
		// Accept the amqp.Conn so that we can start processing it.
		c, err := ic.Accept(amqp.ConnAllowIncoming())
		check(err)
		wg.Add(1)
		go func() { // goroutine per connection
			defer wg.Done()
			for {
				issn, err := c.NextIncoming() // Get next amqp.IncomingSession
				check(err)
				ssn, err := issn.Accept() // Accept the amqp.Session
				check(err)
				go func() { // goroutine per session
					for {
						ilink, err := ssn.NextIncoming() // Get next amqp.IncomingLink
						check(err)
						link, err := ilink.Accept()
						check(err)
						switch l := link.(type) {
						case *amqp.Sender:
							b.queue.Send(l) // Accept and process a sender link
						case *amqp.Receiver:
							b.queue.Receive(l) // Accept and process a receiver link
						}
					}
				}()
			}
		}()
	}
}

func Example_server() {
	b := NewBroker()
	go b.Run()

	// Open 2 client connections, one to send one to receive.
	c, err := amqp.Dial("amqp://" + b.Addr().String())
	check(err)
	defer c.Close()
	ssn, err := c.NewSession()
	check(err)
	s, err := ssn.NewSender()
	check(err)

	c, err = amqp.Dial("amqp://" + b.Addr().String())
	check(err)
	defer c.Close()
	ssn, err = c.NewSession()
	check(err)
	r, err := ssn.NewReceiver()
	check(err)

	for _, str := range []string{"a", "b", "c", "d"} {
		check(s.Send(context.Background(), &amqp.Message{Value: str}))
		m, err := r.Receive(context.Background())
		check(err)
		fmt.Println(m.Value)
	}
	// OUTPUT:
	// a
	// b
	// c
	// d

	// FIXME don't run example yet, server/incoming not implemented.
}
