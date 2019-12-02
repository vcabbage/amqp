// +build local

package amqp_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pack.ag/amqp"
)

// Tests that require a local broker running on the standard AMQP port.

func TestDial_IPV6(t *testing.T) {
	c, err := amqp.Dial("amqp://localhost")
	assert.NoError(t, err)
	c.Close()

	l, err := net.Listen("tcp6", "[::]:0")
	if err != nil {
		t.Skip("ipv6 not supported")
	}
	l.Close()

	for _, u := range []string{"amqp://[::]:5672", "amqp://[::]"} {
		u := u // Don't  use range variable in func literal.
		t.Run(u, func(t *testing.T) {
			c, err := amqp.Dial(u)
			if err != nil {
				t.Errorf("%q: %v", u, err)
			} else {
				c.Close()
			}
		})
	}
}

func TestSendReceive(t *testing.T) {
	c, err := amqp.Dial("amqp://")
	require.NoError(t, err)
	defer c.Close()

	ssn, err := c.NewSession()
	require.NoError(t, err)

	r, err := ssn.NewReceiver(amqp.LinkAddressDynamic())
	require.NoError(t, err)
	var m *amqp.Message
	done := make(chan error)
	go func() {
		var err error
		defer func() { done <- err; close(done) }()
		m, err = r.Receive(context.Background())
		m.Accept()
	}()

	s, err := ssn.NewSender(amqp.LinkAddress(r.Address()))
	require.NoError(t, s.Send(context.Background(), amqp.NewMessage([]byte("hello"))))
	require.NoError(t, <-done)
	assert.Equal(t, "hello", string(m.GetData()))
}
