// +build gofuzz

package amqp

import (
	"bytes"
	"context"
	"time"

	"pack.ag/amqp/conntest"
)

func Fuzz(data []byte) int {
	conn, err := New(conntest.New(data),
		ConnSASLPlain("listen", "3aCXZYFcuZA89xe6lZkfYJvOPnTGipA3ap7NvPruBhI="),
		ConnIdleTimeout(3*time.Millisecond),
	)
	if err != nil {
		return 0
	}
	defer conn.Close()

	s, err := conn.NewSession()
	if err != nil {
		return 0
	}

	r, err := s.NewReceiver(LinkSource("source"), LinkCredit(2))
	if err != nil {
		return 0
	}

	_, err = r.Receive(context.Background())
	if err != nil {
		return 0
	}

	return 1
}

func FuzzUnmarshal(data []byte) int {
	types := []interface{}{
		new(performAttach),
		new(performBegin),
		new(performClose),
		new(performDetach),
		new(performDisposition),
		new(performEnd),
		new(performFlow),
		new(performOpen),
		new(performTransfer),
		new(source),
		new(target),
		new(Error),
		new(saslCode),
		new(saslMechanisms),
		new(saslOutcome),
		new(Message),
		new(MessageHeader),
		new(MessageProperties),
		new(StateReceived),
		new(StateAccepted),
		new(StateRejected),
		new(StateReleased),
		new(StateModified),
		new(mapAnyAny),
		new(mapStringAny),
		new(mapSymbolAny),
		new(milliseconds),
	}

	for _, t := range types {
		unmarshal(bytes.NewBuffer(append([]byte(nil), data...)), t)
	}
	return 0
}
