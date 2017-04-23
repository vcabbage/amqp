package amqp

import (
	"bytes"
	"fmt"
)

// SASL Codes
const (
	codeSASLOK      saslCode = iota // Connection authentication succeeded.
	codeSASLAuth                    // Connection authentication failed due to an unspecified problem with the supplied credentials.
	codeSASLSys                     // Connection authentication failed due to a system error.
	codeSASLSysPerm                 // Connection authentication failed due to a system error that is unlikely to be corrected without intervention.
	codeSASLSysTemp                 // Connection authentication failed due to a transient system error.
)

type amqpType uint8

// Composite Types
const (
	typeSASLMechanism amqpType = 0x40
	typeSASLInit      amqpType = 0x41
	typeSASLChallenge amqpType = 0x42
	typeSASLResponse  amqpType = 0x43
	typeSASLOutcome   amqpType = 0x44
)

// SASL Mechanisms
const (
	saslMechanismPLAIN Symbol = "PLAIN"
)

type saslCode int

func (s *saslCode) unmarshal(r byteReader) error {
	return unmarshal(r, (*int)(s))
}

func ConnSASLPlain(username, password string) ConnOpt {
	return func(c *Conn) error {
		if c.saslHandlers == nil {
			c.saslHandlers = make(map[Symbol]stateFunc)
		}
		c.saslHandlers[saslMechanismPLAIN] = (&saslHandlerPlain{
			c:        c,
			username: username,
			password: password,
		}).init
		return nil
	}
}

type saslHandlerPlain struct {
	c        *Conn
	username string
	password string
}

func (h *saslHandlerPlain) init() stateFunc {
	saslInit, err := marshal(&saslInit{
		Mechanism:       "PLAIN",
		InitialResponse: []byte("\x00" + h.username + "\x00" + h.password),
		Hostname:        "",
	})
	if err != nil {
		h.c.err = err
		return nil
	}

	wr := bufPool.New().(*bytes.Buffer)
	wr.Reset()
	defer bufPool.Put(wr)

	writeFrame(wr, frameTypeSASL, 0, saslInit)

	fmt.Printf("Writing: %# 02x\n", wr.Bytes())

	_, err = h.c.net.Write(wr.Bytes())
	if err != nil {
		h.c.err = err
		return nil
	}

	return h.c.saslOutcome
}

/*
<type name="sasl-init" class="composite" source="list" provides="sasl-frame">
    <descriptor name="amqp:sasl-init:list" code="0x00000000:0x00000041"/>
    <field name="mechanism" type="symbol" mandatory="true"/>
    <field name="initial-response" type="binary"/>
    <field name="hostname" type="string"/>
</type>
*/
type saslInit struct {
	Mechanism       Symbol
	InitialResponse []byte
	Hostname        string
}

func (si *saslInit) marshal() ([]byte, error) {
	return marshalComposite(typeSASLInit, []field{
		{value: si.Mechanism, omit: false},
		{value: si.InitialResponse, omit: len(si.InitialResponse) == 0},
		{value: si.Hostname, omit: len(si.Hostname) == 0},
	}...)
}

/*
SASLMechanisms Frame

00 53 40 c0 0e 01 e0 0b 01 b3 00 00 00 05 50 4c 41 49 4e

0 - indicates decriptor
53 - smallulong (?)
40 - sasl-mechanisms (?)

// composites are always lists
c0 - list
0e - size 14 bytes
01 - 1 element

e0 - array
0b - size 11 bytes
01 - 1 element

b3 - sym32

00 00 00 05 - 5 charaters
50 - "P"
4c - "L"
41 - "A"
49 - "I"
4e - "N"
*/
type saslMechanisms struct {
	Mechanisms []Symbol
}

func (sm *saslMechanisms) unmarshal(r byteReader) error {
	return unmarshalComposite(r, typeSASLMechanism,
		&sm.Mechanisms,
	)
}

type saslOutcome struct {
	Code           saslCode
	AdditionalData []byte
}

func (so *saslOutcome) unmarshal(r byteReader) error {
	return unmarshalComposite(r, typeSASLOutcome,
		&so.Code,
		&so.AdditionalData,
	)
}
