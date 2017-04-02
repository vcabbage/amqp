package amqp

import (
	"bytes"
	"errors"
	"fmt"
)

// SASL Codes
const (
	CodeSASLOK      SASLCode = iota // Connection authentication succeeded.
	CodeSASLAuth                    // Connection authentication failed due to an unspecified problem with the supplied credentials.
	CodeSASLSys                     // Connection authentication failed due to a system error.
	CodeSASLSysPerm                 // Connection authentication failed due to a system error that is unlikely to be corrected without intervention.
	CodeSASLSysTemp                 // Connection authentication failed due to a transient system error.
)

type Type uint8

// Composite Types
const (
	TypeSASLMechanism Type = 0x40
	TypeSASLInit      Type = 0x41
	TypeSASLChallenge Type = 0x42
	TypeSASLResponse  Type = 0x43
	TypeSASLOutcome   Type = 0x44
)

// SASL Mechanisms
const (
	SASLMechanismPLAIN Symbol = "PLAIN"
)

type SASLCode int

func (s *SASLCode) UnmarshalBinary(r byteReader) error {
	return Unmarshal(r, (*int)(s))
}

func OptSASLPlain(username, password string) Opt {
	return func(c *Conn) error {
		if c.saslHandlers == nil {
			c.saslHandlers = make(map[Symbol]stateFunc)
		}
		c.saslHandlers[SASLMechanismPLAIN] = (&saslHandlerPlain{
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
	saslInit, err := SASLInitPlain(h.username, h.password, "")
	if err != nil {
		h.c.err = err
		return nil
	}

	wr := bufPool.New().(*bytes.Buffer)
	wr.Reset()
	defer bufPool.Put(wr)

	writeFrame(wr, FrameTypeSASL, 0, saslInit)

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
type SASLInit struct {
	Mechanism       Symbol
	InitialResponse []byte
	Hostname        string
}

func (si *SASLInit) MarshalBinary() ([]byte, error) {
	mechanism, err := Marshal(si.Mechanism)
	if err != nil {
		return nil, err
	}

	initResponse, err := Marshal(si.InitialResponse)
	if err != nil {
		return nil, err
	}

	fields := [][]byte{
		mechanism,
		initResponse,
	}

	if si.Hostname != "" {
		hostname, err := Marshal(si.Hostname)
		if err != nil {
			return nil, err
		}
		fields = append(fields, hostname)
	}

	buf := bufPool.New().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	err = writeComposite(buf, TypeSASLInit, fields...)
	if err != nil {
		return nil, err
	}

	return append([]byte(nil), buf.Bytes()...), nil
}

func SASLInitPlain(username, password, hostname string) ([]byte, error) {
	return Marshal(&SASLInit{
		Mechanism:       "PLAIN",
		InitialResponse: []byte("\x00" + username + "\x00" + password),
		Hostname:        hostname,
	})
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
type SASLMechanisms struct {
	Mechanisms []Symbol
}

func (sm *SASLMechanisms) UnmarshalBinary(r byteReader) error {
	t, _, err := readCompositeHeader(r)
	if err != nil {
		return err
	}

	if t != TypeSASLMechanism {
		return errors.New("invalid header for SASL mechanisms")
	}

	err = Unmarshal(r, &sm.Mechanisms)
	return err
}

type SASLOutcome struct {
	Code           SASLCode
	AdditionalData []byte
}

func (so *SASLOutcome) UnmarshalBinary(r byteReader) error {
	t, fields, err := readCompositeHeader(r)
	if err != nil {
		return err
	}

	if t != TypeSASLOutcome {
		return errors.New("invalid header for SASL outcode")
	}

	err = Unmarshal(r, &so.Code)
	if err != nil {
		return err
	}

	if fields > 1 {
		err = Unmarshal(r, &so.AdditionalData)
		if err != nil {
			return err
		}
	}

	return nil
}
