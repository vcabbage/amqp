package amqp

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

type frameHeader struct {
	// size: an unsigned 32-bit integer that MUST contain the total frame size of the frame header,
	// extended header, and frame body. The frame is malformed if the size is less than the size of
	// the frame header (8 bytes).
	Size uint32
	// doff: gives the position of the body within the frame. The value of the data offset is an
	// unsigned, 8-bit integer specifying a count of 4-byte words. Due to the mandatory 8-byte
	// frame header, the frame is malformed if the value is less than 2.
	DataOffset uint8
	FrameType  uint8
	Channel    uint16
}

// Frame Types
const (
	frameTypeAMQP = 0x0
	frameTypeSASL = 0x1
)

func parseFrameHeader(r io.Reader) (frameHeader, error) {
	var fh frameHeader
	err := binary.Read(r, binary.BigEndian, &fh)
	return fh, err
}

type proto struct {
	Proto    [4]byte
	ProtoID  uint8
	Major    uint8
	Minor    uint8
	Revision uint8
}

func parseProto(r io.Reader) (proto, error) {
	var p proto
	err := binary.Read(r, binary.LittleEndian, &p)
	if err != nil {
		return p, err
	}
	if p.Proto != [4]byte{'A', 'M', 'Q', 'P'} {
		return p, fmt.Errorf("unexpected protocol %q", p.Proto)
	}
	if p.Major != 1 || p.Minor != 0 || p.Revision != 0 {
		return p, fmt.Errorf("unexpected protocol version %d.%d.%d", p.Major, p.Minor, p.Revision)
	}
	return p, nil
}

func parseFrame(r byteReader) (preformative, error) {
	pType, err := peekPerformType(r)
	if err != nil {
		return nil, err
	}

	var t preformative
	switch pType {
	case typePerformOpen:
		t = new(performOpen)
	case typePerformBegin:
		t = new(performBegin)
	case typePerformAttach:
		t = new(performAttach)
	case typePerformFlow:
		t = new(performFlow)
	case typePerformTransfer:
		t = new(performTransfer)
	case typePerformDisposition:
		t = new(performDisposition)
	case typePerformDetach:
		t = new(performDetach)
	case typePerformEnd:
		t = new(performEnd)
	case typePerformClose:
		t = new(performClose)
	case typeSASLMechanism:
		t = new(saslMechanisms)
	case typeSASLOutcome:
		t = new(saslOutcome)
	default:
		return nil, errors.Errorf("unknown preformative type %0x", pType)
	}

	err = unmarshal(r, t)
	return t, err
}

type frame struct {
	channel      uint16
	preformative preformative
}

/*
	header (8 bytes)
		0-3:	SIZE (total size, at least 8 bytes for header, uint32)
		4: 		DOFF (data offset,at least 2, count of 4 bytes words, uint8)
		5:		TYPE (frame type)
					0x0: AMQP
					0x1: SASL
		6-7:	TYPE dependent
	extended header (opt)
	body (opt)
*/
func writeFrame(wr io.Writer, frameType byte, channel uint16, data []byte) error {
	err := binary.Write(wr, binary.BigEndian, uint32(len(data)+8)) // SIZE
	if err != nil {
		return err
	}
	_, err = wr.Write([]byte{2, frameType})
	if err != nil {
		return err
	}

	err = binary.Write(wr, binary.BigEndian, channel)
	if err != nil {
		return err
	}

	_, err = wr.Write(data)
	return err
}
