package amqp

import (
	"encoding/binary"
	"io"
	"math"
)

// Frame structure:
//
//     header (8 bytes)
//       0-3: SIZE (total size, at least 8 bytes for header, uint32)
//       4:   DOFF (data offset,at least 2, count of 4 bytes words, uint8)
//       5:   TYPE (frame type)
//                0x0: AMQP
//                0x1: SASL
//       6-7: type dependent (channel for AMQP)
//     extended header (opt)
//     body (opt)

// frameHeader in a structure appropriate for use with binary.Read()
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

const (
	frameTypeAMQP = 0x0
	frameTypeSASL = 0x1

	frameHeaderSize = 8
)

// parseFrameHeader reads the header from r and returns the result.
//
// No validation is done.
func parseFrameHeader(r io.Reader) (frameHeader, error) {
	var fh frameHeader
	err := binary.Read(r, binary.BigEndian, &fh)
	return fh, err
}

// protoHeader in a structure appropriate for use with binary.Read()
type protoHeader struct {
	Proto    [4]byte
	ProtoID  uint8
	Major    uint8
	Minor    uint8
	Revision uint8
}

// parseProtoHeader reads the proto header from r and returns the results
//
// An error is returned if the protocol is not "AMQP" or if the version is not 1.0.0.
func parseProtoHeader(r io.Reader) (protoHeader, error) {
	var p protoHeader
	err := binary.Read(r, binary.LittleEndian, &p)
	if err != nil {
		return p, err
	}
	if p.Proto != [4]byte{'A', 'M', 'Q', 'P'} {
		return p, errorErrorf("unexpected protocol %q", p.Proto)
	}
	if p.Major != 1 || p.Minor != 0 || p.Revision != 0 {
		return p, errorErrorf("unexpected protocol version %d.%d.%d", p.Major, p.Minor, p.Revision)
	}
	return p, nil
}

// parseFrame reads and unmarshals an AMQP frame.
func parseFrame(r byteReader) (frameBody, error) {
	pType, err := peekPerformType(r)
	if err != nil {
		return nil, err
	}

	var t frameBody
	switch pType {
	case typeCodeOpen:
		t = new(performOpen)
	case typeCodeBegin:
		t = new(performBegin)
	case typeCodeAttach:
		t = new(performAttach)
	case typeCodeFlow:
		t = new(performFlow)
	case typeCodeTransfer:
		t = new(performTransfer)
	case typeCodeDisposition:
		t = new(performDisposition)
	case typeCodeDetach:
		t = new(performDetach)
	case typeCodeEnd:
		t = new(performEnd)
	case typeCodeClose:
		t = new(performClose)
	case typeCodeSASLMechanism:
		t = new(saslMechanisms)
	case typeCodeSASLOutcome:
		t = new(saslOutcome)
	default:
		return nil, errorErrorf("unknown preformative type %0x", pType)
	}

	_, err = unmarshal(r, t)
	return t, err
}

type frame struct {
	typ     uint8     // AMQP/SASL
	channel uint16    // channel this frame is for
	body    frameBody // body of the frame
}

// frameBody is the interface all frame bodies must implement
type frameBody interface {
	// if the frame is for a link, link() should return (link#, true),
	// otherwise it should return (0, false)
	link() (handle uint32, ok bool)
}

// writeFrame encodes and writes fr to wr.
func writeFrame(wr io.Writer, fr frame) error {
	data, err := marshal(fr.body)
	if err != nil {
		return err
	}

	frameSize := len(data) + frameHeaderSize
	if frameSize > math.MaxUint32 {
		return errorNew("frame too large")
	}

	header := frameHeader{
		Size:       uint32(frameSize),
		DataOffset: 2, // see frameHeader.DataOffset comment
		FrameType:  fr.typ,
		Channel:    fr.channel,
	}

	err = binary.Write(wr, binary.BigEndian, header)
	if err != nil {
		return err
	}

	_, err = wr.Write(data)
	return err
}
