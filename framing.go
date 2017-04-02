package amqp

import (
	"encoding/binary"
	"fmt"
)

type frameHeader struct {
	// size: an unsigned 32-bit integer that MUST contain the total frame size of the frame header,
	// extended header, and frame body. The frame is malformed if the size is less than the size of
	// the frame header (8 bytes).
	size uint32
	// doff: gives the position of the body within the frame. The value of the data offset is an
	// unsigned, 8-bit integer specifying a count of 4-byte words. Due to the mandatory 8-byte
	// frame header, the frame is malformed if the value is less than 2.
	dataOffset uint8
	frameType  uint8
	channel    uint16
}

func (fh frameHeader) dataOffsetBytes() int {
	return int(fh.dataOffset) * 4
}

// Frame Types
const (
	FrameTypeAMQP = 0x0
	FrameTypeSASL = 0x1
)

func parseFrameHeader(buf []byte) (frameHeader, error) {
	var fh frameHeader

	if len(buf) < 8 {
		return fh, fmt.Errorf("frame size %d, must be at least 8 bytes", len(buf))
	}

	fh.size = binary.BigEndian.Uint32(buf)
	fh.dataOffset = buf[4]
	fh.frameType = buf[5]
	fh.channel = binary.BigEndian.Uint16(buf[6:])

	return fh, nil
}

type proto struct {
	proto string

	// 0: AMQP
	// 2: TLS -> tls.Conn -> AMQP
	// 3: SASL
	protoID  uint8
	major    uint8
	minor    uint8
	revision uint8
}

func parseProto(buf []byte) (proto, error) {
	if len(buf) != 8 {
		return proto{}, fmt.Errorf("expected protocol header to be 8 bytes, not %d", len(buf))
	}
	p := proto{
		proto:    string(buf[:4]),
		protoID:  buf[4],
		major:    buf[5],
		minor:    buf[6],
		revision: buf[7],
	}
	if p.proto != "AMQP" {
		return p, fmt.Errorf("unexpected protocol %q", p.proto)
	}

	if p.major != 1 || p.minor != 0 || p.revision != 0 {
		return p, fmt.Errorf("unexpected protocol version %d.%d.%d", p.major, p.minor, p.revision)
	}

	return p, nil
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
func writeFrame(wr byteWriter, frameType byte, channel uint16, data []byte) error {
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
