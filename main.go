package main

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"sync"
	"unicode/utf8"
)

func main() {
	conn, err := New("amqp://10.211.55.4:5672/", OptSASLPlain("guest", "guest"))
	if err != nil {
		fmt.Printf("Error: %v", err)
		os.Exit(1)
	}
	defer conn.Close()
}

// connection defaults
const (
	initialMaxFrameSize = 512
)

type Conn struct {
	url *url.URL
	net net.Conn

	maxFrameSize int

	rxBuf []byte
	err   error

	// SASL
	saslHandlers map[Symbol]stateFunc
	saslComplete bool
}

func New(addr string, opts ...Opt) (*Conn, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	conn := &Conn{
		url:          u,
		maxFrameSize: initialMaxFrameSize,
	}

	for _, opt := range opts {
		err = opt(conn)
		if err != nil {
			return nil, err
		}
	}

	for state := conn.connect; state != nil; {
		state = state()
	}

	if conn.err != nil && conn.net != nil {
		conn.net.Close()
	}

	return conn, conn.err
}

func (c *Conn) Close() error {
	// TODO: shutdown AMQP
	return c.net.Close()
}

/*
On connection open, we'll need to handle 4 possible scenarios:
1. Straight into AMQP.
2. SASL -> AMQP.
3. TLS -> AMQP.
4. TLS -> SASL -> AMQP
*/
func (c *Conn) connect() stateFunc {
	c.net, c.err = net.Dial("tcp", c.url.Host)
	if c.err != nil {
		return nil
	}
	return c.negotiateProto
}

func (c *Conn) negotiateProto() stateFunc {
	// TODO: technically we should be sending a protocol header first,
	//       but Microsoft is fine with this for now

	if c.saslHandlers != nil && !c.saslComplete {
		_, c.err = c.net.Write([]byte{'A', 'M', 'Q', 'P', ProtoSASL, 1, 0, 0})
		if c.err != nil {
			return nil
		}
	}

	c.rxBuf = make([]byte, c.maxFrameSize)
	n, err := c.net.Read(c.rxBuf)
	if err != nil {
		c.err = err
		return nil
	}

	fmt.Printf("Read %d bytes.\n", n)

	p, err := parseProto(c.rxBuf[:n])
	if err != nil {
		c.err = err
		return nil
	}

	fmt.Printf("Proto: %s; ProtoID: %d; Version: %d.%d.%d\n",
		p.proto,
		p.protoID,
		p.major,
		p.minor,
		p.revision,
	)

	switch p.protoID {
	case ProtoAMQP:
		// TODO
		return nil
	case ProtoTLS:
		// TODO
		return nil
	case ProtoSASL:
		return c.protoSASL
	default:
		c.err = fmt.Errorf("unknown protocol ID %#02x", p.protoID)
		return nil
	}
}

func (c *Conn) protoSASL() stateFunc {
	if c.saslHandlers == nil {
		// we don't support SASL
		c.err = fmt.Errorf("server request SASL, but not configured")
		return nil
	}

	n, err := c.net.Read(c.rxBuf)
	if err != nil {
		c.err = err
		return nil
	}

	fh, err := parseFrameHeader(c.rxBuf[:n])
	if err != nil {
		c.err = err
		return nil
	}

	if fh.frameType != FrameSASL {
		c.err = fmt.Errorf("unexpected frame type %#02x", fh.frameType)
	}

	var sm SASLMechanisms
	err = Unmarshal(c.rxBuf[fh.dataOffsetBytes():n], &sm)
	if err != nil {
		c.err = err
		return nil
	}

	for _, mech := range sm.Mechanisms {
		if state, ok := c.saslHandlers[mech]; ok {
			return state
		}
	}

	// TODO: send some sort of "auth not supported" frame?
	c.err = fmt.Errorf("no supported auth mechanism (%v)", sm.Mechanisms)
	return nil
}

func (c *Conn) saslOutcome() stateFunc {
	// TODO: implement
	return nil
}

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

const (
	FrameAMQP = 0x0
	FrameSASL = 0x1
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

// ProtoIDs
const (
	ProtoAMQP = 0x1
	ProtoTLS  = 0x2
	ProtoSASL = 0x3
)

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

type Opt func(*Conn) error

// SASL Mechanisms
const (
	SASLMechanismPLAIN Symbol = "PLAIN"
)

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

	writeFrame(wr, FrameSASL, 0, saslInit)

	fmt.Printf("Writing: %# 02x\n", wr.Bytes())

	_, err = h.c.net.Write(wr.Bytes())
	if err != nil {
		h.c.err = err
		return nil
	}

	return h.c.saslOutcome
}

type stateFunc func() stateFunc

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

type byteReader interface {
	io.Reader
	io.ByteReader
}

type byteWriter interface {
	io.Writer
	io.ByteWriter
}

type Type uint8

// Composite Types
const (
	TypeSASLMechanism Type = 0x40
	TypeSASLInit      Type = 0x41
	TypeSASLChallenge Type = 0x42
	TypeSASLResponse  Type = 0x43
	TypeSASLOutcome   Type = 0x44
)

// SASL Codes
const (
	CodeSASLOK      = 0 // Connection authentication succeeded.
	CodeSASLAuth    = 1 // Connection authentication failed due to an unspecified problem with the supplied credentials.
	CodeSASLSys     = 2 // Connection authentication failed due to a system error.
	CodeSASLSysPerm = 3 // Connection authentication failed due to a system error that is unlikely to be corrected without intervention.
	CodeSASLSysTemp = 4 // Connection authentication failed due to a transient system error.
)

/*
<type name="sasl-init" class="composite" source="list" provides="sasl-frame">
    <descriptor name="amqp:sasl-init:list" code="0x00000000:0x00000041"/>
    <field name="mechanism" type="symbol" mandatory="true"/>
    <field name="initial-response" type="binary"/>
    <field name="hostname" type="string"/>
</type>
*/

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

type Symbol string

func (s Symbol) MarshalBinary() ([]byte, error) {
	l := len(s)

	buf := bufPool.New().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	var err error
	switch {
	// List8
	case l < 256:
		_, err = buf.Write(append([]byte{Sym8, uint8(l)}, []byte(s)...))

	// List32
	case l < math.MaxUint32:
		err = binary.Write(buf, binary.BigEndian, uint32(l))
		if err != nil {
			return nil, err
		}
		_, err = buf.Write([]byte(s))
	default:
		return nil, errors.New("too long")
	}

	return append([]byte(nil), buf.Bytes()...), err
}

func Marshal(i interface{}) ([]byte, error) {
	if bm, ok := i.(encoding.BinaryMarshaler); ok {
		return bm.MarshalBinary()
	}

	buf := bufPool.New().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	var err error
	switch t := i.(type) {
	case string:
		err = writeString(buf, t)
	case []byte:
		err = writeBinary(buf, t)
	default:
		return nil, fmt.Errorf("marshal not implemented for %T", i)
	}
	return append([]byte(nil), buf.Bytes()...), err
}

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

func writeString(wr byteWriter, str string) error {
	if !utf8.ValidString(str) {
		return errors.New("not a valid UTF-8 string")
	}
	l := len(str)

	switch {
	// Str8
	case l < 256:
		_, err := wr.Write(append([]byte{Str8, uint8(l)}, []byte(str)...))
		return err

	// Str32
	case l < math.MaxUint32:
		wr.WriteByte(Str32)
		err := binary.Write(wr, binary.BigEndian, uint32(l))
		if err != nil {
			return err
		}
		_, err = wr.Write([]byte(str))
		return err

	default:
		return errors.New("too long")
	}
}

func writeBinary(wr byteWriter, bin []byte) error {
	l := len(bin)

	switch {
	// List8
	case l < 256:
		_, err := wr.Write(append([]byte{Vbin8, uint8(l)}, bin...))
		return err

	// List32
	case l < math.MaxUint32:
		wr.WriteByte(Vbin32)
		err := binary.Write(wr, binary.BigEndian, uint32(l))
		if err != nil {
			return err
		}
		_, err = wr.Write(bin)
		return err

	default:
		return errors.New("too long")
	}
}

func writeComposite(wr byteWriter, code Type, fields ...[]byte) error {
	_, err := wr.Write([]byte{0x0, Smallulong, uint8(code)})
	if err != nil {
		return err
	}

	return writeList(wr, fields...)
}

func writeList(wr byteWriter, fields ...[]byte) error {
	var size int
	for _, field := range fields {
		size += len(field)
	}

	l := len(fields)
	switch {
	// List0
	case l == 0:
		return wr.WriteByte(List0)

	// List8
	case l < 256 && size < 256:
		_, err := wr.Write([]byte{List8, uint8(size + 1), uint8(l)})
		if err != nil {
			return err
		}

	// List32
	case l < math.MaxUint32 && size < math.MaxUint32:
		wr.WriteByte(List32)
		err := binary.Write(wr, binary.BigEndian, uint32(size+4))
		if err != nil {
			return err
		}
		err = binary.Write(wr, binary.BigEndian, uint32(l))
		if err != nil {
			return err
		}

	default:
		return errors.New("too many fields")
	}

	// Write fields
	for _, field := range fields {
		_, err := wr.Write(field)
		if err != nil {
			return err
		}
	}

	return nil
}

func Unmarshal(b []byte, i interface{}) error {
	r := bytes.NewReader(b)

	if um, ok := i.(encoding.BinaryUnmarshaler); ok {
		return um.UnmarshalBinary(b)
	}

	switch t := i.(type) {
	case *int:
		val, err := readInt(bytes.NewReader(b))
		if err != nil {
			return err
		}
		*t = val
	case *[]Symbol:
		sa, err := readSymbolArray(r)
		if err != nil {
			return err
		}
		*t = sa
	}
	return nil
}

type SASLMechanisms struct {
	Mechanisms []Symbol
}

func (sm *SASLMechanisms) UnmarshalBinary(b []byte) (err error) {
	r := bytes.NewBuffer(b)

	t, _, err := readCompositeHeader(r)
	if err != nil {
		return err
	}

	if t != TypeSASLMechanism {
		return errors.New("invalid header for SASL mechanisms")
	}

	err = Unmarshal(r.Bytes(), &sm.Mechanisms)
	return err
}

func readCompositeHeader(r byteReader) (_ Type, fields int, _ error) {
	byt, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	if byt != 0 {
		return 0, 0, errors.New("invalid composite header")
	}

	v, err := readInt(r)
	if err != nil {
		return 0, 0, err
	}

	fields, _, err = readSlice(r)

	return Type(v), fields, err
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

func readStringArray(r byteReader) ([]string, error) {
	lElems, _, err := readSlice(r)
	if err != nil {
		return nil, err
	}

	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	var strs []string
	for i := 0; i < lElems; i++ {
		vari, err := readVariableType(r, b)
		if err != nil {
			return nil, err
		}

		strs = append(strs, string(vari))
	}
	return strs, nil
}

func readSymbolArray(r byteReader) ([]Symbol, error) {
	lElems, _, err := readSlice(r)
	if err != nil {
		return nil, err
	}

	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	var strs []Symbol
	for i := 0; i < lElems; i++ {
		vari, err := readVariableType(r, b)
		if err != nil {
			return nil, err
		}

		strs = append(strs, Symbol(vari))
	}
	return strs, nil
}

func readVariableType(r byteReader, t byte) ([]byte, error) {
	var buf []byte
	switch t {
	case Vbin8, Str8, Sym8:
		n, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		buf = make([]byte, int(n))
	case Vbin32, Str32, Sym32:
		var n uint32
		err := binary.Read(r, binary.BigEndian, &n)
		if err != nil {
			return nil, err
		}
		buf = make([]byte, int(n))
	default:
		return nil, fmt.Errorf("type code %x is not a recognized variable length type", t)
	}
	_, err := io.ReadFull(r, buf)
	return buf, err
}

func readSlice(r byteReader) (elements int, length int, _ error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	switch b {
	case List0:
		return 0, 0, nil
	case List8, Array8:
		lByte, err := r.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		elemByte, err := r.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		return int(elemByte), int(lByte), nil
	case List32, Array32:
		var elems uint32
		var l uint32
		err = binary.Read(r, binary.BigEndian, &l)
		if err != nil {
			return 0, 0, err
		}
		err = binary.Read(r, binary.BigEndian, &elems)
		if err != nil {
			return 0, 0, err
		}
		return int(elems), int(l), nil
	default:
		return 0, 0, fmt.Errorf("type code %x is not a recognized list type", b)
	}
}

// Type codes
const (
	Null = 0x40

	// Unsigned
	Ubyte      = 0x50 // 8-bit unsigned integer (1)
	Ushort     = 0x60 // 16-bit unsigned integer in network byte order (2)
	Uint       = 0x70 // 32-bit unsigned integer in network byte order (4)
	Smalluint  = 0x52 // unsigned integer value in the range 0 to 255 inclusive (1)
	Uint0      = 0x43 // the uint value 0 (0)
	Ulong      = 0x80 // 64-bit unsigned integer in network byte order (8)
	Smallulong = 0x53 // unsigned long value in the range 0 to 255 inclusive (1)
	Ulong0     = 0x44 // the ulong value 0 (0)

	// Signed
	Byte      = 0x51 // 8-bit two's-complement integer (1)
	Short     = 0x61 // 16-bit two's-complement integer in network byte order (2)
	Int       = 0x71 // 32-bit two's-complement integer in network byte order (4)
	Smallint  = 0x54 // 8-bit two's-complement integer (1)
	Long      = 0x81 // 64-bit two's-complement integer in network byte order (8)
	Smalllong = 0x55 // 8-bit two's-complement integer

	// Decimal
	Float      = 0x72 // IEEE 754-2008 binary32 (4)
	Double     = 0x82 // IEEE 754-2008 binary64 (8)
	Decimal32  = 0x74 // IEEE 754-2008 decimal32 using the Binary Integer Decimal encoding (4)
	Decimal64  = 0x84 // IEEE 754-2008 decimal64 using the Binary Integer Decimal encoding (8)
	Decimal128 = 0x94 // IEEE 754-2008 decimal128 using the Binary Integer Decimal encoding (16)

	// Other
	Char      = 0x73 // a UTF-32BE encoded Unicode character (4)
	Timestamp = 0x83 // 64-bit two's-complement integer representing milliseconds since the unix epoc
	UUID      = 0x98 // UUID as defined in section 4.1.2 of RFC-4122

	// Variable Length
	Vbin8  = 0xa0 // up to 2^8 - 1 octets of binary data (1 + variable)
	Vbin32 = 0xb0 // up to 2^32 - 1 octets of binary data (4 + variable)
	Str8   = 0xa1 // up to 2^8 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (1 + variable)
	Str32  = 0xb1 // up to 2^32 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (4 +variable)
	Sym8   = 0xa3 // up to 2^8 - 1 seven bit ASCII characters representing a symbolic value (1 + variable)
	Sym32  = 0xb3 // up to 2^32 - 1 seven bit ASCII characters representing a symbolic value (4 + variable)

	// Compound
	List0   = 0x45 // the empty list (i.e. the list with no elements) (0)
	List8   = 0xc0 // up to 2^8 - 1 list elements with total size less than 2^8 octets (1 + compound)
	List32  = 0xd0 // up to 2^32 - 1 list elements with total size less than 2^32 octets (4 + compound)
	Map8    = 0xc1 // up to 2^8 - 1 octets of encoded map data (1 + compound)
	Map32   = 0xd1 // up to 2^32 - 1 octets of encoded map data (4 + compound)
	Array8  = 0xe0 // up to 2^8 - 1 array elements with total size less than 2^8 octets (1 + array)
	Array32 = 0xf0 // up to 2^32 - 1 array elements with total size less than 2^32 octets (4 + array)
)

func readInt(r byteReader) (value int, _ error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	switch b {
	// Unsigned
	case Uint0, Ulong0:
		return 0, nil
	case Ubyte, Smalluint, Smallulong:
		n, err := r.ReadByte()
		return int(n), err
	case Ushort:
		var n uint16
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	case Uint:
		var n uint32
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	case Ulong:
		var n uint64
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err

	// Signed
	case Byte, Smallint, Smalllong:
		var n int8
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	case Short:
		var n int16
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	case Int:
		var n int32
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	case Long:
		var n int64
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	default:
		return 0, fmt.Errorf("type code %x is not a recognized number type", b)
	}
}

func checkLen(b []byte, n int) error {
	if len(b) < n {
		return fmt.Errorf("len must be at least %d", n)
	}
	return nil
}
