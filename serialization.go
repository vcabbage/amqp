package amqp

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"
	"unicode/utf8"
)

type byteReader interface {
	io.Reader
	io.ByteReader
}

type byteWriter interface {
	io.Writer
	io.ByteWriter
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

type unmarshaler interface {
	UnmarshalBinary(r byteReader) error
}

func Unmarshal(r byteReader, i interface{}) error {
	if um, ok := i.(unmarshaler); ok {
		return um.UnmarshalBinary(r)
	}

	switch t := i.(type) {
	case *int:
		val, err := readInt(r)
		if err != nil {
			return err
		}
		*t = val
	case *uint32:
		val, err := readUint(r)
		if err != nil {
			return err
		}
		*t = uint32(val)
	case *uint16:
		val, err := readUint(r)
		if err != nil {
			return err
		}
		*t = uint16(val)
	case *string:
		val, err := readString(r)
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
	default:
		return fmt.Errorf("unable to unmarshal %T", i)
	}
	return nil
}

func writeSymbolArray(w byteWriter, symbols []Symbol) error {
	ofType := Sym8
	for _, symbol := range symbols {
		if len(symbol) > math.MaxUint8 {
			ofType = Sym32
			break
		}
	}

	buf := bufPool.New().(*bytes.Buffer)
	defer bufPool.Put(buf)

	var elems [][]byte
	for _, symbol := range symbols {
		buf.Reset()
		err := writeSymbol(buf, symbol, ofType)
		if err != nil {
			return err
		}

		elems = append(elems, append([]byte(nil), buf.Bytes()...))
	}

	return writeArray(w, ofType, elems...)
}

func writeSymbol(wr byteWriter, sym Symbol, typ uint8) error {
	if !utf8.ValidString(string(sym)) {
		return errors.New("not a valid UTF-8 string")
	}

	l := len(sym)

	switch typ {
	case Sym8:
		wr.WriteByte(uint8(l))
	case Sym32:
		err := binary.Write(wr, binary.BigEndian, uint32(l))
		if err != nil {
			return err
		}
	default:
		return errors.New("invalid symbol type")
	}
	_, err := wr.Write([]byte(sym))
	return err
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

func writeArray(wr byteWriter, ofType uint8, fields ...[]byte) error {
	const isArray = true
	return writeSlice(wr, isArray, ofType, fields...)
}

func writeList(wr byteWriter, fields ...[]byte) error {
	const isArray = false
	return writeSlice(wr, isArray, 0, fields...)
}

func writeSlice(wr byteWriter, isArray bool, arrayType uint8, fields ...[]byte) error {
	var size int
	for _, field := range fields {
		size += len(field)
	}

	size8 := List8
	size32 := List32
	if isArray {
		size8 = Array8
		size32 = Array32
	}

	switch l := len(fields); {
	// List0
	case l == 0:
		if isArray {
			return errors.New("invalid array length 0")
		}
		return wr.WriteByte(List0)

	// List8
	case l < 256 && size < 256:
		_, err := wr.Write([]byte{size8, uint8(size + 1), uint8(l)})
		if err != nil {
			return err
		}

	// List32
	case l < math.MaxUint32 && size < math.MaxUint32:
		err := wr.WriteByte(size32)
		if err != nil {
			return err
		}
		err = binary.Write(wr, binary.BigEndian, uint32(size+4))
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

	if isArray {
		err := wr.WriteByte(arrayType)
		if err != nil {
			return err
		}
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

func readString(r byteReader) (string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return "", err
	}

	vari, err := readVariableType(r, b)
	return string(vari), err
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
		return nil, fmt.Errorf("type code %#00x is not a recognized variable length type", t)
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
	Null uint8 = 0x40

	// Unsigned
	Ubyte      uint8 = 0x50 // 8-bit unsigned integer (1)
	Ushort     uint8 = 0x60 // 16-bit unsigned integer in network byte order (2)
	Uint       uint8 = 0x70 // 32-bit unsigned integer in network byte order (4)
	Smalluint  uint8 = 0x52 // unsigned integer value in the range 0 to 255 inclusive (1)
	Uint0      uint8 = 0x43 // the uint value 0 (0)
	Ulong      uint8 = 0x80 // 64-bit unsigned integer in network byte order (8)
	Smallulong uint8 = 0x53 // unsigned long value in the range 0 to 255 inclusive (1)
	Ulong0     uint8 = 0x44 // the ulong value 0 (0)

	// Signed
	Byte      uint8 = 0x51 // 8-bit two's-complement integer (1)
	Short     uint8 = 0x61 // 16-bit two's-complement integer in network byte order (2)
	Int       uint8 = 0x71 // 32-bit two's-complement integer in network byte order (4)
	Smallint  uint8 = 0x54 // 8-bit two's-complement integer (1)
	Long      uint8 = 0x81 // 64-bit two's-complement integer in network byte order (8)
	Smalllong uint8 = 0x55 // 8-bit two's-complement integer

	// Decimal
	Float      uint8 = 0x72 // IEEE 754-2008 binary32 (4)
	Double     uint8 = 0x82 // IEEE 754-2008 binary64 (8)
	Decimal32  uint8 = 0x74 // IEEE 754-2008 decimal32 using the Binary Integer Decimal encoding (4)
	Decimal64  uint8 = 0x84 // IEEE 754-2008 decimal64 using the Binary Integer Decimal encoding (8)
	Decimal128 uint8 = 0x94 // IEEE 754-2008 decimal128 using the Binary Integer Decimal encoding (16)

	// Other
	Char      uint8 = 0x73 // a UTF-32BE encoded Unicode character (4)
	Timestamp uint8 = 0x83 // 64-bit two's-complement integer representing milliseconds since the unix epoc
	UUID      uint8 = 0x98 // UUID as defined in section 4.1.2 of RFC-4122

	// Variable Length
	Vbin8  uint8 = 0xa0 // up to 2^8 - 1 octets of binary data (1 + variable)
	Vbin32 uint8 = 0xb0 // up to 2^32 - 1 octets of binary data (4 + variable)
	Str8   uint8 = 0xa1 // up to 2^8 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (1 + variable)
	Str32  uint8 = 0xb1 // up to 2^32 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (4 +variable)
	Sym8   uint8 = 0xa3 // up to 2^8 - 1 seven bit ASCII characters representing a symbolic value (1 + variable)
	Sym32  uint8 = 0xb3 // up to 2^32 - 1 seven bit ASCII characters representing a symbolic value (4 + variable)

	// Compound
	List0   uint8 = 0x45 // the empty list (i.e. the list with no elements) (0)
	List8   uint8 = 0xc0 // up to 2^8 - 1 list elements with total size less than 2^8 octets (1 + compound)
	List32  uint8 = 0xd0 // up to 2^32 - 1 list elements with total size less than 2^32 octets (4 + compound)
	Map8    uint8 = 0xc1 // up to 2^8 - 1 octets of encoded map data (1 + compound)
	Map32   uint8 = 0xd1 // up to 2^32 - 1 octets of encoded map data (4 + compound)
	Array8  uint8 = 0xe0 // up to 2^8 - 1 array elements with total size less than 2^8 octets (1 + array)
	Array32 uint8 = 0xf0 // up to 2^32 - 1 array elements with total size less than 2^32 octets (4 + array)
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

func readUint(r byteReader) (value uint, _ error) {
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
		return uint(n), err
	case Ushort:
		var n uint16
		err := binary.Read(r, binary.BigEndian, &n)
		return uint(n), err
	case Uint:
		var n uint32
		err := binary.Read(r, binary.BigEndian, &n)
		return uint(n), err
	case Ulong:
		var n uint64
		err := binary.Read(r, binary.BigEndian, &n)
		return uint(n), err

	default:
		return 0, fmt.Errorf("type code %x is not a recognized number type", b)
	}
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
	case uint32:
		if t == 0 {
			err = buf.WriteByte(Uint0)
			break
		}
		err = buf.WriteByte(Uint)
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.BigEndian, t)
	case uint16:
		err = buf.WriteByte(Ushort)
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.BigEndian, t)
	case []Symbol:
		err = writeSymbolArray(buf, t)
	case string:
		err = writeString(buf, t)
	case []byte:
		err = writeBinary(buf, t)
	default:
		return nil, fmt.Errorf("marshal not implemented for %T", i)
	}
	return append([]byte(nil), buf.Bytes()...), err
}

type Milliseconds time.Duration

func (m *Milliseconds) UnmarshalBinary(r byteReader) error {
	var n uint32
	err := Unmarshal(r, &n)
	*m = Milliseconds(time.Duration(n) * time.Millisecond)
	return err
}
