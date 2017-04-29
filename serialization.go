package amqp

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"reflect"
	"sync"
	"time"
	"unicode/utf8"
)

type amqpType uint8

// Type codes
const (
	typeCodeNull amqpType = 0x40

	// Bool
	typeCodeBool      amqpType = 0x56 // boolean with the octet 0x00 being false and octet 0x01 being true
	typeCodeBoolTrue  amqpType = 0x41
	typeCodeBoolFalse amqpType = 0x42

	// Unsigned
	typeCodeUbyte      amqpType = 0x50 // 8-bit unsigned integer (1)
	typeCodeUshort     amqpType = 0x60 // 16-bit unsigned integer in network byte order (2)
	typeCodeUint       amqpType = 0x70 // 32-bit unsigned integer in network byte order (4)
	typeCodeSmalluint  amqpType = 0x52 // unsigned integer value in the range 0 to 255 inclusive (1)
	typeCodeUint0      amqpType = 0x43 // the uint value 0 (0)
	typeCodeUlong      amqpType = 0x80 // 64-bit unsigned integer in network byte order (8)
	typeCodeSmallulong amqpType = 0x53 // unsigned long value in the range 0 to 255 inclusive (1)
	typeCodeUlong0     amqpType = 0x44 // the ulong value 0 (0)

	// Signed
	typeCodeByte      amqpType = 0x51 // 8-bit two's-complement integer (1)
	typeCodeShort     amqpType = 0x61 // 16-bit two's-complement integer in network byte order (2)
	typeCodeInt       amqpType = 0x71 // 32-bit two's-complement integer in network byte order (4)
	typeCodeSmallint  amqpType = 0x54 // 8-bit two's-complement integer (1)
	typeCodeLong      amqpType = 0x81 // 64-bit two's-complement integer in network byte order (8)
	typeCodeSmalllong amqpType = 0x55 // 8-bit two's-complement integer

	// Decimal
	typeCodeFloat      amqpType = 0x72 // IEEE 754-2008 binary32 (4)
	typeCodeDouble     amqpType = 0x82 // IEEE 754-2008 binary64 (8)
	typeCodeDecimal32  amqpType = 0x74 // IEEE 754-2008 decimal32 using the Binary Integer Decimal encoding (4)
	typeCodeDecimal64  amqpType = 0x84 // IEEE 754-2008 decimal64 using the Binary Integer Decimal encoding (8)
	typeCodeDecimal128 amqpType = 0x94 // IEEE 754-2008 decimal128 using the Binary Integer Decimal encoding (16)

	// Other
	typeCodeChar      amqpType = 0x73 // a UTF-32BE encoded Unicode character (4)
	typeCodeTimestamp amqpType = 0x83 // 64-bit two's-complement integer representing milliseconds since the unix epoc
	typeCodeUUID      amqpType = 0x98 // UUID as defined in section 4.1.2 of RFC-4122

	// Variable Length
	typeCodeVbin8  amqpType = 0xa0 // up to 2^8 - 1 octets of binary data (1 + variable)
	typeCodeVbin32 amqpType = 0xb0 // up to 2^32 - 1 octets of binary data (4 + variable)
	typeCodeStr8   amqpType = 0xa1 // up to 2^8 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (1 + variable)
	typeCodeStr32  amqpType = 0xb1 // up to 2^32 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (4 +variable)
	typeCodeSym8   amqpType = 0xa3 // up to 2^8 - 1 seven bit ASCII characters representing a symbolic value (1 + variable)
	typeCodeSym32  amqpType = 0xb3 // up to 2^32 - 1 seven bit ASCII characters representing a symbolic value (4 + variable)

	// Compound
	typeCodeList0   amqpType = 0x45 // the empty list (i.e. the list with no elements) (0)
	typeCodeList8   amqpType = 0xc0 // up to 2^8 - 1 list elements with total size less than 2^8 octets (1 + compound)
	typeCodeList32  amqpType = 0xd0 // up to 2^32 - 1 list elements with total size less than 2^32 octets (4 + compound)
	typeCodeMap8    amqpType = 0xc1 // up to 2^8 - 1 octets of encoded map data (1 + compound)
	typeCodeMap32   amqpType = 0xd1 // up to 2^32 - 1 octets of encoded map data (4 + compound)
	typeCodeArray8  amqpType = 0xe0 // up to 2^8 - 1 array elements with total size less than 2^8 octets (1 + array)
	typeCodeArray32 amqpType = 0xf0 // up to 2^32 - 1 array elements with total size less than 2^32 octets (4 + array)

	// Composites
	typeCodeOpen        amqpType = 0x10
	typeCodeBegin       amqpType = 0x11
	typeCodeAttach      amqpType = 0x12
	typeCodeFlow        amqpType = 0x13
	typeCodeTransfer    amqpType = 0x14
	typeCodeDisposition amqpType = 0x15
	typeCodeDetach      amqpType = 0x16
	typeCodeEnd         amqpType = 0x17
	typeCodeClose       amqpType = 0x18

	typeCodeSource amqpType = 0x28
	typeCodeTarget amqpType = 0x29
	typeCodeError  amqpType = 0x1d

	typeCodeMessageHeader         amqpType = 0x70
	typeCodeDeliveryAnnotations   amqpType = 0x71
	typeCodeMessageAnnotations    amqpType = 0x72
	typeCodeMessageProperties     amqpType = 0x73
	typeCodeApplicationProperties amqpType = 0x74
	typeCodeApplicationData       amqpType = 0x75
	typeCodeAMQPSequence          amqpType = 0x76
	typeCodeAMQPValue             amqpType = 0x77
	typeCodeFooter                amqpType = 0x78

	typeCodeStateReceived amqpType = 0x23
	typeCodeStateAccepted amqpType = 0x24
	typeCodeStateRejected amqpType = 0x25
	typeCodeStateReleased amqpType = 0x26
	typeCodeStateModified amqpType = 0x27

	typeCodeSASLMechanism amqpType = 0x40
	typeCodeSASLInit      amqpType = 0x41
	typeCodeSASLChallenge amqpType = 0x42
	typeCodeSASLResponse  amqpType = 0x43
	typeCodeSASLOutcome   amqpType = 0x44
)

type byteReader interface {
	io.Reader
	io.ByteReader
	UnreadByte() error
	Bytes() []byte
	Len() int
	Next(int) []byte
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
	unmarshal(r byteReader) error
}

func unmarshal(r byteReader, i interface{}) error {
	if um, ok := i.(unmarshaler); ok {
		return um.unmarshal(r)
	}

	switch t := i.(type) {
	case *int:
		val, err := readInt(r)
		if err == errNull {
			return nil
		}
		if err != nil {
			return err
		}
		*t = val
	case *uint64:
		val, err := readUint(r)
		if err == errNull {
			return nil
		}
		if err != nil {
			return err
		}
		*t = uint64(val)
	case *uint32:
		val, err := readUint(r)
		if err == errNull {
			return nil
		}
		if err != nil {
			return err
		}
		*t = uint32(val)
	case *uint16:
		val, err := readUint(r)
		if err == errNull {
			return nil
		}
		if err != nil {
			return err
		}
		*t = uint16(val)
	case *uint8:
		val, err := readUint(r)
		if err == errNull {
			return nil
		}
		if err != nil {
			return err
		}
		*t = uint8(val)
	case *string:
		val, err := readString(r)
		if err == errNull {
			return nil
		}
		if err != nil {
			return err
		}
		*t = val
	case *[]Symbol:
		sa, err := readSymbolArray(r)
		if err == errNull {
			return nil
		}
		if err != nil {
			return err
		}
		*t = sa
	case *Symbol:
		s, err := readString(r)
		if err == errNull {
			return nil
		}
		if err != nil {
			return err
		}
		*t = Symbol(s)
	case *[]byte:
		val, err := readBinary(r)
		if err != nil {
			return err
		}
		*t = val
	case *bool:
		b, err := readBool(r)
		if err == errNull {
			return nil
		}
		if err != nil {
			return err
		}
		*t = b
	case *time.Time:
		ts, err := readTimestamp(r)
		if err == errNull {
			return nil
		}
		if err != nil {
			return err
		}
		*t = ts
	case *map[interface{}]interface{}:
		return (*mapAnyAny)(t).unmarshal(r)
	case *map[string]interface{}:
		return (*mapStringAny)(t).unmarshal(r)
	case *map[Symbol]interface{}:
		return (*mapSymbolAny)(t).unmarshal(r)
	case *interface{}:
		v, err := readAny(r)
		if err != nil {
			return err
		}
		*t = v
	default:
		v := reflect.ValueOf(i)         // **struct
		indirect := reflect.Indirect(v) // *struct
		if indirect.Kind() == reflect.Ptr {
			if indirect.IsNil() { // *struct == nil
				indirect.Set(reflect.New(indirect.Type().Elem()))
			}
			return unmarshal(r, indirect.Interface())
		}
		// if um, ok := indirect.Interface().(unmarshaler); ok {
		// 	return indirect.Interface().(unmarshaler).UnmarshalBinary(r)
		// }

		return errorErrorf("unable to unmarshal %T", i)
	}
	return nil
}

type mapAnyAny map[interface{}]interface{}

func (m *mapAnyAny) unmarshal(r byteReader) error {
	mr, err := newMapReader(r)
	if err == errNull {
		return nil
	}
	if err != nil {
		return err
	}

	pairs := mr.count / 2

	mm := make(mapAnyAny, pairs)
	for i := 0; i < pairs; i++ {
		var (
			key   interface{}
			value interface{}
		)
		err = mr.next(&key, &value)
		if err != nil {
			return err
		}
		mm[key] = value
	}
	*m = mm
	return nil
}

type mapStringAny map[string]interface{}

func (m *mapStringAny) unmarshal(r byteReader) error {
	mr, err := newMapReader(r)
	if err == errNull {
		return nil
	}
	if err != nil {
		return err
	}

	pairs := mr.count / 2

	mm := make(mapStringAny, pairs)
	for i := 0; i < pairs; i++ {
		var (
			key   string
			value interface{}
		)
		err = mr.next(&key, &value)
		if err != nil {
			return err
		}
		mm[key] = value
	}
	*m = mm
	return nil
}

type mapSymbolAny map[Symbol]interface{}

func (f *mapSymbolAny) unmarshal(r byteReader) error {
	mr, err := newMapReader(r)
	if err == errNull {
		return nil
	}
	if err != nil {
		return err
	}

	pairs := mr.count / 2

	m := make(mapSymbolAny, pairs)
	for i := 0; i < pairs; i++ {
		var (
			key   Symbol
			value interface{}
		)
		err = mr.next(&key, &value)
		if err != nil {
			return err
		}
		m[key] = value
	}
	*f = m
	return nil
}

func unmarshalComposite(r byteReader, typ amqpType, fields ...interface{}) error {
	t, numFields, err := readCompositeHeader(r)
	if err == errNull {
		return nil
	}
	if err != nil {
		return errorWrapf(err, "reading composite header")
	}

	if t != typ {
		return errorErrorf("invalid header %#0x for %#0x", t, typ)
	}

	if numFields > len(fields) {
		return errorErrorf("invalid field count %d for %#0x", numFields, typ)
	}

	for i := 0; i < numFields; i++ {
		err = unmarshal(r, fields[i])
		if err != nil {
			return errorWrapf(err, "unmarshaling field %d", i)
		}
	}
	return nil
}

func readCompositeHeader(r byteReader) (_ amqpType, fields int, _ error) {
	byt, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	if amqpType(byt) == typeCodeNull {
		return 0, 0, errNull
	}

	if byt != 0 {
		return 0, 0, errorErrorf("invalid composite header %0x", byt)
	}

	v, err := readInt(r)
	if err != nil {
		return 0, 0, err
	}

	fields, _, err = readSlice(r)

	return amqpType(v), fields, err
}

type field struct {
	value interface{}
	omit  bool
}

func marshalComposite(code amqpType, fields ...field) ([]byte, error) {
	var (
		rawFields  = make([][]byte, len(fields))
		lastSetIdx = -1
		err        error
	)

	for i, f := range fields {
		if f.omit {
			continue
		}

		rawFields[i], err = marshal(f.value)
		if err != nil {
			return nil, err
		}

		lastSetIdx = i
	}

	for i := 0; i < lastSetIdx+1; i++ {
		if rawFields[i] == nil {
			rawFields[i] = []byte{byte(typeCodeNull)}
		}
	}

	buf := bufPool.New().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	err = writeComposite(buf, code, rawFields[:lastSetIdx+1]...)
	if err != nil {
		return nil, err
	}

	return append([]byte(nil), buf.Bytes()...), nil
}

func writeSymbolArray(w byteWriter, symbols []Symbol) error {
	ofType := typeCodeSym8
	for _, symbol := range symbols {
		if len(symbol) > math.MaxUint8 {
			ofType = typeCodeSym32
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

func writeSymbol(wr byteWriter, sym Symbol, typ amqpType) error {
	if !utf8.ValidString(string(sym)) {
		return errorNew("not a valid UTF-8 string")
	}

	l := len(sym)

	switch typ {
	case typeCodeSym8:
		wr.WriteByte(uint8(l))
	case typeCodeSym32:
		err := binary.Write(wr, binary.BigEndian, uint32(l))
		if err != nil {
			return err
		}
	default:
		return errorNew("invalid symbol type")
	}
	_, err := wr.Write([]byte(sym))
	return err
}

func writeString(wr byteWriter, str string) error {
	if !utf8.ValidString(str) {
		return errorNew("not a valid UTF-8 string")
	}
	l := len(str)

	switch {
	// Str8
	case l < 256:
		_, err := wr.Write(append([]byte{byte(typeCodeStr8), uint8(l)}, []byte(str)...))
		return err

	// Str32
	case l < math.MaxUint32:
		wr.WriteByte(byte(typeCodeStr32))
		err := binary.Write(wr, binary.BigEndian, uint32(l))
		if err != nil {
			return err
		}
		_, err = wr.Write([]byte(str))
		return err

	default:
		return errorNew("too long")
	}
}

func writeBinary(wr byteWriter, bin []byte) error {
	l := len(bin)

	switch {
	// List8
	case l < 256:
		_, err := wr.Write(append([]byte{byte(typeCodeVbin8), uint8(l)}, bin...))
		return err

	// List32
	case l < math.MaxUint32:
		wr.WriteByte(byte(typeCodeVbin32))
		err := binary.Write(wr, binary.BigEndian, uint32(l))
		if err != nil {
			return err
		}
		_, err = wr.Write(bin)
		return err

	default:
		return errorNew("too long")
	}
}

func writeComposite(wr byteWriter, code amqpType, fields ...[]byte) error {
	_, err := wr.Write([]byte{0x0, byte(typeCodeSmallulong), uint8(code)})
	if err != nil {
		return err
	}

	return writeList(wr, fields...)
}

func writeArray(wr byteWriter, of amqpType, fields ...[]byte) error {
	const isArray = true
	return writeSlice(wr, isArray, of, fields...)
}

func writeList(wr byteWriter, fields ...[]byte) error {
	const isArray = false
	return writeSlice(wr, isArray, 0, fields...)
}

func writeSlice(wr byteWriter, isArray bool, of amqpType, fields ...[]byte) error {
	var size int
	for _, field := range fields {
		size += len(field)
	}

	size8 := typeCodeList8
	size32 := typeCodeList32
	if isArray {
		size8 = typeCodeArray8
		size32 = typeCodeArray32
	}

	switch l := len(fields); {
	// list0
	case l == 0:
		if isArray {
			return errorNew("invalid array length 0")
		}
		return wr.WriteByte(byte(typeCodeList0))

	// list8
	case l < 256 && size < 256:
		_, err := wr.Write([]byte{byte(size8), uint8(size + 1), uint8(l)})
		if err != nil {
			return err
		}

	// list32
	case l < math.MaxUint32 && size < math.MaxUint32:
		err := wr.WriteByte(byte(size32))
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
		return errorNew("too many fields")
	}

	if isArray {
		err := wr.WriteByte(byte(of))
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
		vari, err := readVariableType(r, amqpType(b))
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
		vari, err := readVariableType(r, amqpType(b))
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

	vari, err := readVariableType(r, amqpType(b))
	return string(vari), err
}

func readBinary(r byteReader) ([]byte, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	vari, err := readVariableType(r, amqpType(b))
	return vari, err
}

var errInvalidLength = errorNew("length field is larger than frame")

func readVariableType(r byteReader, of amqpType) ([]byte, error) {
	var buf []byte
	switch of {
	case typeCodeNull:
		return nil, nil
	case typeCodeVbin8, typeCodeStr8, typeCodeSym8:
		n, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if uint64(n) > uint64(r.Len()) {
			return nil, errInvalidLength
		}
		buf = make([]byte, n)
	case typeCodeVbin32, typeCodeStr32, typeCodeSym32:
		var n uint32
		err := binary.Read(r, binary.BigEndian, &n)
		if err != nil {
			return nil, err
		}
		if uint64(n) > uint64(r.Len()) {
			return nil, errInvalidLength
		}
		buf = make([]byte, n)
	default:
		return nil, errorErrorf("type code %#00x is not a recognized variable length type", of)
	}
	_, err := io.ReadFull(r, buf)
	return buf, err
}

func readSlice(r byteReader) (elements int, length int, _ error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	switch amqpType(b) {
	case typeCodeNull:
		return 0, 0, errNull
	case typeCodeList0:
		return 0, 0, nil
	case typeCodeList8, typeCodeArray8:
		lByte, err := r.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		elemByte, err := r.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		return int(elemByte), int(lByte), nil
	case typeCodeList32, typeCodeArray32:
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
		return 0, 0, errorErrorf("type code %x is not a recognized list type", b)
	}
}

func readAny(r byteReader) (interface{}, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	if amqpType(b) == typeCodeNull {
		return nil, nil
	}

	err = r.UnreadByte()
	if err != nil {
		return nil, err
	}

	switch amqpType(b) {
	case typeCodeBool, typeCodeBoolTrue, typeCodeBoolFalse:
		return readBool(r)
	case typeCodeUbyte, typeCodeUshort, typeCodeUint, typeCodeSmalluint, typeCodeUint0, typeCodeUlong, typeCodeSmallulong, typeCodeUlong0:
		return readUint(r)
	case typeCodeByte, typeCodeShort, typeCodeInt, typeCodeSmallint, typeCodeLong, typeCodeSmalllong:
		return readInt(r)
	case typeCodeFloat, typeCodeDouble, typeCodeDecimal32, typeCodeDecimal64, typeCodeDecimal128, typeCodeChar, typeCodeUUID,
		typeCodeList0, typeCodeList8, typeCodeList32, typeCodeMap8, typeCodeMap32, typeCodeArray8, typeCodeArray32:
		return nil, errorErrorf("%0x not implemented", b)
	case typeCodeVbin8, typeCodeVbin32:
		return readBinary(r)
	case typeCodeStr8, typeCodeStr32, typeCodeSym8, typeCodeSym32:
		return readString(r)
	case typeCodeTimestamp:
		return readTimestamp(r)
	default:
		return nil, errorErrorf("unknown type %0x", b)
	}
}

func readTimestamp(r byteReader) (time.Time, error) {
	b, err := r.ReadByte()
	if err != nil {
		return time.Time{}, err
	}

	switch t := amqpType(b); {
	case t == typeCodeNull:
		return time.Time{}, errNull
	case t != typeCodeTimestamp:
		return time.Time{}, errorErrorf("invaild type for timestamp %0x", t)
	}

	var n uint64
	err = binary.Read(r, binary.BigEndian, &n)
	rem := n % 1000
	return time.Unix(int64(n)/1000, int64(rem)*1000000).UTC(), err
}

func readInt(r byteReader) (value int, _ error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	switch amqpType(b) {
	// Unsigned
	case typeCodeUint0, typeCodeUlong0:
		return 0, nil
	case typeCodeUbyte, typeCodeSmalluint, typeCodeSmallulong:
		n, err := r.ReadByte()
		return int(n), err
	case typeCodeUshort:
		var n uint16
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	case typeCodeUint:
		var n uint32
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	case typeCodeUlong:
		var n uint64
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err

	// Signed
	case typeCodeByte, typeCodeSmallint, typeCodeSmalllong:
		var n int8
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	case typeCodeShort:
		var n int16
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	case typeCodeInt:
		var n int32
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	case typeCodeLong:
		var n int64
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	default:
		return 0, errorErrorf("type code %x is not a recognized number type", b)
	}
}

func readBool(r byteReader) (bool, error) {
	b, err := r.ReadByte()
	if err != nil {
		return false, err
	}

	switch amqpType(b) {
	case typeCodeNull:
		return false, errNull
	case typeCodeBool:
		b, err = r.ReadByte()
		if err != nil {
			return false, err
		}
		return b != 0, nil
	case typeCodeBoolTrue:
		return true, nil
	case typeCodeBoolFalse:
		return false, nil
	default:
		return false, errorErrorf("type code %x is not a recognized bool type", b)
	}
}

var errNull = errorNew("error is null")

func readUint(r byteReader) (value uint64, _ error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	switch amqpType(b) {
	case typeCodeNull:
		return 0, errNull
	case typeCodeUint0, typeCodeUlong0:
		return 0, nil
	case typeCodeUbyte, typeCodeSmalluint, typeCodeSmallulong:
		n, err := r.ReadByte()
		return uint64(n), err
	case typeCodeUshort:
		var n uint16
		err := binary.Read(r, binary.BigEndian, &n)
		return uint64(n), err
	case typeCodeUint:
		var n uint32
		err := binary.Read(r, binary.BigEndian, &n)
		return uint64(n), err
	case typeCodeUlong:
		var n uint64
		err := binary.Read(r, binary.BigEndian, &n)
		return n, err

	default:
		return 0, errorErrorf("type code %x is not a recognized number type", b)
	}
}

type Symbol string

func (s Symbol) marshal() ([]byte, error) {
	l := len(s)

	buf := bufPool.New().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	var err error
	switch {
	// List8
	case l < 256:
		_, err = buf.Write(append([]byte{byte(typeCodeSym8), byte(l)}, []byte(s)...))

	// List32
	case l < math.MaxUint32:
		err = binary.Write(buf, binary.BigEndian, uint32(l))
		if err != nil {
			return nil, err
		}
		_, err = buf.Write([]byte(s))
	default:
		return nil, errorNew("too long")
	}

	return append([]byte(nil), buf.Bytes()...), err
}

type marshaler interface {
	marshal() ([]byte, error)
}

func marshal(i interface{}) ([]byte, error) {
	if bm, ok := i.(marshaler); ok {
		return bm.marshal()
	}

	buf := bufPool.New().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	var err error
	switch t := i.(type) {
	case bool:
		if t {
			err = buf.WriteByte(byte(typeCodeBoolTrue))
		} else {
			err = buf.WriteByte(byte(typeCodeBoolFalse))
		}
	case uint64:
		if t == 0 {
			err = buf.WriteByte(byte(typeCodeUlong0))
			break
		}
		err = buf.WriteByte(byte(typeCodeUlong))
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.BigEndian, t)
	case uint32:
		if t == 0 {
			err = buf.WriteByte(byte(typeCodeUint0))
			break
		}
		err = buf.WriteByte(byte(typeCodeUint))
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.BigEndian, t)
	case *uint32:
		if t == nil {
			err = buf.WriteByte(byte(typeCodeNull))
			break
		}
		if *t == 0 {
			err = buf.WriteByte(byte(typeCodeUint0))
			break
		}
		err = buf.WriteByte(byte(typeCodeUint))
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.BigEndian, *t)
	case uint16:
		err = buf.WriteByte(byte(typeCodeUshort))
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.BigEndian, t)
	case uint8:
		_, err = buf.Write([]byte{byte(typeCodeUbyte), t})
	case []Symbol:
		err = writeSymbolArray(buf, t)
	case string:
		err = writeString(buf, t)
	case []byte:
		err = writeBinary(buf, t)
	default:
		return nil, errorErrorf("marshal not implemented for %T", i)
	}
	return append([]byte(nil), buf.Bytes()...), err
}

type milliseconds time.Duration

func (m milliseconds) marshal() ([]byte, error) {
	return marshal(uint32((time.Duration)(m).Nanoseconds() / 1000000))
}

func (m *milliseconds) unmarshal(r byteReader) error {
	var n uint32
	err := unmarshal(r, &n)
	*m = milliseconds(time.Duration(n) * time.Millisecond)
	return err
}

func writeMapHeader(wr byteWriter, elements int) error {
	if elements < math.MaxUint8 {
		err := wr.WriteByte(byte(typeCodeMap8))
		if err != nil {
			return err
		}
		return wr.WriteByte(uint8(elements))
	}

	err := wr.WriteByte(byte(typeCodeMap32))
	if err != nil {
		return err
	}
	return binary.Write(wr, binary.BigEndian, uint32(elements))
}

func writeMapElement(wr byteWriter, key, value interface{}) error {
	keyBytes, err := marshal(key)
	if err != nil {
		return err
	}
	valueBytes, err := marshal(value)
	if err != nil {
		return err
	}
	_, err = wr.Write(keyBytes)
	if err != nil {
		return err
	}
	_, err = wr.Write(valueBytes)
	return err
}

type limitByteReader struct {
	byteReader
	limit uint32
	read  uint32
}

var errLimitReached = errorNew("limit reached")

func (r *limitByteReader) Read(p []byte) (int, error) {
	if r.read >= r.limit {
		return 0, errLimitReached
	}
	n, err := r.byteReader.Read(p)
	r.read += uint32(n)
	return n, err
}

func (r *limitByteReader) limitReached() bool {
	return r.read >= r.limit
}

type mapReader struct {
	r     *limitByteReader
	count int // elements (2 * # of pairs)
}

func (mr *mapReader) next(key, value interface{}) error {
	err := unmarshal(mr.r, key)
	if err != nil {
		return err
	}
	return unmarshal(mr.r, value)
}

func newMapReader(r byteReader) (*mapReader, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	var n uint32
	switch amqpType(b) {
	case typeCodeNull:
		return nil, errNull
	case typeCodeMap8:
		bn, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		n = uint32(bn)
	case typeCodeMap32:
		err = binary.Read(r, binary.BigEndian, &n)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errorErrorf("invalid map type %x", b)
	}

	if uint64(n) > uint64(r.Len()) {
		return nil, errInvalidLength
	}

	b, err = r.ReadByte()
	if err != nil {
		return nil, err
	}

	return &mapReader{
		r:     &limitByteReader{byteReader: r, limit: n},
		count: int(b),
	}, nil
}
