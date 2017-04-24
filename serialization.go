package amqp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/pkg/errors"
)

type byteReader interface {
	io.Reader
	io.ByteReader
	UnreadByte() error
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

		return fmt.Errorf("unable to unmarshal %T", i)
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
		return errors.Wrapf(err, "reading composite header")
	}

	if t != typ {
		return errors.Errorf("invalid header %#0x for %#0x", t, typ)
	}

	if numFields > len(fields) {
		return errors.Errorf("invalid field count %d for %#0x", numFields, typ)
	}

	for i := 0; i < numFields; i++ {
		err = unmarshal(r, fields[i])
		if err != nil {
			return errors.Wrapf(err, "unmarshaling field %d", i)
		}
	}
	return nil
}

func readCompositeHeader(r byteReader) (_ amqpType, fields int, _ error) {
	byt, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	if byt == null {
		return 0, 0, errNull
	}

	if byt != 0 {
		return 0, 0, errors.Errorf("invalid composite header %0x", byt)
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
			rawFields[i] = []byte{null}
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
	ofType := sym8
	for _, symbol := range symbols {
		if len(symbol) > math.MaxUint8 {
			ofType = sym32
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
	case sym8:
		wr.WriteByte(uint8(l))
	case sym32:
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
		_, err := wr.Write(append([]byte{str8, uint8(l)}, []byte(str)...))
		return err

	// Str32
	case l < math.MaxUint32:
		wr.WriteByte(str32)
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
		_, err := wr.Write(append([]byte{vbin8, uint8(l)}, bin...))
		return err

	// List32
	case l < math.MaxUint32:
		wr.WriteByte(vbin32)
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

func writeComposite(wr byteWriter, code amqpType, fields ...[]byte) error {
	_, err := wr.Write([]byte{0x0, smallulong, uint8(code)})
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

	size8 := list8
	size32 := list32
	if isArray {
		size8 = array8
		size32 = array32
	}

	switch l := len(fields); {
	// list0
	case l == 0:
		if isArray {
			return errors.New("invalid array length 0")
		}
		return wr.WriteByte(list0)

	// list8
	case l < 256 && size < 256:
		_, err := wr.Write([]byte{size8, uint8(size + 1), uint8(l)})
		if err != nil {
			return err
		}

	// list32
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

func readBinary(r byteReader) ([]byte, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	vari, err := readVariableType(r, b)
	return vari, err
}

func readVariableType(r byteReader, t byte) ([]byte, error) {
	var buf []byte
	switch t {
	case null:
		return nil, nil
	case vbin8, str8, sym8:
		n, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		buf = make([]byte, int(n))
	case vbin32, str32, sym32:
		var n uint32
		err := binary.Read(r, binary.BigEndian, &n)
		if err != nil {
			return nil, err
		}
		buf = make([]byte, int(n))
	default:
		return nil, errors.Errorf("type code %#00x is not a recognized variable length type", t)
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
	case null:
		return 0, 0, errNull
	case list0:
		return 0, 0, nil
	case list8, array8:
		lByte, err := r.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		elemByte, err := r.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		return int(elemByte), int(lByte), nil
	case list32, array32:
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
		return 0, 0, errors.Errorf("type code %x is not a recognized list type", b)
	}
}

func readAny(r byteReader) (interface{}, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	if b == null {
		return nil, nil
	}

	err = r.UnreadByte()
	if err != nil {
		return nil, err
	}

	switch b {
	case boolAMQP, boolTrue, boolFalse:
		return readBool(r)
	case ubyte, ushort, uintAMQP, smalluint, uint0, ulong, smallulong, ulong0:
		return readUint(r)
	case byteAMQP, short, intAMQP, smallint, long, smalllong:
		return readInt(r)
	case float, double, decimal32, decimal64, decimal128, char, uuid,
		list0, list8, list32, map8, map32, array8, array32:
		return nil, errors.Errorf("%0x not implemented", b)
	case vbin8, vbin32:
		return readBinary(r)
	case str8, str32, sym8, sym32:
		return readString(r)
	case timestamp:
		return readTimestamp(r)
	default:
		return nil, errors.Errorf("unknown type %0x", b)
	}
}

func readTimestamp(r byteReader) (time.Time, error) {
	typ, err := r.ReadByte()
	if err != nil {
		return time.Time{}, err
	}
	if typ == null {
		return time.Time{}, errNull
	}
	if typ != timestamp {
		return time.Time{}, errors.Errorf("invaild type for timestamp %0x", typ)
	}
	var n uint64
	err = binary.Read(r, binary.BigEndian, &n)
	rem := n % 1000
	return time.Unix(int64(n)/1000, int64(rem)*1000000).UTC(), err
}

// Type codes
const (
	null uint8 = 0x40

	// Bool
	boolAMQP  uint8 = 0x56 // boolean with the octet 0x00 being false and octet 0x01 being true
	boolTrue  uint8 = 0x41
	boolFalse uint8 = 0x42

	// Unsigned
	ubyte      uint8 = 0x50 // 8-bit unsigned integer (1)
	ushort     uint8 = 0x60 // 16-bit unsigned integer in network byte order (2)
	uintAMQP   uint8 = 0x70 // 32-bit unsigned integer in network byte order (4)
	smalluint  uint8 = 0x52 // unsigned integer value in the range 0 to 255 inclusive (1)
	uint0      uint8 = 0x43 // the uint value 0 (0)
	ulong      uint8 = 0x80 // 64-bit unsigned integer in network byte order (8)
	smallulong uint8 = 0x53 // unsigned long value in the range 0 to 255 inclusive (1)
	ulong0     uint8 = 0x44 // the ulong value 0 (0)

	// Signed
	byteAMQP  uint8 = 0x51 // 8-bit two's-complement integer (1)
	short     uint8 = 0x61 // 16-bit two's-complement integer in network byte order (2)
	intAMQP   uint8 = 0x71 // 32-bit two's-complement integer in network byte order (4)
	smallint  uint8 = 0x54 // 8-bit two's-complement integer (1)
	long      uint8 = 0x81 // 64-bit two's-complement integer in network byte order (8)
	smalllong uint8 = 0x55 // 8-bit two's-complement integer

	// Decimal
	float      uint8 = 0x72 // IEEE 754-2008 binary32 (4)
	double     uint8 = 0x82 // IEEE 754-2008 binary64 (8)
	decimal32  uint8 = 0x74 // IEEE 754-2008 decimal32 using the Binary Integer Decimal encoding (4)
	decimal64  uint8 = 0x84 // IEEE 754-2008 decimal64 using the Binary Integer Decimal encoding (8)
	decimal128 uint8 = 0x94 // IEEE 754-2008 decimal128 using the Binary Integer Decimal encoding (16)

	// Other
	char      uint8 = 0x73 // a UTF-32BE encoded Unicode character (4)
	timestamp uint8 = 0x83 // 64-bit two's-complement integer representing milliseconds since the unix epoc
	uuid      uint8 = 0x98 // UUID as defined in section 4.1.2 of RFC-4122

	// Variable Length
	vbin8  uint8 = 0xa0 // up to 2^8 - 1 octets of binary data (1 + variable)
	vbin32 uint8 = 0xb0 // up to 2^32 - 1 octets of binary data (4 + variable)
	str8   uint8 = 0xa1 // up to 2^8 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (1 + variable)
	str32  uint8 = 0xb1 // up to 2^32 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (4 +variable)
	sym8   uint8 = 0xa3 // up to 2^8 - 1 seven bit ASCII characters representing a symbolic value (1 + variable)
	sym32  uint8 = 0xb3 // up to 2^32 - 1 seven bit ASCII characters representing a symbolic value (4 + variable)

	// Compound
	list0   uint8 = 0x45 // the empty list (i.e. the list with no elements) (0)
	list8   uint8 = 0xc0 // up to 2^8 - 1 list elements with total size less than 2^8 octets (1 + compound)
	list32  uint8 = 0xd0 // up to 2^32 - 1 list elements with total size less than 2^32 octets (4 + compound)
	map8    uint8 = 0xc1 // up to 2^8 - 1 octets of encoded map data (1 + compound)
	map32   uint8 = 0xd1 // up to 2^32 - 1 octets of encoded map data (4 + compound)
	array8  uint8 = 0xe0 // up to 2^8 - 1 array elements with total size less than 2^8 octets (1 + array)
	array32 uint8 = 0xf0 // up to 2^32 - 1 array elements with total size less than 2^32 octets (4 + array)
)

func readInt(r byteReader) (value int, _ error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	switch b {
	// Unsigned
	case uint0, ulong0:
		return 0, nil
	case ubyte, smalluint, smallulong:
		n, err := r.ReadByte()
		return int(n), err
	case ushort:
		var n uint16
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	case uintAMQP:
		var n uint32
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	case ulong:
		var n uint64
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err

	// Signed
	case byteAMQP, smallint, smalllong:
		var n int8
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	case short:
		var n int16
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	case intAMQP:
		var n int32
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	case long:
		var n int64
		err := binary.Read(r, binary.BigEndian, &n)
		return int(n), err
	default:
		return 0, errors.Errorf("type code %x is not a recognized number type", b)
	}
}

func readBool(r byteReader) (bool, error) {
	b, err := r.ReadByte()
	if err != nil {
		return false, err
	}

	switch b {
	case null:
		return false, errNull
	case boolAMQP:
		b, err = r.ReadByte()
		if err != nil {
			return false, err
		}
		return b != 0, nil
	case boolTrue:
		return true, nil
	case boolFalse:
		return false, nil
	default:
		return false, fmt.Errorf("type code %x is not a recognized bool type", b)
	}
}

var errNull = errors.New("error is null")

func readUint(r byteReader) (value uint64, _ error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	switch b {
	case null:
		return 0, errNull
	case uint0, ulong0:
		return 0, nil
	case ubyte, smalluint, smallulong:
		n, err := r.ReadByte()
		return uint64(n), err
	case ushort:
		var n uint16
		err := binary.Read(r, binary.BigEndian, &n)
		return uint64(n), err
	case uintAMQP:
		var n uint32
		err := binary.Read(r, binary.BigEndian, &n)
		return uint64(n), err
	case ulong:
		var n uint64
		err := binary.Read(r, binary.BigEndian, &n)
		return n, err

	default:
		return 0, errors.Errorf("type code %x is not a recognized number type", b)
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
		_, err = buf.Write(append([]byte{sym8, uint8(l)}, []byte(s)...))

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
			err = buf.WriteByte(boolTrue)
		} else {
			err = buf.WriteByte(boolFalse)
		}
	case uint64:
		if t == 0 {
			err = buf.WriteByte(ulong0)
			break
		}
		err = buf.WriteByte(ulong)
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.BigEndian, t)
	case uint32:
		if t == 0 {
			err = buf.WriteByte(uint0)
			break
		}
		err = buf.WriteByte(uintAMQP)
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.BigEndian, t)
	case *uint32:
		if t == nil {
			err = buf.WriteByte(null)
			break
		}
		if *t == 0 {
			err = buf.WriteByte(uint0)
			break
		}
		err = buf.WriteByte(uintAMQP)
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.BigEndian, *t)
	case uint16:
		err = buf.WriteByte(ushort)
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.BigEndian, t)
	case uint8:
		_, err = buf.Write([]byte{ubyte, t})
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
		err := wr.WriteByte(map8)
		if err != nil {
			return err
		}
		return wr.WriteByte(uint8(elements))
	}

	err := wr.WriteByte(map32)
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
	limit int
	read  int
}

var errLimitReached = errors.New("limit reached")

func (r *limitByteReader) Read(p []byte) (int, error) {
	if r.read >= r.limit {
		return 0, errLimitReached
	}
	n, err := r.byteReader.Read(p)
	r.read += n
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

	var n int
	switch b {
	case null:
		return nil, errNull
	case map8:
		bn, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		n = int(bn)
	case map32:
		var n32 uint32
		err = binary.Read(r, binary.BigEndian, &n32)
		if err != nil {
			return nil, err
		}
		n = int(n32)
	default:
		return nil, fmt.Errorf("invalid map type %x", b)
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
