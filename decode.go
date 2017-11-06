package amqp

import (
	"encoding/binary"
	"io"
	"reflect"
	"time"
)

// reader is the required interface for unmarshaling AMQP encoded
// data. It is fulfilled byte *bytes.Buffer.
type reader interface {
	io.Reader
	io.ByteReader
	UnreadByte() error
	Bytes() []byte
	Len() int
	Next(int) []byte
}

// parseFrameHeader reads the header from r and returns the result.
//
// No validation is done.
func parseFrameHeader(r io.Reader) (frameHeader, error) {
	var fh frameHeader
	err := binary.Read(r, binary.BigEndian, &fh)
	return fh, err
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

// peekFrameBodyType peeks at the frame body's type code without advancing r.
func peekFrameBodyType(r reader) (amqpType, error) {
	payload := r.Bytes()

	if r.Len() < 3 || payload[0] != 0 || amqpType(payload[1]) != typeCodeSmallUlong {
		return 0, errorNew("invalid frame body header")
	}

	return amqpType(payload[2]), nil
}

// parseFrameBody reads and unmarshals an AMQP frame.
func parseFrameBody(r reader) (frameBody, error) {
	pType, err := peekFrameBodyType(r)
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

// unmarshaler is fulfilled by types that can unmarshal
// themselves from AMQP data.
type unmarshaler interface {
	unmarshal(r reader) error
}

// unmarshal decodes AMQP encoded data into i.
//
// The decoding method is based on the type of i.
//
// If i implements unmarshaler, i.unmarshal() will be called.
//
// Pointers to primitive types will be decoded via the appropriate read[Type] function.
//
// If i is a pointer to a pointer (**Type), it will be dereferenced and a new instance
// of (*Type) is allocated via reflection.
//
// Common map types (map[string]string, map[Symbol]interface{}, and
// map[interface{}]interface{}), will be decoded via conversion to the mapStringAny,
// mapSymbolAny, and mapAnyAny types.
//
// If the decoding function returns errNull, the isNull return value will
// be true and err will be nil.
func unmarshal(r reader, i interface{}) (isNull bool, err error) {
	defer func() {
		// prevent errNull from being passed up
		if err == errNull {
			isNull = true
			err = nil
		}
	}()

	switch t := i.(type) {
	case unmarshaler:
		return isNull, t.unmarshal(r)
	case *int:
		val, err := readInt(r)
		if err != nil {
			return isNull, err
		}
		*t = val
	case *uint64:
		val, err := readUint(r)
		if err != nil {
			return isNull, err
		}
		*t = uint64(val)
	case *uint32:
		val, err := readUint(r)
		if err != nil {
			return isNull, err
		}
		*t = uint32(val)
	case *uint16:
		val, err := readUint(r)
		if err != nil {
			return isNull, err
		}
		*t = uint16(val)
	case *uint8:
		val, err := readUint(r)
		if err != nil {
			return isNull, err
		}
		*t = uint8(val)
	case *string:
		val, err := readString(r)
		if err != nil {
			return isNull, err
		}
		*t = val
	case *[]symbol:
		sa, err := readSymbolArray(r)
		if err != nil {
			return isNull, err
		}
		*t = sa
	case *symbol:
		s, err := readString(r)
		if err != nil {
			return isNull, err
		}
		*t = symbol(s)
	case *[]byte:
		val, err := readBinary(r)
		if err != nil {
			return isNull, err
		}
		*t = val
	case *bool:
		b, err := readBool(r)
		if err != nil {
			return isNull, err
		}
		*t = b
	case *time.Time:
		ts, err := readTimestamp(r)
		if err != nil {
			return isNull, err
		}
		*t = ts
	case *map[interface{}]interface{}:
		return isNull, (*mapAnyAny)(t).unmarshal(r)
	case *map[string]interface{}:
		return isNull, (*mapStringAny)(t).unmarshal(r)
	case *map[symbol]interface{}:
		return isNull, (*mapSymbolAny)(t).unmarshal(r)
	case *deliveryState:
		typ, err := peekMessageType(r.Bytes())
		if err != nil {
			if err == errNull {
				_, err = r.ReadByte() // if null, discard nullCode
				return true, err
			}
			return false, err
		}

		switch amqpType(typ) {
		case typeCodeStateAccepted:
			*t = &stateAccepted{}
		case typeCodeStateModified:
			*t = &stateModified{}
		case typeCodeStateReceived:
			*t = &stateReceived{}
		case typeCodeStateRejected:
			*t = &stateRejected{}
		case typeCodeStateReleased:
			*t = &stateReleased{}
		default:
			return false, errorErrorf("unexpected type %d for deliveryState", typ)
		}
		return unmarshal(r, *t)

	case *interface{}:
		v, err := readAny(r)
		if err != nil {
			return isNull, err
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
		return isNull, errorErrorf("unable to unmarshal %T", i)
	}
	return isNull, nil
}

// unmarshalComposite is a helper for us in a composite's unmarshal() function.
//
// The composite from r will be unmarshaled into zero or more fields. An error
// will be returned if typ does not match the decoded type.
func unmarshalComposite(r reader, typ amqpType, fields ...unmarshalField) error {
	t, numFields, err := readCompositeHeader(r)
	if err != nil {
		return err
	}

	// check type matches expectation
	if t != typ {
		return errorErrorf("invalid header %#0x for %#0x", t, typ)
	}

	// Validate the field count is less than or equal to the number of fields
	// provided. Fields may be omitted by the sender if they are not set.
	if numFields > len(fields) {
		return errorErrorf("invalid field count %d for %#0x", numFields, typ)
	}

	for i := 0; i < numFields; i++ {
		// Unmarshal each of the received fields.
		null, err := unmarshal(r, fields[i].field)
		if err != nil {
			return errorWrapf(err, "unmarshaling field %d", i)
		}

		// If the field is null and handleNull is set, call it.
		if null && fields[i].handleNull != nil {
			err = fields[i].handleNull()
			if err != nil {
				return err
			}
		}
	}

	// check and call handleNull for the remaining fields
	for i := numFields; i < len(fields); i++ {
		if fields[i].handleNull != nil {
			err = fields[i].handleNull()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// unmarshalField is a struct that contains a field to be unmarshaled into.
//
// An optional nullHandler can be set. If the composite field being unmarshaled
// is null and handleNull is not nil, nullHandler will be called.
type unmarshalField struct {
	field      interface{}
	handleNull nullHandler
}

// nullHandler is a function to be called when a composite's field
// is null.
type nullHandler func() error

// required returns a nullHandler that will cause an error to
// be returned if the field is null.
func required(name string) nullHandler {
	return func() error {
		return errorNew(name + " is required")
	}
}

// defaultUint32 returns a nullHandler that sets n to defaultValue
// if the field is null.
func defaultUint32(n *uint32, defaultValue uint32) nullHandler {
	return func() error {
		*n = defaultValue
		return nil
	}
}

// defaultUint16 returns a nullHandler that sets n to defaultValue
// if the field is null.
func defaultUint16(n *uint16, defaultValue uint16) nullHandler {
	return func() error {
		*n = defaultValue
		return nil
	}
}

// defaultUint8 returns a nullHandler that sets n to defaultValue
// if the field is null.
func defaultUint8(n *uint8, defaultValue uint8) nullHandler {
	return func() error {
		*n = defaultValue
		return nil
	}
}

// defaultSymbol returns a nullHandler that sets s to defaultValue
// if the field is null.
func defaultSymbol(s *symbol, defaultValue symbol) nullHandler {
	return func() error {
		*s = defaultValue
		return nil
	}
}

// readCompositeHeader reads and consumes the composite header from r.
//
// If the composite is null, errNull will be returned.
func readCompositeHeader(r reader) (_ amqpType, fields int, _ error) {
	byt, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	// could be null instead of a composite header
	if amqpType(byt) == typeCodeNull {
		return 0, 0, errNull
	}

	// compsites always start with 0x0
	if byt != 0 {
		return 0, 0, errorErrorf("invalid composite header %0x", byt)
	}

	// next, the composite type is encoded as an AMQP uint8
	v, err := readInt(r)
	if err != nil {
		return 0, 0, err
	}

	// fields are represented as a list
	fields, _, err = readHeaderSlice(r)

	return amqpType(v), fields, err
}

func readStringArray(r reader) ([]string, error) {
	lElems, _, err := readHeaderSlice(r)
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

func readSymbolArray(r reader) ([]symbol, error) {
	lElems, _, err := readHeaderSlice(r)
	if err != nil {
		return nil, err
	}

	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	var strs []symbol
	for i := 0; i < lElems; i++ {
		vari, err := readVariableType(r, amqpType(b))
		if err != nil {
			return nil, err
		}

		strs = append(strs, symbol(vari))
	}
	return strs, nil
}

func readString(r reader) (string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return "", err
	}

	vari, err := readVariableType(r, amqpType(b))
	return string(vari), err
}

func readBinary(r reader) ([]byte, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	vari, err := readVariableType(r, amqpType(b))
	return vari, err
}

func readVariableType(r reader, of amqpType) ([]byte, error) {
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

func readHeaderSlice(r reader) (elements int, length int, _ error) {
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

		elements = int(elemByte)
		length = int(lByte)
	case typeCodeList32, typeCodeArray32:
		var l uint32
		err = binary.Read(r, binary.BigEndian, &l)
		if err != nil {
			return 0, 0, err
		}

		var elems uint32
		err = binary.Read(r, binary.BigEndian, &elems)
		if err != nil {
			return 0, 0, err
		}

		length = int(l)
		elements = int(elems)
	default:
		return 0, 0, errorErrorf("type code %x is not a recognized list type", b)
	}

	if elements > r.Len() {
		return 0, 0, errInvalidLength
	}
	return elements, length, nil
}

func readAny(r reader) (interface{}, error) {
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
	// bool
	case
		typeCodeBool,
		typeCodeBoolTrue,
		typeCodeBoolFalse:
		return readBool(r)

	// unsigned integers
	case
		typeCodeUbyte,
		typeCodeUshort,
		typeCodeUint,
		typeCodeSmallUint,
		typeCodeUint0,
		typeCodeUlong,
		typeCodeSmallUlong,
		typeCodeUlong0:
		return readUint(r)

	// signed integers
	case
		typeCodeByte,
		typeCodeShort,
		typeCodeInt,
		typeCodeSmallint,
		typeCodeLong,
		typeCodeSmalllong:
		return readInt(r)

	// binary
	case
		typeCodeVbin8,
		typeCodeVbin32:
		return readBinary(r)

	// strings
	case
		typeCodeStr8,
		typeCodeStr32,
		typeCodeSym8,
		typeCodeSym32:
		return readString(r)

	// timestamp
	case typeCodeTimestamp:
		return readTimestamp(r)

	// not-implemented
	case
		typeCodeFloat,
		typeCodeDouble,
		typeCodeDecimal32,
		typeCodeDecimal64,
		typeCodeDecimal128,
		typeCodeChar,
		typeCodeUUID,
		typeCodeList0,
		typeCodeList8,
		typeCodeList32,
		typeCodeMap8,
		typeCodeMap32,
		typeCodeArray8,
		typeCodeArray32:
		return nil, errorErrorf("%0x not implemented", b)

	default:
		return nil, errorErrorf("unknown type %0x", b)
	}
}

func readTimestamp(r reader) (time.Time, error) {
	b, err := r.ReadByte()
	if err != nil {
		return time.Time{}, err
	}

	switch t := amqpType(b); {
	case t == typeCodeNull:
		return time.Time{}, errNull
	case t != typeCodeTimestamp:
		return time.Time{}, errorErrorf("invalid type for timestamp %0x", t)
	}

	var n uint64
	err = binary.Read(r, binary.BigEndian, &n)
	rem := n % 1000
	return time.Unix(int64(n)/1000, int64(rem)*1000000).UTC(), err
}

func readInt(r reader) (value int, _ error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	switch amqpType(b) {
	// Unsigned
	case typeCodeUint0, typeCodeUlong0:
		return 0, nil
	case typeCodeUbyte, typeCodeSmallUint, typeCodeSmallUlong:
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

func readBool(r reader) (bool, error) {
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

func readUint(r reader) (value uint64, _ error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	switch amqpType(b) {
	case typeCodeNull:
		return 0, errNull
	case typeCodeUint0, typeCodeUlong0:
		return 0, nil
	case typeCodeUbyte, typeCodeSmallUint, typeCodeSmallUlong:
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

type limitReader struct {
	reader
	limit uint32
	read  uint32
}

var errLimitReached = errorNew("limit reached")

func (r *limitReader) Read(p []byte) (int, error) {
	if r.read >= r.limit {
		return 0, errLimitReached
	}
	n, err := r.reader.Read(p)
	r.read += uint32(n)
	return n, err
}

type mapReader struct {
	r     *limitReader
	count int // elements (2 * # of pairs)
	read  int
}

func (mr *mapReader) pairs() int {
	return mr.count / 2
}

func (mr *mapReader) more() bool {
	return mr.read < mr.count
}

func (mr *mapReader) next(key, value interface{}) error {
	_, err := unmarshal(mr.r, key)
	if err != nil {
		return err
	}
	mr.read++
	_, err = unmarshal(mr.r, value)
	if err != nil {
		return err
	}
	mr.read++
	return nil
}

func newMapReader(r reader) (*mapReader, error) {
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
		r:     &limitReader{reader: r, limit: n},
		count: int(b),
	}, nil
}
