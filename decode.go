package amqp

import (
	"encoding/binary"
	"errors"
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
func parseFrameHeader(r reader) (frameHeader, error) {
	if r.Len() < 8 {
		return frameHeader{}, errorNew("invalid frameHeader")
	}
	b := r.Next(8)
	return frameHeader{
		Size:       binary.BigEndian.Uint32(b[0:4]),
		DataOffset: b[4],
		FrameType:  b[5],
		Channel:    binary.BigEndian.Uint16(b[6:8]),
	}, nil
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

	switch pType {
	case typeCodeOpen:
		t := new(performOpen)
		err := t.unmarshal(r)
		return t, err
	case typeCodeBegin:
		t := new(performBegin)
		err := t.unmarshal(r)
		return t, err
	case typeCodeAttach:
		t := new(performAttach)
		err := t.unmarshal(r)
		return t, err
	case typeCodeFlow:
		t := new(performFlow)
		err := t.unmarshal(r)
		return t, err
	case typeCodeTransfer:
		t := new(performTransfer)
		err := t.unmarshal(r)
		return t, err
	case typeCodeDisposition:
		t := new(performDisposition)
		err := t.unmarshal(r)
		return t, err
	case typeCodeDetach:
		t := new(performDetach)
		err := t.unmarshal(r)
		return t, err
	case typeCodeEnd:
		t := new(performEnd)
		err := t.unmarshal(r)
		return t, err
	case typeCodeClose:
		t := new(performClose)
		err := t.unmarshal(r)
		return t, err
	case typeCodeSASLMechanism:
		t := new(saslMechanisms)
		err := t.unmarshal(r)
		return t, err
	case typeCodeSASLOutcome:
		t := new(saslOutcome)
		err := t.unmarshal(r)
		return t, err
	default:
		return nil, errorErrorf("unknown preformative type %0x", pType)
	}
}

// unmarshaler is fulfilled by types that can unmarshal
// themselves from AMQP data.
type unmarshaler interface {
	unmarshal(r reader) error
}

// constantUnmarshaler allows the returned type to be a non-pointer
type constantUnmarshaler interface {
	unmarshalConstant(r reader) error
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
	case constantUnmarshaler: // must be after unmarshaler
		return false, t.unmarshalConstant(r)
	case *int:
		val, err := readInt(r)
		if err != nil {
			return isNull, err
		}
		*t = val
	case *int8:
		val, err := readInt(r)
		if err != nil {
			return isNull, err
		}
		*t = int8(val)
	case *int16:
		val, err := readInt(r)
		if err != nil {
			return isNull, err
		}
		*t = int16(val)
	case *int32:
		val, err := readInt(r)
		if err != nil {
			return isNull, err
		}
		*t = int32(val)
	case *int64:
		val, err := readInt(r)
		if err != nil {
			return isNull, err
		}
		*t = int64(val)
	case *uint64:
		val, err := readUint(r)
		if err != nil {
			return isNull, err
		}
		*t = val
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
		// handle **T
		v := reflect.Indirect(reflect.ValueOf(i))

		// can't unmarshal into a non-pointer
		if v.Kind() != reflect.Ptr {
			return isNull, errorErrorf("unable to unmarshal %T", i)
		}

		// if the value being unmarshaled is null,
		// skip the rest and set to nil
		b, err := r.ReadByte()
		if err != nil {
			return isNull, err
		}
		if amqpType(b) == typeCodeNull {
			v.Set(reflect.Zero(v.Type()))
			return true, nil
		}
		r.UnreadByte()

		// if nil pointer, allocate a new value to
		// unmarshal into
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}

		return unmarshal(r, v.Interface())
	}
	return isNull, nil
}

// unmarshalComposite is a helper for use in a composite's unmarshal() function.
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

	// composites always start with 0x0
	if byt != 0 {
		return 0, 0, errorErrorf("invalid composite header %0x", byt)
	}

	// next, the composite type is encoded as an AMQP uint8
	v, err := readUlong(r)
	if err != nil {
		return 0, 0, err
	}

	// fields are represented as a list
	fields, err = readHeaderSlice(r)

	return amqpType(v), fields, err
}

func readSymbolArray(r reader) ([]symbol, error) {
	lElems, err := readHeaderSlice(r)
	if err != nil {
		return nil, err
	}

	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	strs := make([]symbol, lElems)
	for i := 0; i < lElems; i++ {
		vari, err := readVariableType(r, amqpType(b))
		if err != nil {
			return nil, err
		}
		strs[i] = symbol(vari)
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
	return append([]byte(nil), vari...), err // copy bytes as it shares backing with r
}

func readUUID(r reader) (UUID, error) {
	var uuid UUID

	b, err := r.ReadByte()
	if err != nil {
		return uuid, err
	}

	if amqpType(b) != typeCodeUUID {
		return uuid, errorErrorf("type code %#00x is not a UUID", b)
	}

	n, err := r.Read(uuid[:])
	if err != nil {
		return uuid, err
	}
	if n != 16 {
		return uuid, errInvalidLength
	}

	return uuid, nil
}

// readVariableType reads binary, strings and symbols
//
// returned bytes are only valid until the next use of r.
func readVariableType(r reader, of amqpType) ([]byte, error) {
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
		return r.Next(int(n)), nil
	case typeCodeVbin32, typeCodeStr32, typeCodeSym32:
		if r.Len() < 4 {
			return nil, errorErrorf("invalid length for type %0x", of)
		}
		n := binary.BigEndian.Uint32(r.Next(4))
		if uint64(n) > uint64(r.Len()) {
			return nil, errInvalidLength
		}
		return r.Next(int(n)), nil
	default:
		return nil, errorErrorf("type code %#00x is not a recognized variable length type", of)
	}
}

func readHeaderSlice(r reader) (elements int, _ error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	switch amqpType(b) {
	case typeCodeNull:
		return 0, errNull
	case typeCodeList0:
		return 0, nil
	case typeCodeList8, typeCodeArray8:
		l, err := r.ReadByte()
		if err != nil {
			return 0, err
		}

		if int(l) > r.Len() {
			return 0, errorErrorf("invalid length for type %0x", b)
		}

		elemByte, err := r.ReadByte()
		if err != nil {
			return 0, err
		}

		elements = int(elemByte)
	case typeCodeList32, typeCodeArray32:
		if r.Len() < 4 {
			return 0, errorErrorf("invalid length for type %0x", b)
		}
		l := binary.BigEndian.Uint32(r.Next(4))
		if int(l) > r.Len() {
			return 0, errorErrorf("invalid length for type %0x", b)
		}

		if r.Len() < 4 {
			return 0, errorErrorf("invalid length for type %0x", b)
		}
		elements = int(binary.BigEndian.Uint32(r.Next(4)))
	default:
		return 0, errorErrorf("type code %x is not a recognized list type", b)
	}

	if elements > r.Len() {
		return 0, errInvalidLength
	}
	return elements, nil
}

func init() {
	// break initialization cycle by assigning in init
	// typeReaders -> readComposite -> unmarshal -> readAny -> typeReaders
	typeReaders[0x0] = readComposite

	typeReaders[typeCodeMap8] = func(r reader) (interface{}, error) { return readMap(r) }
	typeReaders[typeCodeMap32] = func(r reader) (interface{}, error) { return readMap(r) }
}

var typeReaders = [256]func(r reader) (interface{}, error){
	// bool
	typeCodeBool:      func(r reader) (interface{}, error) { return readBool(r) },
	typeCodeBoolTrue:  func(r reader) (interface{}, error) { return readBool(r) },
	typeCodeBoolFalse: func(r reader) (interface{}, error) { return readBool(r) },
	// uint
	typeCodeUbyte:      func(r reader) (interface{}, error) { return readUint(r) },
	typeCodeUshort:     func(r reader) (interface{}, error) { return readUint(r) },
	typeCodeUint:       func(r reader) (interface{}, error) { return readUint(r) },
	typeCodeSmallUint:  func(r reader) (interface{}, error) { return readUint(r) },
	typeCodeUint0:      func(r reader) (interface{}, error) { return readUint(r) },
	typeCodeUlong:      func(r reader) (interface{}, error) { return readUint(r) },
	typeCodeSmallUlong: func(r reader) (interface{}, error) { return readUint(r) },
	typeCodeUlong0:     func(r reader) (interface{}, error) { return readUint(r) },
	// int
	typeCodeByte:      func(r reader) (interface{}, error) { return readInt(r) },
	typeCodeShort:     func(r reader) (interface{}, error) { return readInt(r) },
	typeCodeInt:       func(r reader) (interface{}, error) { return readInt(r) },
	typeCodeSmallint:  func(r reader) (interface{}, error) { return readInt(r) },
	typeCodeLong:      func(r reader) (interface{}, error) { return readInt(r) },
	typeCodeSmalllong: func(r reader) (interface{}, error) { return readInt(r) },
	// binary
	typeCodeVbin8:  func(r reader) (interface{}, error) { return readBinary(r) },
	typeCodeVbin32: func(r reader) (interface{}, error) { return readBinary(r) },
	// strings
	typeCodeStr8:  func(r reader) (interface{}, error) { return readString(r) },
	typeCodeStr32: func(r reader) (interface{}, error) { return readString(r) },
	typeCodeSym8:  func(r reader) (interface{}, error) { return readString(r) },
	typeCodeSym32: func(r reader) (interface{}, error) { return readString(r) },
	// timestamp
	typeCodeTimestamp: func(r reader) (interface{}, error) { return readTimestamp(r) },
	// UUID
	typeCodeUUID: func(r reader) (interface{}, error) { return readUUID(r) },

	// arrays
	typeCodeArray8:  func(r reader) (interface{}, error) { return readArray(r) },
	typeCodeArray32: func(r reader) (interface{}, error) { return readArray(r) },

	// TODO: implement
	typeCodeFloat:      func(r reader) (interface{}, error) { return nil, errorNew("float not implemented") },
	typeCodeDouble:     func(r reader) (interface{}, error) { return nil, errorNew("double not implemented") },
	typeCodeDecimal32:  func(r reader) (interface{}, error) { return nil, errorNew("decimal32 not implemented") },
	typeCodeDecimal64:  func(r reader) (interface{}, error) { return nil, errorNew("decimal64 not implemented") },
	typeCodeDecimal128: func(r reader) (interface{}, error) { return nil, errorNew("decimal128 not implemented") },
	typeCodeChar:       func(r reader) (interface{}, error) { return nil, errorNew("char not implemented") },
	typeCodeList0:      func(r reader) (interface{}, error) { return nil, errorNew("list0 not implemented") },
	typeCodeList8:      func(r reader) (interface{}, error) { return nil, errorNew("list8 not implemented") },
	typeCodeList32:     func(r reader) (interface{}, error) { return nil, errorNew("list32 not implemented") },
}

func readMap(r reader) (map[interface{}]interface{}, error) {
	m := mapAnyAny{}
	if err := m.unmarshal(r); err != nil {
		return nil, err
	}
	return map[interface{}]interface{}(m), nil
}

func readArray(r reader) (interface{}, error) {
	lElems, err := readHeaderSlice(r)
	if err != nil {
		return nil, err
	}

	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch amqpType(b) {
	case typeCodeStr8, typeCodeStr32:
		elems := make([]string, lElems)
		for i := 0; i < lElems; i++ {
			vari, err := readVariableType(r, amqpType(b))
			if err != nil {
				return nil, err
			}
			elems[i] = string(vari)
		}
		return elems, nil
	case typeCodeVbin8, typeCodeVbin32:
		elems := make([][]byte, lElems)
		for i := 0; i < lElems; i++ {
			vari, err := readVariableType(r, amqpType(b))
			if err != nil {
				return nil, err
			}
			elems[i] = vari
		}
		return elems, nil
	case typeCodeUint:
		elems := make([]uint32, lElems)
		for i := 0; i < lElems; i++ {
			v, err := readUint32(r, false)
			if err != nil {
				return nil, err
			}
			elems[i] = v
		}
		return elems, nil
	default:
		return nil, errors.New("not supported")
	}
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

	tr := typeReaders[amqpType(b)]
	if tr == nil {
		return nil, errorErrorf("unknown type %0x", b)
	}
	return tr(r)
}

// roReader is similar to bytes.Reader but does not support rune
// operations and add Next() to fulfill the reader interface.
type roReader struct {
	b []byte
	i int
}

func (r *roReader) Read(b []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(b, r.b[r.i:])
	r.i += n
	return n, nil
}
func (r *roReader) ReadByte() (byte, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	b := r.b[r.i]
	r.i++
	return b, nil
}

func (r *roReader) UnreadByte() error {
	if r.i < 1 {
		return errorNew("at beginning of slice")
	}
	r.i--
	return nil
}

func (r *roReader) Bytes() []byte {
	return r.b[r.i:]
}

func (r *roReader) Len() int {
	return len(r.b) - r.i
}

func (r *roReader) Next(n int) []byte {
	if r.i+n > len(r.b) {
		n = len(r.b)
	}
	return r.b[r.i : r.i+n]
}

func readComposite(r reader) (interface{}, error) {
	// create separate reader to determine type without advancing
	// the original
	peekReader := &roReader{b: r.Bytes()}

	byt, err := peekReader.ReadByte()
	if err != nil {
		return nil, err
	}

	// could be null instead of a composite header
	if amqpType(byt) == typeCodeNull {
		return nil, errNull
	}

	// composites always start with 0x0
	if byt != 0 {
		return nil, errorErrorf("invalid composite header %0x", byt)
	}

	var iface interface{}

	// next, the composite type is encoded as an AMQP ulong
	v, err := readUlong(peekReader)

	// check if known composite type
	if err == nil && compositeTypes[amqpType(v)] != nil {
		iface = compositeTypes[amqpType(v)]()
	} else {
		iface = new(describedType) // try as described type
	}

	_, err = unmarshal(r, iface)
	return iface, err
}

var compositeTypes = [256]func() interface{}{
	typeCodeError: func() interface{} { return new(Error) },
	// Lifetime Policies
	typeCodeDeleteOnClose:             func() interface{} { return deleteOnClose },
	typeCodeDeleteOnNoMessages:        func() interface{} { return deleteOnNoMessages },
	typeCodeDeleteOnNoLinks:           func() interface{} { return deleteOnNoLinks },
	typeCodeDeleteOnNoLinksOrMessages: func() interface{} { return deleteOnNoLinksOrMessages },
	// Delivery States
	typeCodeStateAccepted: func() interface{} { return new(stateAccepted) },
	typeCodeStateModified: func() interface{} { return new(stateModified) },
	typeCodeStateReceived: func() interface{} { return new(stateReceived) },
	typeCodeStateRejected: func() interface{} { return new(stateRejected) },
	typeCodeStateReleased: func() interface{} { return new(stateReleased) },

	typeCodeOpen:                  notImplementedConstructor("typeCodeOpen"),
	typeCodeBegin:                 notImplementedConstructor("typeCodeBegin"),
	typeCodeAttach:                notImplementedConstructor("typeCodeAttach"),
	typeCodeFlow:                  notImplementedConstructor("typeCodeFlow"),
	typeCodeTransfer:              notImplementedConstructor("typeCodeTransfer"),
	typeCodeDisposition:           notImplementedConstructor("typeCodeDisposition"),
	typeCodeDetach:                notImplementedConstructor("typeCodeDetach"),
	typeCodeEnd:                   notImplementedConstructor("typeCodeEnd"),
	typeCodeClose:                 notImplementedConstructor("typeCodeClose"),
	typeCodeSource:                notImplementedConstructor("typeCodeSource"),
	typeCodeTarget:                notImplementedConstructor("typeCodeTarget"),
	typeCodeMessageHeader:         notImplementedConstructor("typeCodeMessageHeader"),
	typeCodeDeliveryAnnotations:   notImplementedConstructor("typeCodeDeliveryAnnotations"),
	typeCodeMessageAnnotations:    notImplementedConstructor("typeCodeMessageAnnotations"),
	typeCodeMessageProperties:     notImplementedConstructor("typeCodeMessageProperties"),
	typeCodeApplicationProperties: notImplementedConstructor("typeCodeApplicationProperties"),
	typeCodeApplicationData:       notImplementedConstructor("typeCodeApplicationData"),
	typeCodeAMQPSequence:          notImplementedConstructor("typeCodeAMQPSequence"),
	typeCodeAMQPValue:             notImplementedConstructor("typeCodeAMQPValue"),
	typeCodeFooter:                notImplementedConstructor("typeCodeFooter"),
	typeCodeSASLMechanism:         notImplementedConstructor("typeCodeSASLMechanism"),
	typeCodeSASLInit:              notImplementedConstructor("typeCodeSASLInit"),
	typeCodeSASLChallenge:         notImplementedConstructor("typeCodeSASLChallenge"),
	typeCodeSASLResponse:          notImplementedConstructor("typeCodeSASLResponse"),
	typeCodeSASLOutcome:           notImplementedConstructor("typeCodeSASLOutcome"),
}

type notImplemented string

func (ni notImplemented) unmarshal(r reader) error {
	return errorNew("readComposite unmarshal not implemented for " + string(ni))
}

func notImplementedConstructor(s string) func() interface{} {
	return func() interface{} { return notImplemented(s) }
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

	if r.Len() < 8 {
		return time.Time{}, errorErrorf("invalid length for timestamp")
	}
	n := binary.BigEndian.Uint64(r.Next(8))
	rem := n % 1000
	return time.Unix(int64(n)/1000, int64(rem)*1000000).UTC(), err
}

func readInt(r reader) (int, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	r.UnreadByte()

	switch amqpType(b) {
	// Unsigned
	case typeCodeUbyte:
		n, err := readUbyte(r)
		return int(n), err
	case typeCodeUshort:
		n, err := readUlong(r)
		return int(n), err
	case typeCodeUint0, typeCodeSmallUint, typeCodeUint:
		n, err := readUint32(r, true)
		return int(n), err
	case typeCodeUlong0, typeCodeSmallUlong, typeCodeUlong:
		n, err := readUlong(r)
		return int(n), err

	// Signed
	case typeCodeByte:
		n, err := readSbyte(r)
		return int(n), err
	case typeCodeShort:
		n, err := readShort(r)
		return int(n), err
	case typeCodeSmallint, typeCodeInt:
		n, err := readInt32(r)
		return int(n), err
	case typeCodeSmalllong, typeCodeLong:
		n, err := readLong(r)
		return int(n), err
	default:
		return 0, errorErrorf("type code %x is not a recognized number type", b)
	}
}

func readLong(r reader) (int64, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if amqpType(b) == typeCodeSmalllong {
		n, err := r.ReadByte()
		return int64(n), err
	}
	if amqpType(b) != typeCodeLong {
		return 0, errorErrorf("invalid type for uint32 %0x", b)
	}
	if r.Len() < 8 {
		return 0, errorNew("invalid ulong")
	}
	n := binary.BigEndian.Uint64(r.Next(8))
	return int64(n), nil
}

func readInt32(r reader) (int32, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if amqpType(b) == typeCodeSmallint {
		n, err := r.ReadByte()
		return int32(n), err
	}
	if amqpType(b) != typeCodeInt {
		return 0, errorErrorf("invalid type for int32 %0x", b)
	}
	if r.Len() < 4 {
		return 0, errorNew("invalid int")
	}
	n := binary.BigEndian.Uint32(r.Next(4))
	return int32(n), nil
}

func readShort(r reader) (int16, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if amqpType(b) != typeCodeShort {
		return 0, errorErrorf("invalid type for short %0x", b)
	}
	if r.Len() < 2 {
		return 0, errorNew("invalid sshort")
	}
	n := binary.BigEndian.Uint16(r.Next(2))
	return int16(n), nil
}

func readSbyte(r reader) (int8, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if amqpType(b) != typeCodeByte {
		return 0, errorErrorf("invalid type for int8 %0x", b)
	}
	n, err := r.ReadByte()
	return int8(n), err
}

func readUbyte(r reader) (uint8, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if amqpType(b) != typeCodeUbyte {
		return 0, errorErrorf("invalid type for ubyte %0x", b)
	}
	return r.ReadByte()
}

func readUshort(r reader) (uint16, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if amqpType(b) != typeCodeUshort {
		return 0, errorErrorf("invalid type for ushort %0x", b)
	}
	if r.Len() < 2 {
		return 0, errorNew("invalid ushort")
	}
	return binary.BigEndian.Uint16(r.Next(2)), nil
}

func readUint32(r reader, readType bool) (uint32, error) {
	if readType {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		if amqpType(b) == typeCodeUint0 {
			return 0, nil
		}
		if amqpType(b) == typeCodeSmallUint {
			n, err := r.ReadByte()
			return uint32(n), err
		}
		if amqpType(b) != typeCodeUint {
			return 0, errorErrorf("invalid type for uint32 %0x", b)
		}
	}
	if r.Len() < 4 {
		return 0, errorNew("invalid uint")
	}
	return binary.BigEndian.Uint32(r.Next(4)), nil
}

func readUlong(r reader) (uint64, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	if amqpType(b) == typeCodeUlong0 {
		return 0, nil
	}
	if amqpType(b) == typeCodeSmallUlong {
		n, err := r.ReadByte()
		return uint64(n), err
	}
	if amqpType(b) != typeCodeUlong {
		return 0, errorErrorf("invalid type for uint32 %0x", b)
	}
	if r.Len() < 8 {
		return 0, errorNew("invalid ulong")
	}
	return binary.BigEndian.Uint64(r.Next(8)), nil
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
		if r.Len() < 2 {
			return 0, errorNew("invalid ushort")
		}
		n := binary.BigEndian.Uint16(r.Next(2))
		return uint64(n), err
	case typeCodeUint:
		if r.Len() < 4 {
			return 0, errorNew("invalid uint")
		}
		n := binary.BigEndian.Uint32(r.Next(4))
		return uint64(n), err
	case typeCodeUlong:
		if r.Len() < 8 {
			return 0, errorNew("invalid ulong")
		}
		n := binary.BigEndian.Uint64(r.Next(8))
		return n, err

	default:
		return 0, errorErrorf("type code %x is not a recognized number type", b)
	}
}

func readMapHeader(r reader) (size uint32, count uint8, _ error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	switch amqpType(b) {
	case typeCodeNull:
		return 0, 0, errNull
	case typeCodeMap8:
		bn, err := r.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		size = uint32(bn)
	case typeCodeMap32:
		if r.Len() < 4 {
			return 0, 0, errInvalidLength
		}
		size = binary.BigEndian.Uint32(r.Next(4))
	default:
		return 0, 0, errorErrorf("invalid map type %x", b)
	}

	if uint64(size) > uint64(r.Len()) {
		return 0, 0, errInvalidLength
	}

	count, err = r.ReadByte()
	return size, count, err
}
