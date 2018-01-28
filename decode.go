package amqp

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
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

// sentinel error indicating that a decoded value is null
var errNull = errors.New("error is null")

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
		return nil, errorErrorf("unknown preformative type %02x", pType)
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
		val, err := readSbyte(r)
		if err != nil {
			return isNull, err
		}
		*t = val
	case *int16:
		val, err := readShort(r)
		if err != nil {
			return isNull, err
		}
		*t = val
	case *int32:
		val, err := readInt32(r)
		if err != nil {
			return isNull, err
		}
		*t = val
	case *int64:
		val, err := readLong(r)
		if err != nil {
			return isNull, err
		}
		*t = val
	case *uint64:
		val, err := readUlong(r)
		if err != nil {
			return isNull, err
		}
		*t = val
	case *uint32:
		val, err := readUint32(r)
		if err != nil {
			return isNull, err
		}
		*t = val
	case *uint16:
		val, err := readUshort(r)
		if err != nil {
			return isNull, err
		}
		*t = val
	case *uint8:
		val, err := readUbyte(r)
		if err != nil {
			return isNull, err
		}
		*t = val
	case *float32:
		val, err := readFloat(r)
		if err != nil {
			return isNull, err
		}
		*t = val
	case *float64:
		val, err := readDouble(r)
		if err != nil {
			return isNull, err
		}
		*t = val
	case *string:
		val, err := readString(r)
		if err != nil {
			return isNull, err
		}
		*t = val
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
	case *[]int8:
		err = (*arrayInt8)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *[]uint16:
		err = (*arrayUint16)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *[]int16:
		err = (*arrayInt16)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *[]uint32:
		err = (*arrayUint32)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *[]int32:
		err = (*arrayInt32)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *[]uint64:
		err = (*arrayUint64)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *[]int64:
		err = (*arrayInt64)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *[]float32:
		err = (*arrayFloat)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *[]float64:
		err = (*arrayDouble)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *[]bool:
		err = (*arrayBool)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *[]string:
		err = (*arrayString)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *[]symbol:
		err = (*arraySymbol)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *[][]byte:
		err = (*arrayBinary)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *[]time.Time:
		err = (*arrayTimestamp)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *[]UUID:
		err = (*arrayUUID)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *[]interface{}:
		err = (*list)(t).unmarshal(r)
		if err != nil {
			return isNull, err
		}
	case *map[interface{}]interface{}:
		return isNull, (*mapAnyAny)(t).unmarshal(r)
	case *map[string]interface{}:
		return isNull, (*mapStringAny)(t).unmarshal(r)
	case *map[symbol]interface{}:
		return isNull, (*mapSymbolAny)(t).unmarshal(r)
	case *deliveryState:
		type_, err := peekMessageType(r.Bytes())
		if err != nil {
			if err == errNull {
				_, err = r.ReadByte() // if null, discard nullCode
				return true, err
			}
			return false, err
		}

		switch amqpType(type_) {
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
			return false, errorErrorf("unexpected type %d for deliveryState", type_)
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
		type_, err := r.ReadByte()
		if err != nil {
			return isNull, err
		}
		if amqpType(type_) == typeCodeNull {
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
	type_, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	// could be null instead of a composite header
	if amqpType(type_) == typeCodeNull {
		return 0, 0, errNull
	}

	// compsites always start with 0x0
	if type_ != 0 {
		return 0, 0, errorErrorf("invalid composite header %#02x", type_)
	}

	// next, the composite type is encoded as an AMQP uint8
	v, err := readUlong(r)
	if err != nil {
		return 0, 0, err
	}

	// fields are represented as a list
	_, fields, err = readListHeader(r)

	return amqpType(v), fields, err
}

func readListHeader(r reader) (size, length int, _ error) {
	type_, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	listLength := r.Len()

	switch amqpType(type_) {
	case typeCodeNull:
		return 0, 0, errNull
	case typeCodeList0:
		return 0, 0, nil
	case typeCodeList8:
		if listLength < 2 {
			return 0, 0, errorNew("invalid length")
		}
		buf := r.Next(2)
		size = int(buf[0])
		if int(size) > listLength-1 {
			return 0, 0, errorNew("invalid length")
		}
		length = int(buf[1])
	case typeCodeList32:
		if listLength < 8 {
			return 0, 0, errorNew("invalid length")
		}
		buf := r.Next(8)
		size = int(binary.BigEndian.Uint32(buf[:4]))
		if size > listLength-4 {
			return 0, 0, errorNew("invalid length")
		}
		length = int(binary.BigEndian.Uint32(buf[4:]))
	default:
		return 0, 0, errorErrorf("type code %#02x is not a recognized list type", type_)
	}

	return size, length, nil
}

func readArrayHeader(r reader) (size, length int, _ error) {
	type_, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	arrayLength := r.Len()

	switch amqpType(type_) {
	case typeCodeNull:
		return 0, 0, errNull
	case typeCodeArray8:
		if arrayLength < 2 {
			return 0, 0, errorNew("invalid length")
		}
		bytes := r.Next(2)
		size = int(bytes[0])
		if int(size) > arrayLength-1 {
			return 0, 0, errorNew("invalid length")
		}
		length = int(bytes[1])
	case typeCodeArray32:
		if arrayLength < 8 {
			return 0, 0, errorNew("invalid length")
		}
		l := binary.BigEndian.Uint32(r.Next(4))
		if int(l) > arrayLength-4 {
			return 0, 0, errorErrorf("invalid length for type %02x", type_)
		}
		length = int(binary.BigEndian.Uint32(r.Next(4)))
	default:
		return 0, 0, errorErrorf("type code %#02x is not a recognized list type", type_)
	}
	return size, length, nil
}

func readString(r reader) (string, error) {
	type_, err := r.ReadByte()
	if err != nil {
		return "", err
	}

	var length int
	switch amqpType(type_) {
	case typeCodeNull:
		return "", errNull
	case typeCodeStr8, typeCodeSym8:
		n, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		length = int(n)
	case typeCodeStr32, typeCodeSym32:
		if r.Len() < 4 {
			return "", errorErrorf("invalid length for type %#02x", type_)
		}
		length = int(binary.BigEndian.Uint32(r.Next(4)))
	default:
		return "", errorErrorf("type code %#02x is not a recognized string type", type_)
	}

	if length > r.Len() {
		return "", errorNew("invalid length")
	}
	return string(r.Next(length)), nil
}

func readBinary(r reader) ([]byte, error) {
	type_, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	var length int
	switch amqpType(type_) {
	case typeCodeNull:
		return nil, errNull
	case typeCodeVbin8:
		n, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		length = int(n)
	case typeCodeVbin32:
		if r.Len() < 4 {
			return nil, errorErrorf("invalid length for type %#02x", type_)
		}
		length = int(binary.BigEndian.Uint32(r.Next(4)))
	default:
		return nil, errorErrorf("type code %#02x is not a recognized string type", type_)
	}

	if length > r.Len() {
		return nil, errorNew("invalid length")
	}
	return append([]byte(nil), r.Next(length)...), nil
}

func readAny(r reader) (interface{}, error) {
	if r.Len() < 1 {
		return nil, errorNew("invalid length")
	}

	type_ := amqpType(r.Bytes()[0])
	switch type_ {
	case typeCodeNull:
		r.ReadByte()
		return nil, errNull

	// composite
	case 0x0:
		return readComposite(r)

	// bool
	case typeCodeBool, typeCodeBoolTrue, typeCodeBoolFalse:
		return readBool(r)

	// uint
	case typeCodeUbyte:
		return readUbyte(r)
	case typeCodeUshort:
		return readUshort(r)
	case typeCodeUint,
		typeCodeSmallUint,
		typeCodeUint0:
		return readUint32(r)
	case typeCodeUlong,
		typeCodeSmallUlong,
		typeCodeUlong0:
		return readUlong(r)

	// int
	case typeCodeByte:
		return readSbyte(r)
	case typeCodeShort:
		return readShort(r)
	case typeCodeInt,
		typeCodeSmallint:
		return readInt32(r)
	case typeCodeLong,
		typeCodeSmalllong:
		return readLong(r)

	// floating point
	case typeCodeFloat:
		return readFloat(r)
	case typeCodeDouble:
		return readDouble(r)

	// binary
	case typeCodeVbin8, typeCodeVbin32:
		return readBinary(r)

	// strings
	case typeCodeStr8, typeCodeStr32:
		return readString(r)
	case typeCodeSym8, typeCodeSym32:
		// symbols currently decoded as string to avoid
		// exposing symbol type in message, this may need
		// to change if users need to distinguish strings
		// from symbols
		return readString(r)

	// timestamp
	case typeCodeTimestamp:
		return readTimestamp(r)

	// UUID
	case typeCodeUUID:
		return readUUID(r)

	// arrays
	case typeCodeArray8, typeCodeArray32:
		return readAnyArray(r)

	// lists
	case typeCodeList0, typeCodeList8, typeCodeList32:
		return readAnyList(r)

	// maps
	case typeCodeMap8:
		return readAnyMap(r)
	case typeCodeMap32:
		return readAnyMap(r)

	// TODO: implement
	case typeCodeDecimal32:
		return nil, errorNew("decimal32 not implemented")
	case typeCodeDecimal64:
		return nil, errorNew("decimal64 not implemented")
	case typeCodeDecimal128:
		return nil, errorNew("decimal128 not implemented")
	case typeCodeChar:
		return nil, errorNew("char not implemented")
	default:
		return nil, errorErrorf("unknown type %#02x", type_)
	}
}

func readAnyMap(r reader) (interface{}, error) {
	var m map[interface{}]interface{}
	err := (*mapAnyAny)(&m).unmarshal(r)
	if err != nil {
		return nil, err
	}

	if len(m) == 0 {
		return m, nil
	}

	stringKeys := true
Loop:
	for key, _ := range m {
		switch key.(type) {
		case string:
		case symbol:
		default:
			stringKeys = false
			break Loop
		}
	}

	if stringKeys {
		mm := make(map[string]interface{}, len(m))
		for key, value := range m {
			switch key := key.(type) {
			case string:
				mm[key] = value
			case symbol:
				mm[string(key)] = value
			}
		}
		return mm, nil
	}

	return m, nil
}

func readAnyList(r reader) (interface{}, error) {
	var a []interface{}
	err := (*list)(&a).unmarshal(r)
	return a, err
}

func readAnyArray(r reader) (interface{}, error) {
	// get the array type
	buf := r.Bytes()
	if len(buf) < 1 {
		return 0, errorNew("invalid length")
	}
	var typeIdx int
	switch amqpType(buf[0]) {
	case typeCodeArray8:
		typeIdx = 3
	case typeCodeArray32:
		typeIdx = 9
	default:
		return 0, errorErrorf("invalid array type %02x", buf[0])
	}
	if len(buf) < typeIdx+1 {
		return 0, errorNew("invalid length")
	}

	switch amqpType(buf[typeIdx]) {
	case typeCodeByte:
		var a []int8
		err := (*arrayInt8)(&a).unmarshal(r)
		return a, err
	case typeCodeUbyte:
		var a ArrayUByte
		err := a.unmarshal(r)
		return a, err
	case typeCodeUshort:
		var a []uint16
		err := (*arrayUint16)(&a).unmarshal(r)
		return a, err
	case typeCodeShort:
		var a []int16
		err := (*arrayInt16)(&a).unmarshal(r)
		return a, err
	case typeCodeUint0, typeCodeSmallUint, typeCodeUint:
		var a []uint32
		err := (*arrayUint32)(&a).unmarshal(r)
		return a, err
	case typeCodeSmallint, typeCodeInt:
		var a []int32
		err := (*arrayInt32)(&a).unmarshal(r)
		return a, err
	case typeCodeUlong0, typeCodeSmallUlong, typeCodeUlong:
		var a []uint64
		err := (*arrayUint64)(&a).unmarshal(r)
		return a, err
	case typeCodeSmalllong, typeCodeLong:
		var a []int64
		err := (*arrayInt64)(&a).unmarshal(r)
		return a, err
	case typeCodeFloat:
		var a []float32
		err := (*arrayFloat)(&a).unmarshal(r)
		return a, err
	case typeCodeDouble:
		var a []float64
		err := (*arrayDouble)(&a).unmarshal(r)
		return a, err
	case typeCodeBool, typeCodeBoolTrue, typeCodeBoolFalse:
		var a []bool
		err := (*arrayBool)(&a).unmarshal(r)
		return a, err
	case typeCodeStr8, typeCodeStr32:
		var a []string
		err := (*arrayString)(&a).unmarshal(r)
		return a, err
	case typeCodeSym8, typeCodeSym32:
		var a []symbol
		err := (*arraySymbol)(&a).unmarshal(r)
		return a, err
	case typeCodeVbin8, typeCodeVbin32:
		var a [][]byte
		err := (*arrayBinary)(&a).unmarshal(r)
		return a, err
	case typeCodeTimestamp:
		var a []time.Time
		err := (*arrayTimestamp)(&a).unmarshal(r)
		return a, err
	case typeCodeUUID:
		var a []UUID
		err := (*arrayUUID)(&a).unmarshal(r)
		return a, err
	default:
		return nil, errorErrorf("array decoding not implemented for %#02x", buf[typeIdx])
	}
}

func readComposite(r reader) (interface{}, error) {
	buf := r.Bytes()

	if len(buf) < 2 {
		return nil, errorNew("invalid length for composite")
	}

	switch amqpType(buf[0]) {
	case 0x0:
		// compsites start with 0x0
	case typeCodeNull:
		return nil, errNull
	default:
		return nil, errorErrorf("invalid composite header %#02x", buf[0])
	}

	var compositeType uint64
	switch amqpType(buf[1]) {
	case typeCodeSmallUlong:
		if len(buf) < 3 {
			return nil, errorNew("invalid length for smallulong")
		}
		compositeType = uint64(buf[2])
	case typeCodeUlong:
		if len(buf) < 10 {
			return nil, errorNew("invalid length for ulong")
		}
		compositeType = uint64(binary.BigEndian.Uint64(buf[2:]))
	}

	if compositeType > math.MaxUint8 {
		// try as described type
		var dt describedType
		err := dt.unmarshal(r)
		return dt, err
	}

	switch amqpType(compositeType) {
	// Error
	case typeCodeError:
		t := new(Error)
		err := t.unmarshal(r)
		return t, err

	// Lifetime Policies
	case typeCodeDeleteOnClose:
		t := deleteOnClose
		err := t.unmarshal(r)
		return t, err
	case typeCodeDeleteOnNoMessages:
		t := deleteOnNoMessages
		err := t.unmarshal(r)
		return t, err
	case typeCodeDeleteOnNoLinks:
		t := deleteOnNoLinks
		err := t.unmarshal(r)
		return t, err
	case typeCodeDeleteOnNoLinksOrMessages:
		t := deleteOnNoLinksOrMessages
		err := t.unmarshal(r)
		return t, err

	// Delivery States
	case typeCodeStateAccepted:
		t := new(stateAccepted)
		err := t.unmarshal(r)
		return t, err
	case typeCodeStateModified:
		t := new(stateModified)
		err := t.unmarshal(r)
		return t, err
	case typeCodeStateReceived:
		t := new(stateReceived)
		err := t.unmarshal(r)
		return t, err
	case typeCodeStateRejected:
		t := new(stateRejected)
		err := t.unmarshal(r)
		return t, err
	case typeCodeStateReleased:
		t := new(stateReleased)
		err := t.unmarshal(r)
		return t, err

	case typeCodeOpen,
		typeCodeBegin,
		typeCodeAttach,
		typeCodeFlow,
		typeCodeTransfer,
		typeCodeDisposition,
		typeCodeDetach,
		typeCodeEnd,
		typeCodeClose,
		typeCodeSource,
		typeCodeTarget,
		typeCodeMessageHeader,
		typeCodeDeliveryAnnotations,
		typeCodeMessageAnnotations,
		typeCodeMessageProperties,
		typeCodeApplicationProperties,
		typeCodeApplicationData,
		typeCodeAMQPSequence,
		typeCodeAMQPValue,
		typeCodeFooter,
		typeCodeSASLMechanism,
		typeCodeSASLInit,
		typeCodeSASLChallenge,
		typeCodeSASLResponse,
		typeCodeSASLOutcome:
		return nil, errorErrorf("readComposite unmarshal not implemented for %#02x", compositeType)

	default:
		// try as described type
		var dt describedType
		err := dt.unmarshal(r)
		return dt, err
	}
}

func readTimestamp(r reader) (time.Time, error) {
	type_, err := r.ReadByte()
	if err != nil {
		return time.Time{}, err
	}

	switch amqpType(type_) {
	case typeCodeTimestamp:
	case typeCodeNull:
		return time.Time{}, errNull
	default:
		return time.Time{}, errorErrorf("invalid type for timestamp %02x", type_)
	}

	if r.Len() < 8 {
		return time.Time{}, errorErrorf("invalid length for timestamp")
	}
	n := int64(binary.BigEndian.Uint64(r.Next(8)))
	rem := n % 1000
	return time.Unix(int64(n)/1000, int64(rem)*1000000).UTC(), err
}

func readInt(r reader) (int, error) {
	type_, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	r.UnreadByte()

	switch amqpType(type_) {
	case typeCodeNull:
		return 0, errNull

	// Unsigned
	case typeCodeUbyte:
		n, err := readUbyte(r)
		return int(n), err
	case typeCodeUshort:
		n, err := readUshort(r)
		return int(n), err
	case typeCodeUint0, typeCodeSmallUint, typeCodeUint:
		n, err := readUint32(r)
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
		return 0, errorErrorf("type code %#02x is not a recognized number type", type_)
	}
}

func readLong(r reader) (int64, error) {
	type_, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch amqpType(type_) {
	case typeCodeNull:
		return 0, errNull
	case typeCodeSmalllong:
		n, err := r.ReadByte()
		return int64(n), err
	case typeCodeLong:
		if r.Len() < 8 {
			return 0, errorNew("invalid ulong")
		}
		n := binary.BigEndian.Uint64(r.Next(8))
		return int64(n), nil
	default:
		return 0, errorErrorf("invalid type for uint32 %02x", type_)
	}

}

func readInt32(r reader) (int32, error) {
	type_, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch amqpType(type_) {
	case typeCodeNull:
		return 0, errNull
	case typeCodeSmallint:
		n, err := r.ReadByte()
		return int32(n), err
	case typeCodeInt:
		if r.Len() < 4 {
			return 0, errorNew("invalid int")
		}
		n := binary.BigEndian.Uint32(r.Next(4))
		return int32(n), nil
	default:
		return 0, errorErrorf("invalid type for int32 %02x", type_)
	}
}

func readShort(r reader) (int16, error) {
	type_, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch amqpType(type_) {
	case typeCodeNull:
		return 0, errNull
	case typeCodeShort:
		if r.Len() < 2 {
			return 0, errorNew("invalid sshort")
		}
		n := binary.BigEndian.Uint16(r.Next(2))
		return int16(n), nil
	default:
		return 0, errorErrorf("invalid type for short %02x", type_)
	}
}

func readSbyte(r reader) (int8, error) {
	type_, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch amqpType(type_) {
	case typeCodeNull:
		return 0, errNull
	case typeCodeByte:
		n, err := r.ReadByte()
		return int8(n), err
	default:
		return 0, errorErrorf("invalid type for int8 %02x", type_)
	}
}

func readUbyte(r reader) (uint8, error) {
	type_, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch amqpType(type_) {
	case typeCodeNull:
		return 0, errNull
	case typeCodeUbyte:
		return r.ReadByte()
	default:
		return 0, errorErrorf("invalid type for ubyte %02x", type_)
	}
}

func readUshort(r reader) (uint16, error) {
	type_, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch amqpType(type_) {
	case typeCodeNull:
		return 0, errNull
	case typeCodeUshort:
		if r.Len() < 2 {
			return 0, errorNew("invalid ushort")
		}
		return binary.BigEndian.Uint16(r.Next(2)), nil
	default:
		return 0, errorErrorf("invalid type for ushort %02x", type_)
	}
}

func readUint32(r reader) (uint32, error) {
	type_, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch amqpType(type_) {
	case typeCodeNull:
		return 0, errNull
	case typeCodeUint0:
		return 0, nil
	case typeCodeSmallUint:
		n, err := r.ReadByte()
		return uint32(n), err
	case typeCodeUint:
		if r.Len() < 4 {
			return 0, errorNew("invalid uint")
		}
		return binary.BigEndian.Uint32(r.Next(4)), nil
	default:
		return 0, errorErrorf("invalid type for uint32 %02x", type_)
	}
}

func readUlong(r reader) (uint64, error) {
	type_, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch amqpType(type_) {
	case typeCodeNull:
		return 0, errNull
	case typeCodeUlong0:
		return 0, nil
	case typeCodeSmallUlong:
		n, err := r.ReadByte()
		return uint64(n), err
	case typeCodeUlong:
		if r.Len() < 8 {
			return 0, errorNew("invalid ulong")
		}
		return binary.BigEndian.Uint64(r.Next(8)), nil
	default:
		return 0, errorErrorf("invalid type for uint32 %02x", type_)
	}
}

func readFloat(r reader) (float32, error) {
	type_, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	if amqpType(type_) != typeCodeFloat {
		return 0, errorErrorf("invalid type for float32 %02x", type_)
	}

	if r.Len() < 4 {
		return 0, errorNew("invalid float")
	}

	bits := binary.BigEndian.Uint32(r.Next(4))
	return math.Float32frombits(bits), nil
}

func readDouble(r reader) (float64, error) {
	type_, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	if amqpType(type_) != typeCodeDouble {
		return 0, errorErrorf("invalid type for float64 %02x", type_)
	}

	if r.Len() < 8 {
		return 0, errorNew("invalid double")
	}

	bits := binary.BigEndian.Uint64(r.Next(8))
	return math.Float64frombits(bits), nil
}

func readBool(r reader) (bool, error) {
	type_, err := r.ReadByte()
	if err != nil {
		return false, err
	}

	switch amqpType(type_) {
	case typeCodeNull:
		return false, errNull
	case typeCodeBool:
		b, err := r.ReadByte()
		if err != nil {
			return false, err
		}
		return b != 0, nil
	case typeCodeBoolTrue:
		return true, nil
	case typeCodeBoolFalse:
		return false, nil
	default:
		return false, errorErrorf("type code %#02x is not a recognized bool type", type_)
	}
}

func readUint(r reader) (value uint64, _ error) {
	type_, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	switch amqpType(type_) {
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
		return 0, errorErrorf("type code %#02x is not a recognized number type", type_)
	}
}

func readUUID(r reader) (UUID, error) {
	var uuid UUID

	type_, err := r.ReadByte()
	if err != nil {
		return uuid, err
	}

	if amqpType(type_) != typeCodeUUID {
		return uuid, errorErrorf("type code %#00x is not a UUID", type_)
	}

	n, err := r.Read(uuid[:])
	if err != nil {
		return uuid, err
	}
	if n != 16 {
		return uuid, errorNew("invalid length")
	}

	return uuid, nil
}

func readMapHeader(r reader) (size uint32, count uint32, _ error) {
	type_, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	var width int
	switch amqpType(type_) {
	case typeCodeNull:
		return 0, 0, errNull
	case typeCodeMap8:
		b, err := r.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		size = uint32(b)
		b, err = r.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		count = uint32(b)
		width = 1
	case typeCodeMap32:
		if r.Len() < 8 { // two uint32's
			return 0, 0, errorNew("invalid length")
		}
		size = binary.BigEndian.Uint32(r.Next(4))
		count = binary.BigEndian.Uint32(r.Next(4))
		width = 4
	default:
		return 0, 0, errorErrorf("invalid map type %#02x", type_)
	}

	if uint64(size) > uint64(r.Len()+width) || uint64(count) > uint64(r.Len()) {
		return 0, 0, errorNew("invalid length")
	}
	return size, count, err
}
