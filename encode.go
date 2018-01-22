package amqp

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"time"
	"unicode/utf8"
)

const intSize = 32 << (^uint(0) >> 63)

var (
	errInvalidLength = errorNew("length field is larger than frame")
	errNull          = errorNew("error is null")
)

// writer is the required interface for marshaling AMQP encoded data.
type writer interface {
	io.Writer
	io.ByteWriter
	WriteString(s string) (n int, err error)
	Len() int
	Bytes() []byte
}

// writesFrame encodes fr into buf.
func writeFrame(buf *bytes.Buffer, fr frame) error {
	// write header
	_, err := buf.Write([]byte{0, 0, 0, 0}) // size, overwrite later
	if err != nil {
		return err
	}
	err = buf.WriteByte(2) // doff, see frameHeader.DataOffset comment
	if err != nil {
		return err
	}
	err = buf.WriteByte(fr.typ) // frame type
	if err != nil {
		return err
	}
	err = binaryWriteUint16(buf, fr.channel) // channel
	if err != nil {
		return err
	}

	// write AMQP frame body
	err = marshal(buf, fr.body)
	if err != nil {
		return err
	}

	// validate size
	if buf.Len() > math.MaxUint32 {
		return errorNew("frame too large")
	}

	// retrieve raw bytes
	bufBytes := buf.Bytes()

	// write correct size
	binary.BigEndian.PutUint32(bufBytes, uint32(len(bufBytes)))
	return nil
}

type marshaler interface {
	marshal(writer) error
}

func marshal(wr writer, i interface{}) error {
	var err error
	switch t := i.(type) {
	case marshaler:
		return t.marshal(wr)
	case bool:
		if t {
			err = wr.WriteByte(byte(typeCodeBoolTrue))
		} else {
			err = wr.WriteByte(byte(typeCodeBoolFalse))
		}
	case *bool:
		if *t {
			err = wr.WriteByte(byte(typeCodeBoolTrue))
		} else {
			err = wr.WriteByte(byte(typeCodeBoolFalse))
		}
	case uint:
		if intSize == 64 {
			return writeUint64(wr, uint64(t))
		}
		return writeUint32(wr, uint32(t))
	case *uint:
		if intSize == 64 {
			return writeUint64(wr, uint64(*t))
		}
		return writeUint32(wr, uint32(*t))
	case uint64:
		return writeUint64(wr, t)
	case *uint64:
		return writeUint64(wr, *t)
	case uint32:
		return writeUint32(wr, t)
	case *uint32:
		if t == nil {
			err = wr.WriteByte(byte(typeCodeNull))
			break
		}
		return writeUint32(wr, *t)
	case uint16:
		err = wr.WriteByte(byte(typeCodeUshort))
		if err != nil {
			return err
		}
		err = binaryWriteUint16(wr, t)
	case *uint16:
		err = wr.WriteByte(byte(typeCodeUshort))
		if err != nil {
			return err
		}
		err = binaryWriteUint16(wr, *t)
	case uint8:
		err = wr.WriteByte(byte(typeCodeUbyte))
		if err != nil {
			return err
		}
		err = wr.WriteByte(t)
	case *uint8:
		err = wr.WriteByte(byte(typeCodeUbyte))
		if err != nil {
			return err
		}
		err = wr.WriteByte(*t)
	case int:
		if intSize == 64 {
			return writeInt64(wr, int64(t))
		}
		return writeInt32(wr, int32(t))
	case *int:
		if intSize == 64 {
			return writeInt64(wr, int64(*t))
		}
		return writeInt32(wr, int32(*t))
	case int8:
		err = wr.WriteByte(byte(typeCodeByte))
		if err != nil {
			return err
		}
		err = wr.WriteByte(uint8(t))
	case *int8:
		err = wr.WriteByte(byte(typeCodeByte))
		if err != nil {
			return err
		}
		err = wr.WriteByte(uint8(*t))
	case int16:
		err = wr.WriteByte(byte(typeCodeShort))
		if err != nil {
			return err
		}
		err = binaryWriteUint16(wr, uint16(t))
	case *int16:
		err = wr.WriteByte(byte(typeCodeShort))
		if err != nil {
			return err
		}
		err = binaryWriteUint16(wr, uint16(*t))
	case int32:
		return writeInt32(wr, t)
	case *int32:
		return writeInt32(wr, *t)
	case int64:
		return writeInt64(wr, t)
	case *int64:
		return writeInt64(wr, *t)
	case []symbol:
		err = writeSymbolArray(wr, t)
	case *[]symbol:
		err = writeSymbolArray(wr, *t)
	case string:
		err = writeString(wr, t)
	case *string:
		err = writeString(wr, *t)
	case []byte:
		err = writeBinary(wr, t)
	case *[]byte:
		err = writeBinary(wr, *t)
	case map[interface{}]interface{}:
		err = writeMap(wr, t)
	case *map[interface{}]interface{}:
		err = writeMap(wr, *t)
	case map[string]interface{}:
		err = writeMap(wr, t)
	case *map[string]interface{}:
		err = writeMap(wr, *t)
	case map[symbol]interface{}:
		err = writeMap(wr, t)
	case *map[symbol]interface{}:
		err = writeMap(wr, *t)
	case unsettled:
		err = writeMap(wr, t)
	case *unsettled:
		err = writeMap(wr, *t)
	case time.Time:
		err = writeTimestamp(wr, t)
	case *time.Time:
		err = writeTimestamp(wr, *t)
	default:
		return errorErrorf("marshal not implemented for %T", i)
	}
	return err
}

func writeInt32(wr writer, n int32) error {
	if n < 128 && n >= -128 {
		err := wr.WriteByte(byte(typeCodeSmallint))
		if err != nil {
			return err
		}
		return wr.WriteByte(byte(n))
	}

	err := wr.WriteByte(byte(typeCodeInt))
	if err != nil {
		return err
	}

	return binaryWriteUint32(wr, uint32(n))
}

func binaryWriteUint16(wr writer, n uint16) error {
	err := wr.WriteByte(byte(n >> 8))
	if err != nil {
		return err
	}
	return wr.WriteByte(byte(n))
}

func binaryWriteUint32(wr writer, n uint32) error {
	err := wr.WriteByte(byte(n >> 24))
	if err != nil {
		return err
	}
	err = wr.WriteByte(byte(n >> 16))
	if err != nil {
		return err
	}
	err = wr.WriteByte(byte(n >> 8))
	if err != nil {
		return err
	}
	return wr.WriteByte(byte(n))
}

func binaryWriteUint64(wr writer, n uint64) error {
	err := wr.WriteByte(byte(n >> 56))
	if err != nil {
		return err
	}
	err = wr.WriteByte(byte(n >> 48))
	if err != nil {
		return err
	}
	err = wr.WriteByte(byte(n >> 40))
	if err != nil {
		return err
	}
	err = wr.WriteByte(byte(n >> 32))
	if err != nil {
		return err
	}
	err = wr.WriteByte(byte(n >> 24))
	if err != nil {
		return err
	}
	err = wr.WriteByte(byte(n >> 16))
	if err != nil {
		return err
	}
	err = wr.WriteByte(byte(n >> 8))
	if err != nil {
		return err
	}
	return wr.WriteByte(byte(n))
}

func writeInt64(wr writer, n int64) error {
	if n < 128 && n >= -128 {
		err := wr.WriteByte(byte(typeCodeSmalllong))
		if err != nil {
			return err
		}
		return wr.WriteByte(byte(n))
	}

	err := wr.WriteByte(byte(typeCodeLong))
	if err != nil {
		return err
	}
	err = binaryWriteUint64(wr, uint64(n))
	return err
}

func writeUint32(wr writer, n uint32) error {
	if n == 0 {
		return wr.WriteByte(byte(typeCodeUint0))
	}

	if n < 256 {
		err := wr.WriteByte(byte(typeCodeSmallUint))
		if err != nil {
			return err
		}
		return wr.WriteByte(byte(n))
	}

	err := wr.WriteByte(byte(typeCodeUint))
	if err != nil {
		return err
	}
	return binaryWriteUint32(wr, n)
}

func writeUint64(wr writer, n uint64) error {
	if n == 0 {
		return wr.WriteByte(byte(typeCodeUlong0))
	}

	if n < 256 {
		err := wr.WriteByte(byte(typeCodeSmallUlong))
		if err != nil {
			return err
		}
		err = wr.WriteByte(byte(n))
		return err
	}

	err := wr.WriteByte(byte(typeCodeUlong))
	if err != nil {
		return err
	}

	return binaryWriteUint64(wr, n)
}

func writeTimestamp(wr writer, t time.Time) error {
	err := wr.WriteByte(byte(typeCodeTimestamp))
	if err != nil {
		return err
	}

	ms := t.UnixNano() / int64(time.Millisecond)
	return binaryWriteUint64(wr, uint64(ms))
}

// marshalField is a field to be marshaled
type marshalField struct {
	value interface{} // value to be marshaled, use pointers to avoid interface conversion overhead
	omit  bool        // indicates that this field should be omitted (set to null)
}
type marshalField2 struct {
	marshal func(wr writer) error
	omit    bool // indicates that this field should be omitted (set to null)
}

// marshalComposite is a helper for us in a composite's marshal() function.
//
// The returned bytes include the composite header and fields. Fields with
// omit set to true will be encoded as null or omitted altogether if there are
// no non-null fields after them.
func marshalComposite(wr writer, code amqpType, fields ...marshalField) error {
	// lastSetIdx is the last index to have a non-omitted field.
	// start at -1 as it's possible to have no fields in a composite
	lastSetIdx := -1

	// marshal each field into it's index in rawFields,
	// null fields are skipped, leaving the index nil.
	for i, f := range fields {
		if f.omit {
			continue
		}
		lastSetIdx = i
	}

	// write header only
	if lastSetIdx == -1 {
		err := wr.WriteByte(0x0)
		if err != nil {
			return err
		}
		err = wr.WriteByte(byte(typeCodeSmallUlong))
		if err != nil {
			return err
		}
		err = wr.WriteByte(byte(code))
		if err != nil {
			return err
		}
		err = wr.WriteByte(byte(typeCodeList0))
		return err
	}

	// write header
	err := writeDescriptor(wr, code)
	if err != nil {
		return err
	}

	// write fields
	err = wr.WriteByte(byte(typeCodeList32))
	if err != nil {
		return err
	}

	// write temp size, replace later
	sizeIdx := wr.Len()
	err = binaryWriteUint32(wr, 0)
	if err != nil {
		return err
	}
	preFieldLen := wr.Len()

	// field count
	err = binaryWriteUint32(wr, uint32(lastSetIdx+1))
	if err != nil {
		return err
	}

	// write null to each index up to lastSetIdx
	for _, f := range fields[:lastSetIdx+1] {
		if f.omit {
			wr.WriteByte(byte(typeCodeNull))
			continue
		}
		err := marshal(wr, f.value)
		if err != nil {
			return err
		}
	}

	// fix size
	size := uint32(wr.Len() - preFieldLen)
	buf := wr.Bytes()
	binary.BigEndian.PutUint32(buf[sizeIdx:], size)

	return err
}

func writeDescriptor(wr writer, code amqpType) error {
	err := wr.WriteByte(0x0)
	if err != nil {
		return err
	}
	err = wr.WriteByte(byte(typeCodeSmallUlong))
	if err != nil {
		return err
	}
	return wr.WriteByte(byte(code))
}

func writeSymbolArray(wr writer, symbols []symbol) error {
	ofType := typeCodeSym8
	for _, symbol := range symbols {
		if len(symbol) > math.MaxUint8 {
			ofType = typeCodeSym32
			break
		}
	}

	// always using array32 might waste a couple bytes,
	// but it simplifies overwriting the size later
	err := wr.WriteByte(byte(typeCodeArray32))
	if err != nil {
		return err
	}

	// temp size, overwrite later
	sizeIdx := wr.Len()
	err = binaryWriteUint32(wr, 0)
	if err != nil {
		return err
	}

	// length
	preArrayLen := wr.Len()
	err = binaryWriteUint32(wr, uint32(len(symbols)))
	if err != nil {
		return err
	}

	// array type
	err = wr.WriteByte(byte(ofType))
	if err != nil {
		return err
	}

	// write symbols
	for _, symbol := range symbols {
		err := writeSymbolType(wr, symbol, ofType)
		if err != nil {
			return err
		}
	}

	// overwrite the size
	binary.BigEndian.PutUint32(wr.Bytes()[sizeIdx:], uint32(wr.Len()-preArrayLen))
	return err
}

func writeSymbol(wr writer, sym symbol) error {
	ofType := typeCodeSym8
	if len(sym) > math.MaxUint8 {
		ofType = typeCodeSym32
	}

	return writeSymbolType(wr, sym, ofType)
}

func writeSymbolType(wr writer, sym symbol, typ amqpType) error {
	if !utf8.ValidString(string(sym)) {
		return errorNew("not a valid UTF-8 string")
	}

	l := len(sym)

	switch typ {
	case typeCodeSym8:
		err := wr.WriteByte(uint8(l))
		if err != nil {
			return err
		}
	case typeCodeSym32:
		err := binaryWriteUint32(wr, uint32(l))
		if err != nil {
			return err
		}
	default:
		return errorNew("invalid symbol type")
	}
	_, err := wr.WriteString(string(sym))
	return err
}

func writeString(wr writer, str string) error {
	if !utf8.ValidString(str) {
		return errorNew("not a valid UTF-8 string")
	}
	l := len(str)

	switch {
	// Str8
	case l < 256:
		err := wr.WriteByte(byte(typeCodeStr8))
		if err != nil {
			return err
		}
		err = wr.WriteByte(byte(l))
		if err != nil {
			return err
		}
		_, err = wr.WriteString(str)
		return err

	// Str32
	case l < math.MaxUint32:
		err := wr.WriteByte(byte(typeCodeStr32))
		if err != nil {
			return err
		}

		err = binaryWriteUint32(wr, uint32(l))
		if err != nil {
			return err
		}

		_, err = wr.WriteString(str)
		return err

	default:
		return errorNew("too long")
	}
}

func writeBinary(wr writer, bin []byte) error {
	l := len(bin)

	switch {
	// List8
	case l < 256:
		err := wr.WriteByte(byte(typeCodeVbin8))
		if err != nil {
			return err
		}
		err = wr.WriteByte(uint8(l))
		if err != nil {
			return err
		}
		_, err = wr.Write(bin)
		return err

	// List32
	case l < math.MaxUint32:
		err := wr.WriteByte(byte(typeCodeVbin32))
		if err != nil {
			return err
		}

		err = binaryWriteUint32(wr, uint32(l))
		if err != nil {
			return err
		}

		_, err = wr.Write(bin)
		return err

	default:
		return errorNew("too long")
	}
}

func writeArray(wr writer, of amqpType, numFields int, size int) error {
	const isArray = true
	return writeSlice(wr, isArray, of, numFields, size)
}

func writeList(wr writer, numFields int, size int) error {
	const isArray = false
	return writeSlice(wr, isArray, 0, numFields, size)
}

func writeSlice(wr writer, isArray bool, of amqpType, numFields int, size int) error {
	size8 := typeCodeList8
	size32 := typeCodeList32
	if isArray {
		size8 = typeCodeArray8
		size32 = typeCodeArray32
	}

	switch {
	// list0
	case numFields == 0 && isArray:
		return wr.WriteByte(byte(typeCodeList0))

	// list8
	case numFields < 256 && size < 256:
		err := wr.WriteByte(byte(size8))
		if err != nil {
			return err
		}
		err = wr.WriteByte(byte(size + 1))
		if err != nil {
			return err
		}
		err = wr.WriteByte(byte(numFields))
		if err != nil {
			return err
		}

	// list32
	case numFields < math.MaxUint32 && size < math.MaxUint32:
		err := wr.WriteByte(byte(size32))
		if err != nil {
			return err
		}

		err = binaryWriteUint32(wr, uint32(size+4))
		if err != nil {
			return err
		}

		err = binaryWriteUint32(wr, uint32(numFields))
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

	return nil
}

func writeMap(wr writer, m interface{}) error {
	var length int
	buf := new(bytes.Buffer)

	switch m := m.(type) {
	case map[interface{}]interface{}:
		length = len(m)
		for key, val := range m {
			err := marshal(buf, key)
			if err != nil {
				return err
			}
			err = marshal(buf, val)
			if err != nil {
				return err
			}
		}
	case map[string]interface{}:
		length = len(m)
		for key, val := range m {
			err := amqpString(key).marshal(buf)
			if err != nil {
				return err
			}
			err = marshal(buf, val)
			if err != nil {
				return err
			}
		}
	case map[symbol]interface{}:
		length = len(m)
		for key, val := range m {
			err := key.marshal(buf)
			if err != nil {
				return err
			}
			err = marshal(buf, val)
			if err != nil {
				return err
			}
		}
	case unsettled:
		length = len(m)
		for key, val := range m {
			err := amqpString(key).marshal(buf)
			if err != nil {
				return err
			}
			err = marshal(buf, val)
			if err != nil {
				return err
			}
		}
	default:
		return errorErrorf("unsupported type or map type %T", m)
	}

	pairs := length * 2
	if pairs > math.MaxUint32-4 {
		return errorNew("map contains too many elements")
	}

	l := buf.Len()
	switch {
	case l+1 <= math.MaxUint8:
		_, err := wr.Write([]byte{byte(typeCodeMap8), byte(l + 1), byte(pairs)})
		if err != nil {
			return err
		}
	case l+4 <= math.MaxUint32:
		err := wr.WriteByte(byte(typeCodeMap32))
		if err != nil {
			return err
		}
		err = binaryWriteUint32(wr, uint32(l+4))
		if err != nil {
			return err
		}
		err = binaryWriteUint32(wr, uint32(pairs))
		if err != nil {
			return err
		}
	default:
		return errorNew("map too large")
	}

	_, err := buf.WriteTo(wr)
	return err
}
