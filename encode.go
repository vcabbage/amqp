package amqp

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"sync"
	"unicode/utf8"
)

var (
	errInvalidLength = errorNew("length field is larger than frame")
	errNull          = errorNew("error is null")
)

// writer is the required interface for marshaling AMQP encoded data.
type writer interface {
	io.Writer
	io.ByteWriter
}

// bufPool is used to reduce allocations when encoding.
var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// writesFrame encodes fr into buf.
func writeFrame(buf *bytes.Buffer, fr frame) error {
	header := frameHeader{
		Size:       0, // overwrite later
		DataOffset: 2, // see frameHeader.DataOffset comment
		FrameType:  fr.typ,
		Channel:    fr.channel,
	}

	// write header
	err := binary.Write(buf, binary.BigEndian, header)
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
	case uint64:
		if t == 0 {
			err = wr.WriteByte(byte(typeCodeUlong0))
			break
		}
		err = wr.WriteByte(byte(typeCodeUlong))
		if err != nil {
			return err
		}
		err = binary.Write(wr, binary.BigEndian, t)
	case uint32:
		if t == 0 {
			err = wr.WriteByte(byte(typeCodeUint0))
			break
		}
		err = wr.WriteByte(byte(typeCodeUint))
		if err != nil {
			return err
		}
		err = binary.Write(wr, binary.BigEndian, t)
	case *uint32:
		if t == nil {
			err = wr.WriteByte(byte(typeCodeNull))
			break
		}
		if *t == 0 {
			err = wr.WriteByte(byte(typeCodeUint0))
			break
		}
		err = wr.WriteByte(byte(typeCodeUint))
		if err != nil {
			return err
		}
		err = binary.Write(wr, binary.BigEndian, *t)
	case uint16:
		err = wr.WriteByte(byte(typeCodeUshort))
		if err != nil {
			return err
		}
		err = binary.Write(wr, binary.BigEndian, t)
	case uint8:
		_, err = wr.Write([]byte{byte(typeCodeUbyte), t})
	case []Symbol:
		err = writeSymbolArray(wr, t)
	case string:
		err = writeString(wr, t)
	case []byte:
		err = writeBinary(wr, t)
	default:
		return errorErrorf("marshal not implemented for %T", i)
	}
	return err
}

// marshalField is a field to be marshaled
type marshalField struct {
	value interface{}
	omit  bool // indicates that this field should be omitted (set to null)
}

// marshalComposite is a helper for us in a composite's marshal() function.
//
// The returned bytes include the composite header and fields. Fields with
// omit set to true will be encoded as null or omitted altogether if there are
// no non-null fields after them.
func marshalComposite(wr writer, code amqpType, fields ...marshalField) error {
	if len(fields) == 0 {
		// write header only
		_, err := wr.Write([]byte{0x0, byte(typeCodeSmallUlong), byte(code), byte(typeCodeList0)})
		return err
	}

	var (
		rawFields = make([][]byte, len(fields)) // sized to the total number of fields

		// lastSetIdx is the last index to have a non-omitted field.
		// start at -1 as it's possible to have no fields in a composite
		lastSetIdx = -1
	)

	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()

	// marshal each field into it's index in rawFields,
	// null fields are skipped, leaving the index nil.
	var err error
	for i, f := range fields {
		if f.omit {
			continue
		}

		err = marshal(buf, f.value)
		if err != nil {
			return err
		}

		rawFields[i] = append([]byte(nil), buf.Bytes()...) // TODO: is there a cleaner way to do this?
		buf.Reset()

		lastSetIdx = i
	}

	// write null to each index up to lastSetIdx
	for i := 0; i < lastSetIdx+1; i++ {
		if rawFields[i] == nil {
			rawFields[i] = []byte{byte(typeCodeNull)}
		}
	}

	// write header
	_, err = wr.Write([]byte{0x0, byte(typeCodeSmallUlong), uint8(code)})
	if err != nil {
		return err
	}

	// write fields
	return writeList(wr, rawFields[:lastSetIdx+1]...)
}

func writeSymbolArray(w writer, symbols []Symbol) error {
	ofType := typeCodeSym8
	for _, symbol := range symbols {
		if len(symbol) > math.MaxUint8 {
			ofType = typeCodeSym32
			break
		}
	}

	buf := bufPool.Get().(*bytes.Buffer)
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

func writeSymbol(wr writer, sym Symbol, typ amqpType) error {
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

func writeString(wr writer, str string) error {
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

func writeBinary(wr writer, bin []byte) error {
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

func writeArray(wr writer, of amqpType, fields ...[]byte) error {
	const isArray = true
	return writeSlice(wr, isArray, of, fields...)
}

func writeList(wr writer, fields ...[]byte) error {
	const isArray = false
	return writeSlice(wr, isArray, 0, fields...)
}

func writeSlice(wr writer, isArray bool, of amqpType, fields ...[]byte) error {
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

func writeMapHeader(wr writer, elements int) error {
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

func writeMapElement(wr writer, key, value interface{}) error {
	err := marshal(wr, key)
	if err != nil {
		return err
	}
	return marshal(wr, value)
}
