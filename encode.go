package amqp

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"time"
	"unicode/utf8"
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
		return writeUint64(wr, uint64(t))
	case *uint:
		return writeUint64(wr, uint64(*t))
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
		return writeInt64(wr, int64(t))
	case *int:
		return writeInt64(wr, int64(*t))
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
	case float32:
		return writeFloat(wr, t)
	case *float32:
		return writeFloat(wr, *t)
	case float64:
		return writeDouble(wr, t)
	case *float64:
		return writeDouble(wr, *t)
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
	case []int8:
		err = arrayInt8(t).marshal(wr)
	case *[]int8:
		err = arrayInt8(*t).marshal(wr)
	case []uint16:
		err = arrayUint16(t).marshal(wr)
	case *[]uint16:
		err = arrayUint16(*t).marshal(wr)
	case []int16:
		err = arrayInt16(t).marshal(wr)
	case *[]int16:
		err = arrayInt16(*t).marshal(wr)
	case []uint32:
		err = arrayUint32(t).marshal(wr)
	case *[]uint32:
		err = arrayUint32(*t).marshal(wr)
	case []int32:
		err = arrayInt32(t).marshal(wr)
	case *[]int32:
		err = arrayInt32(*t).marshal(wr)
	case []uint64:
		err = arrayUint64(t).marshal(wr)
	case *[]uint64:
		err = arrayUint64(*t).marshal(wr)
	case []int64:
		err = arrayInt64(t).marshal(wr)
	case *[]int64:
		err = arrayInt64(*t).marshal(wr)
	case []float32:
		err = arrayFloat(t).marshal(wr)
	case *[]float32:
		err = arrayFloat(*t).marshal(wr)
	case []float64:
		err = arrayDouble(t).marshal(wr)
	case *[]float64:
		err = arrayDouble(*t).marshal(wr)
	case []bool:
		err = arrayBool(t).marshal(wr)
	case *[]bool:
		err = arrayBool(*t).marshal(wr)
	case []string:
		err = arrayString(t).marshal(wr)
	case *[]string:
		err = arrayString(*t).marshal(wr)
	case []symbol:
		err = arraySymbol(t).marshal(wr)
	case *[]symbol:
		err = arraySymbol(*t).marshal(wr)
	case [][]byte:
		err = arrayBinary(t).marshal(wr)
	case *[][]byte:
		err = arrayBinary(*t).marshal(wr)
	case []time.Time:
		err = arrayTimestamp(t).marshal(wr)
	case *[]time.Time:
		err = arrayTimestamp(*t).marshal(wr)
	case []UUID:
		err = arrayUUID(t).marshal(wr)
	case *[]UUID:
		err = arrayUUID(*t).marshal(wr)
	case []interface{}:
		err = list(t).marshal(wr)
	case *[]interface{}:
		err = list(*t).marshal(wr)
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

func writeFloat(wr writer, f float32) error {
	err := wr.WriteByte(byte(typeCodeFloat))
	if err != nil {
		return err
	}
	return binaryWriteUint32(wr, math.Float32bits(f))
}

func writeDouble(wr writer, f float64) error {
	err := wr.WriteByte(byte(typeCodeDouble))
	if err != nil {
		return err
	}
	return binaryWriteUint64(wr, math.Float64bits(f))
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

func writeArrayHeader(wr writer, length, typeSize int, type_ amqpType) error {
	size := length * typeSize
	// array type
	if size+array8TLSize <= math.MaxUint8 {
		//type
		err := wr.WriteByte(byte(typeCodeArray8))
		if err != nil {
			return err
		}
		// size
		wr.WriteByte(byte(size + array8TLSize))
		if err != nil {
			return err
		}
		// length
		wr.WriteByte(byte(length))
		if err != nil {
			return err
		}
	} else {
		//type
		err := wr.WriteByte(byte(typeCodeArray32))
		if err != nil {
			return err
		}
		// size
		err = binaryWriteUint32(wr, uint32(size+array32TLSize))
		if err != nil {
			return err
		}
		// length
		err = binaryWriteUint32(wr, uint32(length))
		if err != nil {
			return err
		}
	}

	// element type
	return wr.WriteByte(byte(type_))
}
