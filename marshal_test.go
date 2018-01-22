package amqp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

var exampleFrames = []struct {
	label string
	frame frame
}{
	{
		label: "transfer",
		frame: frame{
			typ:     frameTypeAMQP,
			channel: 10,
			body: &performTransfer{
				Handle:             34983,
				DeliveryID:         uint32Ptr(564),
				DeliveryTag:        []byte("foo tag"),
				MessageFormat:      uint32Ptr(34),
				Settled:            true,
				More:               true,
				ReceiverSettleMode: rcvSettle(ModeSecond),
				State:              &stateReceived{},
				Resume:             true,
				Aborted:            true,
				Batchable:          true,
				Payload:            []byte("very important payload"),
			},
		},
	},
}

func TestFrameMarshalUnmarshal(t *testing.T) {
	for _, tt := range exampleFrames {
		t.Run(tt.label, func(t *testing.T) {
			var buf bytes.Buffer

			err := writeFrame(&buf, tt.frame)
			if err != nil {
				t.Error(fmt.Sprintf("%+v", err))
			}

			header, err := parseFrameHeader(&buf)
			if err != nil {
				t.Errorf("%+v", err)
			}

			want := tt.frame
			if header.Channel != want.channel {
				t.Errorf("Expected channel to be %d, but it is %d", want.channel, header.Channel)
			}
			if header.FrameType != want.typ {
				t.Errorf("Expected channel to be %d, but it is %d", want.typ, header.FrameType)
			}

			payload, err := parseFrameBody(&buf)
			cmpOpts := cmp.Options{
				DeepAllowUnexported(want.body, payload),
			}
			if !cmp.Equal(want.body, payload, cmpOpts...) {
				t.Errorf("Roundtrip produced different results:\n %s", cmp.Diff(want.body, payload, cmpOpts...))
			}
		})
	}
}

func BenchmarkFrameMarshal(b *testing.B) {
	for _, tt := range exampleFrames {
		b.Run(tt.label, func(b *testing.B) {
			b.ReportAllocs()
			var buf bytes.Buffer

			for i := 0; i < b.N; i++ {
				err := writeFrame(&buf, tt.frame)
				if err != nil {
					b.Error(fmt.Sprintf("%+v", err))
				}
				bytesSink = buf.Bytes()
				buf.Reset()
			}
		})
	}
}
func BenchmarkFrameUnmarshal(b *testing.B) {
	for _, tt := range exampleFrames {
		b.Run(tt.label, func(b *testing.B) {
			b.ReportAllocs()
			var buf bytes.Buffer
			err := writeFrame(&buf, tt.frame)
			if err != nil {
				b.Error(fmt.Sprintf("%+v", err))
			}
			data := buf.Bytes()
			buf.Reset()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				buf := bytes.NewBuffer(data)
				_, err := parseFrameHeader(buf)
				if err != nil {
					b.Errorf("%+v", err)
				}

				_, err = parseFrameBody(buf)
				if err != nil {
					b.Errorf("%+v", err)
				}
			}
		})
	}
}

var bytesSink []byte

func BenchmarkMarshal(b *testing.B) {
	for _, typ := range exampleTypes {
		b.Run(fmt.Sprintf("%T", typ), func(b *testing.B) {
			b.ReportAllocs()
			var buf bytes.Buffer

			for i := 0; i < b.N; i++ {
				err := marshal(&buf, typ)
				if err != nil {
					b.Error(fmt.Sprintf("%+v", err))
				}
				bytesSink = buf.Bytes()
				buf.Reset()
			}
		})
	}
}

var typeSink interface{}

func BenchmarkUnmarshal(b *testing.B) {
	for _, typ := range exampleTypes {
		b.Run(fmt.Sprintf("%T", typ), func(b *testing.B) {
			var buf bytes.Buffer
			err := marshal(&buf, typ)
			if err != nil {
				b.Error(fmt.Sprintf("%+v", err))
			}
			data := buf.Bytes()
			newTyp := reflect.New(reflect.TypeOf(typ)).Interface()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				typeSink, err = unmarshal(bytes.NewBuffer(data), newTyp)
				if err != nil {
					b.Error(fmt.Sprintf("%v", err))
				}
			}
		})
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	_, updateFuzzCorpus := os.LookupEnv("UPDATE_FUZZ_CORPUS")

	for _, typ := range exampleTypes {
		t.Run(fmt.Sprintf("%T", typ), func(t *testing.T) {
			var buf bytes.Buffer
			err := marshal(&buf, typ)
			if err != nil {
				t.Error(fmt.Sprintf("%+v", err))
			}

			if updateFuzzCorpus {
				name := fmt.Sprintf("%T.bin", typ)
				name = strings.TrimPrefix(name, "amqp.")
				name = strings.TrimPrefix(name, "*amqp.")
				path := filepath.Join("fuzz/marshal/corpus", name)
				err = ioutil.WriteFile(path, buf.Bytes(), 0644)
				if err != nil {
					t.Error(err)
				}
			}

			newTyp := reflect.New(reflect.TypeOf(typ))
			_, err = unmarshal(&buf, newTyp.Interface())
			if err != nil {
				t.Error(fmt.Sprintf("%+v", err))
				return
			}

			cmpTyp := reflect.Indirect(newTyp).Interface()
			cmpOpts := cmp.Options{
				DeepAllowUnexported(typ, cmpTyp),
			}
			if !cmp.Equal(typ, cmpTyp, cmpOpts...) {
				t.Errorf("Roundtrip produced different results:\n %s", cmp.Diff(typ, cmpTyp, cmpOpts...))
			}
		})
	}
}

var exampleTypes = []interface{}{
	&performOpen{
		ContainerID:         "foo",
		Hostname:            "bar.host",
		MaxFrameSize:        4200,
		ChannelMax:          13,
		OutgoingLocales:     []symbol{"fooLocale"},
		IncomingLocales:     []symbol{"barLocale"},
		OfferedCapabilities: []symbol{"fooCap"},
		DesiredCapabilities: []symbol{"barCap"},
		Properties: map[symbol]interface{}{
			"fooProp": 45,
		},
	},
	&performBegin{
		RemoteChannel:       4321,
		NextOutgoingID:      730000,
		IncomingWindow:      9876654,
		OutgoingWindow:      123555,
		HandleMax:           9757,
		OfferedCapabilities: []symbol{"fooCap"},
		DesiredCapabilities: []symbol{"barCap"},
		Properties: map[symbol]interface{}{
			"fooProp": 45,
		},
	},
	&performAttach{
		Name:               "fooName",
		Handle:             435982,
		Role:               roleSender,
		SenderSettleMode:   sndSettle(ModeMixed),
		ReceiverSettleMode: rcvSettle(ModeSecond),
		Source: &source{
			Address:      "fooAddr",
			Durable:      2,
			ExpiryPolicy: "link-detach",
			Timeout:      635,
			Dynamic:      true,
			DynamicNodeProperties: map[symbol]interface{}{
				"lifetime-policy": deleteOnClose,
			},
			DistributionMode: "some-mode",
			Filter: map[symbol]interface{}{
				"foo:filter": "bar value",
			},
			Outcomes:     []symbol{"amqp:accepted:list"},
			Capabilities: []symbol{"barCap"},
		},
		Target: &target{
			Address:      "fooAddr",
			Durable:      2,
			ExpiryPolicy: "link-detach",
			Timeout:      635,
			Dynamic:      true,
			DynamicNodeProperties: map[symbol]interface{}{
				"lifetime-policy": deleteOnClose,
			},
			Capabilities: []symbol{"barCap"},
		},
		Unsettled: unsettled{
			"fooDeliveryTag": &stateAccepted{},
		},
		IncompleteUnsettled:  true,
		InitialDeliveryCount: 3184,
		MaxMessageSize:       75983,
		OfferedCapabilities:  []symbol{"fooCap"},
		DesiredCapabilities:  []symbol{"barCap"},
		Properties: map[symbol]interface{}{
			"fooProp": 45,
		},
	},
	role(true),
	&unsettled{
		"fooDeliveryTag": &stateAccepted{},
	},
	&source{
		Address:      "fooAddr",
		Durable:      2,
		ExpiryPolicy: "link-detach",
		Timeout:      635,
		Dynamic:      true,
		DynamicNodeProperties: map[symbol]interface{}{
			"lifetime-policy": deleteOnClose,
		},
		DistributionMode: "some-mode",
		Filter: map[symbol]interface{}{
			"foo:filter": "bar value",
		},
		Outcomes:     []symbol{"amqp:accepted:list"},
		Capabilities: []symbol{"barCap"},
	},
	&target{
		Address:      "fooAddr",
		Durable:      2,
		ExpiryPolicy: "link-detach",
		Timeout:      635,
		Dynamic:      true,
		DynamicNodeProperties: map[symbol]interface{}{
			"lifetime-policy": deleteOnClose,
		},
		Capabilities: []symbol{"barCap"},
	},
	&performFlow{
		NextIncomingID: uint32Ptr(354),
		IncomingWindow: 4352,
		NextOutgoingID: 85324,
		OutgoingWindow: 24378634,
		Handle:         uint32Ptr(341543),
		DeliveryCount:  uint32Ptr(31341),
		LinkCredit:     uint32Ptr(7634),
		Available:      uint32Ptr(878321),
		Drain:          true,
		Echo:           true,
		Properties: map[symbol]interface{}{
			"fooProp": 45,
		},
	},
	&performTransfer{
		Handle:             34983,
		DeliveryID:         uint32Ptr(564),
		DeliveryTag:        []byte("foo tag"),
		MessageFormat:      uint32Ptr(34),
		Settled:            true,
		More:               true,
		ReceiverSettleMode: rcvSettle(ModeSecond),
		State:              &stateReceived{},
		Resume:             true,
		Aborted:            true,
		Batchable:          true,
		Payload:            []byte("very important payload"),
	},
	&performDisposition{
		Role:      roleSender,
		First:     5644444,
		Last:      uint32Ptr(423),
		Settled:   true,
		State:     &stateReleased{},
		Batchable: true,
	},
	&performDetach{
		Handle: 4352,
		Closed: true,
		Error: &Error{
			Condition:   ErrorNotAllowed,
			Description: "foo description",
			Info: map[string]interface{}{
				"other": "info",
				"and":   875,
			},
		},
	},
	&performDetach{
		Handle: 4352,
		Closed: true,
		Error: &Error{
			Condition:   ErrorLinkRedirect,
			Description: "",
			// payload is bigger than map8 encoding size
			Info: map[string]interface{}{
				"hostname":     "redirected.myservicebus.example.org",
				"network-host": "redirected.myservicebus.example.org",
				"port":         5671,
				"address":      "amqps://redirected.myservicebus.example.org:5671/path",
			},
		},
	},
	ErrorCondition("the condition"),
	&Error{
		Condition:   ErrorNotAllowed,
		Description: "foo description",
		Info: map[string]interface{}{
			"other": "info",
			"and":   875,
		},
	},
	&performEnd{
		Error: &Error{
			Condition:   ErrorNotAllowed,
			Description: "foo description",
			Info: map[string]interface{}{
				"other": "info",
				"and":   875,
			},
		},
	},
	&performClose{
		Error: &Error{
			Condition:   ErrorNotAllowed,
			Description: "foo description",
			Info: map[string]interface{}{
				"other": "info",
				"and":   875,
			},
		},
	},
	&Message{
		Header: &MessageHeader{
			Durable:       true,
			Priority:      234,
			TTL:           10 * time.Second,
			FirstAcquirer: true,
			DeliveryCount: 32,
		},
		DeliveryAnnotations: map[interface{}]interface{}{
			42: "answer",
		},
		Annotations: map[interface{}]interface{}{
			42: "answer",
		},
		Properties: &MessageProperties{
			MessageID:          "yo",
			UserID:             []byte("baz"),
			To:                 "me",
			Subject:            "sup?",
			ReplyTo:            "you",
			CorrelationID:      34513,
			ContentType:        "text/plain",
			ContentEncoding:    "UTF-8",
			AbsoluteExpiryTime: time.Date(2018, 01, 13, 14, 24, 07, 0, time.UTC),
			CreationTime:       time.Date(2018, 01, 13, 14, 14, 07, 0, time.UTC),
			GroupID:            "fooGroup",
			GroupSequence:      89324,
			ReplyToGroupID:     "barGroup",
		},
		ApplicationProperties: map[string]interface{}{
			"baz": "foo",
		},
		Data:  []byte("A nice little data payload."),
		Value: 42,
		Footer: map[interface{}]interface{}{
			"hash": []uint8{0, 1, 2, 34, 5, 6, 7, 8, 9, 0},
		},
	},
	&MessageHeader{
		Durable:       true,
		Priority:      234,
		TTL:           10 * time.Second,
		FirstAcquirer: true,
		DeliveryCount: 32,
	},
	&MessageProperties{
		MessageID:          "yo",
		UserID:             []byte("baz"),
		To:                 "me",
		Subject:            "sup?",
		ReplyTo:            "you",
		CorrelationID:      34513,
		ContentType:        "text/plain",
		ContentEncoding:    "UTF-8",
		AbsoluteExpiryTime: time.Date(2018, 01, 13, 14, 24, 07, 0, time.UTC),
		CreationTime:       time.Date(2018, 01, 13, 14, 14, 07, 0, time.UTC),
		GroupID:            "fooGroup",
		GroupSequence:      89324,
		ReplyToGroupID:     "barGroup",
	},
	&stateReceived{
		SectionNumber: 234,
		SectionOffset: 8973,
	},
	&stateAccepted{},
	&stateRejected{
		Error: &Error{
			Condition:   ErrorStolen,
			Description: "foo description",
			Info: map[string]interface{}{
				"other": "info",
				"and":   875,
			},
		},
	},
	&stateReleased{},
	&stateModified{
		DeliveryFailed:    true,
		UndeliverableHere: true,
		MessageAnnotations: map[symbol]interface{}{
			"more": "annotations",
		},
	},
	UUID{1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16},
	lifetimePolicy(typeCodeDeleteOnClose),
	SenderSettleMode(1),
	ReceiverSettleMode(1),
	bool(true),
	int8(math.MaxInt8),
	int8(math.MinInt8),
	int16(math.MaxInt16),
	int16(math.MinInt16),
	int32(math.MaxInt32),
	int32(math.MinInt32),
	int64(math.MaxInt64),
	int64(math.MinInt64),
	uint8(math.MaxUint8),
	uint16(math.MaxUint16),
	uint32(math.MaxUint32),
	uint64(math.MaxUint64),
	describedType{
		descriptor: binary.BigEndian.Uint64([]byte{0x00, 0x00, 0x46, 0x8C, 0x00, 0x00, 0x00, 0x04}),
		value:      "amqp.annotation.x-opt-offset > '312'",
	},
	&saslInit{
		Mechanism:       "FOO",
		InitialResponse: []byte("BAR\x00RESPONSE\x00"),
		Hostname:        "me",
	},
	&saslMechanisms{
		Mechanisms: []symbol{"FOO", "BAR", "BAZ"},
	},
	&saslOutcome{
		Code:           codeSASLSysPerm,
		AdditionalData: []byte("here's some info for you..."),
	},
	symbol("a symbol"),
	milliseconds(10 * time.Second),
	&mapAnyAny{
		-1234: []uint8{0, 1, 2, 34, 5, 6, 7, 8, 9, 0},
	},
	&mapStringAny{
		"hash": []uint8{0, 1, 2, 34, 5, 6, 7, 8, 9, 0},
	},
	&mapSymbolAny{
		"hash": []uint8{0, 1, 2, 34, 5, 6, 7, 8, 9, 0},
	},
}

func sndSettle(m SenderSettleMode) *SenderSettleMode {
	return &m
}
func rcvSettle(m ReceiverSettleMode) *ReceiverSettleMode {
	return &m
}

func uint32Ptr(u uint32) *uint32 {
	return &u
}

// from https://github.com/google/go-cmp/issues/40
func DeepAllowUnexported(vs ...interface{}) cmp.Option {
	m := make(map[reflect.Type]struct{})
	for _, v := range vs {
		structTypes(reflect.ValueOf(v), m)
	}
	var typs []interface{}
	for t := range m {
		typs = append(typs, reflect.New(t).Elem().Interface())
	}
	return cmp.AllowUnexported(typs...)
}

func structTypes(v reflect.Value, m map[reflect.Type]struct{}) {
	if !v.IsValid() {
		return
	}
	switch v.Kind() {
	case reflect.Ptr:
		if !v.IsNil() {
			structTypes(v.Elem(), m)
		}
	case reflect.Interface:
		if !v.IsNil() {
			structTypes(v.Elem(), m)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			structTypes(v.Index(i), m)
		}
	case reflect.Map:
		for _, k := range v.MapKeys() {
			structTypes(v.MapIndex(k), m)
		}
	case reflect.Struct:
		m[v.Type()] = struct{}{}
		for i := 0; i < v.NumField(); i++ {
			structTypes(v.Field(i), m)
		}
	}
}
