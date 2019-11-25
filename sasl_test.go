package amqp

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"pack.ag/amqp/internal/testconn"
	"strings"
	"testing"
	"time"
)

// Known good challenges/responses taken following specification:
// https://developers.google.com/gmail/imap/xoauth2-protocol#the_sasl_xoauth2_mechanism

func TestSaslXOAUTH2InitialResponse(t *testing.T) {
	wantedRespBase64 := "dXNlcj1zb21ldXNlckBleGFtcGxlLmNvbQFhdXRoPUJlYXJlciB5YTI5LnZGOWRmdDRxbVRjMk52YjNSbGNrQmhkSFJoZG1semRHRXVZMjl0Q2cBAQ=="
	wantedResp, err := base64.StdEncoding.DecodeString(wantedRespBase64)
	if err != nil {
		t.Fatal(err)
	}

	gotResp, err := saslXOAUTH2InitialResponse("someuser@example.com", "ya29.vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg")
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(wantedResp, gotResp) {
		t.Errorf("Initial response does not match expected:\n %s", testDiff(gotResp, wantedResp))
	}
}

//RFC6749 defines the OAUTH2 as comprising VSCHAR elements (\x20-7E)
func TestSaslXOAUTH2InvalidBearer(t *testing.T) {
	tests := []struct {
		label   string
		illegal string
	}{
		{
			label:   "char outside range",
			illegal: "illegalChar\x00",
		},
		{
			label:   "empty bearer",
			illegal: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			_, err := saslXOAUTH2InitialResponse("someuser@example.com", tt.illegal)
			if err == nil {
				t.Errorf("Expected invalid bearer to be rejected")
			}
		})
	}
}

func TestConnSASLXOAUTH2AuthSuccess(t *testing.T) {
	buf, err := peerResponse(
		[]byte("AMQP\x03\x01\x00\x00"),
		frame{
			type_:   frameTypeSASL,
			channel: 0,
			body:    &saslMechanisms{Mechanisms: []symbol{saslMechanismXOAUTH2}},
		},
		frame{
			type_:   frameTypeSASL,
			channel: 0,
			body:    &saslOutcome{Code: codeSASLOK},
		},
		[]byte("AMQP\x00\x01\x00\x00"),
		frame{
			type_:   frameTypeAMQP,
			channel: 0,
			body:    &performOpen{},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	c := testconn.New(buf)
	client, err := New(c,
		ConnSASLXOAUTH2("someuser@example.com", "ya29.vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg", 512),
		ConnIdleTimeout(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
}

func TestConnSASLXOAUTH2AuthFail(t *testing.T) {
	buf, err := peerResponse(
		[]byte("AMQP\x03\x01\x00\x00"),
		frame{
			type_:   frameTypeSASL,
			channel: 0,
			body:    &saslMechanisms{Mechanisms: []symbol{saslMechanismXOAUTH2}},
		},
		frame{
			type_:   frameTypeSASL,
			channel: 0,
			body:    &saslOutcome{Code: codeSASLAuth},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	c := testconn.New(buf)
	client, err := New(c,
		ConnSASLXOAUTH2("someuser@example.com", "ya29.vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg", 512),
		ConnIdleTimeout(10*time.Minute))
	if err == nil {
		t.Errorf("authentication is expected to fail ")
	} else if !strings.Contains(err.Error(), fmt.Sprintf("code %#00x", codeSASLAuth)) {
		t.Errorf("unexpected connection failure : %s", err)
	}
	defer client.Close()
}

func TestConnSASLXOAUTH2AuthFailWithErrorResponse(t *testing.T) {
	buf, err := peerResponse(
		[]byte("AMQP\x03\x01\x00\x00"),
		frame{
			type_:   frameTypeSASL,
			channel: 0,
			body:    &saslMechanisms{Mechanisms: []symbol{saslMechanismXOAUTH2}},
		},
		frame{
			type_:   frameTypeSASL,
			channel: 0,
			body:    &saslChallenge{Challenge: []byte(" { \"status\":\"401\", \"schemes\":\"bearer\", \"scope\":\"https://mail.google.com/\" }")},
		},
		frame{
			type_:   frameTypeSASL,
			channel: 0,
			body:    &saslOutcome{Code: codeSASLAuth},
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	c := testconn.New(buf)
	client, err := New(c,
		ConnSASLXOAUTH2("someuser@example.com", "ya29.vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg", 512),
		ConnIdleTimeout(10*time.Minute))
	if err == nil {
		t.Errorf("authentication is expected to fail ")
	} else if !strings.Contains(err.Error(), fmt.Sprintf("code %#00x", codeSASLAuth)) {
		t.Errorf("unexpected connection failure : %s", err)
	}
	defer client.Close()
}

func peerResponse(items ...interface{}) ([]byte, error) {
	buf := make([]byte, 0)
	for _, item := range items {
		if frame, ok := item.(frame); ok {
			b := &buffer{
				b: make([]byte, 0),
				i: 0,
			}
			e := writeFrame(b, frame)
			if e != nil {
				return buf, e
			}
			buf = append(buf, b.bytes()...)
		} else if raw, ok := item.([]byte); ok {
			buf = append(buf, raw...)
		} else {
			return buf, fmt.Errorf("unrecongized type %T", item)
		}
	}
	return buf, nil
}
