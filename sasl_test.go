package amqp

import (
	"bytes"
	"encoding/base64"
	"testing"
)

// Known good strings from https://developers.google.com/gmail/imap/xoauth2-protocol#the_sasl_xoauth2_mechanism
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
