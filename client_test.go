package amqp

import (
	"encoding/binary"
	"testing"
)

func TestLinkOptions(t *testing.T) {
	tests := []struct {
		label string
		opts  []LinkOption

		wantSource *source
	}{
		{
			label: "no options",
		},
		{
			label: "selector-filter",
			opts: []LinkOption{
				LinkSelectorFilter("amqp.annotation.x-opt-offset > '100'"),
			},

			wantSource: &source{
				Filter: map[symbol]*describedType{
					"apache.org:selector-filter:string": &describedType{
						descriptor: binary.BigEndian.Uint64([]byte{0x00, 0x00, 0x46, 0x8C, 0x00, 0x00, 0x00, 0x04}),
						value:      "amqp.annotation.x-opt-offset > '100'",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			got, err := newLink(nil, nil, tt.opts)
			if err != nil {
				t.Fatal(err)
			}

			if !testEqual(got.source, tt.wantSource) {
				t.Errorf("Properties don't match expected:\n %s", testDiff(got.source, tt.wantSource))
			}
		})
	}
}
