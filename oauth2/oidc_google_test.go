package oauth2

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClaimBool_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		input   string
		want    bool
		wantErr bool
	}{
		{input: `{"v": true}`, want: true},
		{input: `{"v": false}`, want: false},
		{input: `{"v": "true"}`, want: true},
		{input: `{"v": "false"}`, want: false},
		{input: `{"v": null}`, want: false},
		{input: `{}`, want: false},
		{input: `{"v": "yes"}`, wantErr: true},
		{input: `{"v": 1}`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			var out struct {
				V claimBool `json:"v"`
			}
			err := json.Unmarshal([]byte(tt.input), &out)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, bool(out.V))
		})
	}
}
