// Package cursor signs opaque page cursors with HMAC-SHA-256 under an
// integer configured key_id.
package cursor

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

// ErrInvalid is every cursor rejection (400 invalid_pagination).
var ErrInvalid = errors.New("cursor: invalid")

// Key is one configured cursor key.
type Key struct {
	ID     int64
	Secret []byte
}

// Keyring holds the configured keys; the first is used for new cursors.
type Keyring struct{ Keys []Key }

// Position is what a cursor binds: the operation, the team and the last
// returned position.
type Position struct {
	Operation string `json:"op"`
	TeamID    int64  `json:"team"`
	After     string `json:"after"`
}

type payload struct {
	KeyID int64 `json:"key_id"`
	Position
}

var enc = base64.RawURLEncoding.Strict()

func sign(secret []byte, data string) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(data))
	return m.Sum(nil)
}

// Encode signs p.
func Encode(k Keyring, p Position) (string, error) {
	if len(k.Keys) == 0 {
		return "", errors.New("cursor: no key")
	}
	key := k.Keys[0]
	data, err := json.Marshal(payload{KeyID: key.ID, Position: p})
	if err != nil {
		return "", err
	}
	body := enc.EncodeToString(data)
	return body + "." + enc.EncodeToString(sign(key.Secret, body)), nil
}

// Decode verifies token and checks it was issued for operation and team.
func Decode(k Keyring, token, operation string, teamID int64) (Position, error) {
	body, sig, ok := strings.Cut(token, ".")
	if !ok || body == "" || sig == "" {
		return Position{}, ErrInvalid
	}
	data, err := enc.DecodeString(body)
	if err != nil {
		return Position{}, ErrInvalid
	}
	mac, err := enc.DecodeString(sig)
	if err != nil {
		return Position{}, ErrInvalid
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var p payload
	if err := dec.Decode(&p); err != nil {
		return Position{}, ErrInvalid
	}
	for _, key := range k.Keys {
		if key.ID != p.KeyID {
			continue
		}
		if !hmac.Equal(mac, sign(key.Secret, body)) {
			return Position{}, ErrInvalid
		}
		if p.Operation != operation || p.TeamID != teamID {
			return Position{}, ErrInvalid
		}
		return p.Position, nil
	}
	return Position{}, ErrInvalid
}
