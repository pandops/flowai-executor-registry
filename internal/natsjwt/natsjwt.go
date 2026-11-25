// Package natsjwt issues Executor NATS user JWTs with the application
// account signing key and re-signs the application account JWT with the
// dedicated operator signing key for revocation (design rule 36).
package natsjwt

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// Keys are executor-registry's NATS key material (deployment secrets).
type Keys struct {
	AccountPublicKey    string
	AccountSigningSeed  []byte
	OperatorSigningSeed []byte
	// InitialAccountJWT is the account JWT as deployed; its issuer is the
	// operator identity key, which the operator signing key must not equal.
	InitialAccountJWT string
}

// Identity is the JSON carried in the user JWT's name claim (rule 14).
type Identity struct {
	TeamID       int64  `json:"team_id"`
	ExecutorID   int64  `json:"executor_id"`
	Tag          string `json:"tag"`
	ExecutorType string `json:"executor_type"`
}

// Issued is a freshly issued Executor JWT.
type Issued struct {
	JWT           string
	UserPublicKey string
	Fingerprint   string
	IssuedAt      time.Time
}

// Signer holds the parsed keys.
type Signer struct {
	accountPub string
	accountKey nkeys.KeyPair
	operator   nkeys.KeyPair
}

// NewSigner validates the keys. Errors never carry key material.
func NewSigner(k Keys) (*Signer, error) {
	if !nkeys.IsValidPublicAccountKey(k.AccountPublicKey) {
		return nil, errors.New("natsjwt: account public key is not an account key")
	}
	acc, err := nkeys.FromSeed(k.AccountSigningSeed)
	if err != nil {
		return nil, errors.New("natsjwt: account signing key is not a seed")
	}
	if p, _, err := nkeys.DecodeSeed(k.AccountSigningSeed); err != nil || p != nkeys.PrefixByteAccount {
		return nil, errors.New("natsjwt: account signing key is not an account seed")
	}
	op, err := nkeys.FromSeed(k.OperatorSigningSeed)
	if err != nil {
		return nil, errors.New("natsjwt: operator signing key is not a seed")
	}
	if p, _, err := nkeys.DecodeSeed(k.OperatorSigningSeed); err != nil || p != nkeys.PrefixByteOperator {
		return nil, errors.New("natsjwt: operator signing key is not an operator seed")
	}
	ac, err := jwt.DecodeAccountClaims(k.InitialAccountJWT)
	if err != nil {
		return nil, errors.New("natsjwt: initial account JWT does not decode")
	}
	if ac.Subject != k.AccountPublicKey {
		return nil, errors.New("natsjwt: initial account JWT is for another account")
	}
	accPub, _ := acc.PublicKey()
	opPub, _ := op.PublicKey()
	if accPub == k.AccountPublicKey {
		return nil, errors.New("natsjwt: account signing key must not be the account identity key")
	}
	if !ac.SigningKeys.Contains(accPub) {
		return nil, errors.New("natsjwt: account signing key is not listed in the account JWT")
	}
	if opPub == ac.Issuer {
		return nil, errors.New("natsjwt: operator signing key must not be the operator identity key")
	}
	return &Signer{accountPub: k.AccountPublicKey, accountKey: acc, operator: op}, nil
}

// permissions are the subjects an Executor may use (design rule 36, rule 14).
// nats-server checks a publish after the account mapping, so each .pub
// subject is granted as its mapped .sub subject (deviation D7).
func permissions(id Identity) (pub, sub []string) {
	e := strconv.FormatInt(id.ExecutorID, 10)
	task := TaskConsumerName(id.TeamID, id.Tag)
	cmd := "commands-" + e
	pub = []string{
		"executor.event." + e + ".*.sub",
		"*.executor." + e + ".*.sub",
		"$JS.API.CONSUMER.MSG.NEXT.TASK_MANAGER_TASK." + task,
		"$JS.ACK.TASK_MANAGER_TASK." + task + ".>",
		"$JS.API.CONSUMER.CREATE.TASK_MANAGER_COMMAND." + cmd + ".task-manager.command." + e + ".sub",
		"$JS.API.CONSUMER.MSG.NEXT.TASK_MANAGER_COMMAND." + cmd,
		"$JS.ACK.TASK_MANAGER_COMMAND." + cmd + ".>",
	}
	sub = []string{"_INBOX.executor-" + e + ".>"}
	return pub, sub
}

// IssueExecutor signs a bearer user JWT pinned to the Executor.
func (s *Signer) IssueExecutor(id Identity, now time.Time) (Issued, error) {
	user, err := nkeys.CreateUser()
	if err != nil {
		return Issued{}, fmt.Errorf("natsjwt: create user key: %w", err)
	}
	pubKey, _ := user.PublicKey()
	name, err := json.Marshal(id)
	if err != nil {
		return Issued{}, err
	}
	uc := jwt.NewUserClaims(pubKey)
	uc.Name = string(name)
	uc.IssuerAccount = s.accountPub
	uc.BearerToken = true
	pub, sub := permissions(id)
	uc.Pub.Allow.Add(pub...)
	uc.Sub.Allow.Add(sub...)
	tok, err := uc.Encode(s.accountKey)
	if err != nil {
		return Issued{}, fmt.Errorf("natsjwt: sign user JWT: %w", err)
	}
	return Issued{JWT: tok, UserPublicKey: pubKey, Fingerprint: Fingerprint(tok), IssuedAt: now}, nil
}

// RevokeUser returns accountJWT re-signed by the operator signing key with
// userPublicKey added to its revocation list and every other claim kept.
// The revocation time is the later of at and the wall clock, because the
// JWT's iat comes from the wall clock.
func (s *Signer) RevokeUser(accountJWT, userPublicKey string, at time.Time) (string, error) {
	ac, err := jwt.DecodeAccountClaims(accountJWT)
	if err != nil {
		return "", errors.New("natsjwt: live account JWT does not decode")
	}
	if ac.Subject != s.accountPub {
		return "", errors.New("natsjwt: live account JWT is for another account")
	}
	if wall := time.Now(); wall.After(at) {
		at = wall
	}
	ac.RevokeAt(userPublicKey, at)
	out, err := ac.Encode(s.operator)
	if err != nil {
		return "", fmt.Errorf("natsjwt: sign account JWT: %w", err)
	}
	return out, nil
}

// AccountPublicKey returns the application account public key.
func (s *Signer) AccountPublicKey() string { return s.accountPub }

// TagHash is the first 16 lowercase hex characters of SHA-256(tag).
func TagHash(tag string) string {
	sum := sha256.Sum256([]byte(tag))
	return hex.EncodeToString(sum[:])[:16]
}

// TaskConsumerName is tasks-<team_id>-<tag_hash>.
func TaskConsumerName(teamID int64, tag string) string {
	return "tasks-" + strconv.FormatInt(teamID, 10) + "-" + TagHash(tag)
}

// Fingerprint is the lowercase hex SHA-256 of the JWT.
func Fingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
