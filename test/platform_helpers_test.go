package test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"gopkg.in/yaml.v3"
)

// configFixture writes a complete, valid configuration with its files.
type configFixture struct {
	dir  string
	path string
	doc  map[string]any
	keys testKeys
}

func writePEM(t *testing.T, path string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "stand-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeCreds(t *testing.T, path string, account nkeys.KeyPair) {
	t.Helper()
	user, _ := nkeys.CreateUser()
	pub, _ := user.PublicKey()
	uc := jwt.NewUserClaims(pub)
	tok, err := uc.Encode(account)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := user.Seed()
	creds, err := jwt.FormatUserConfig(tok, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, creds, 0o600); err != nil {
		t.Fatal(err)
	}
}

func newConfigFixture(t *testing.T) *configFixture {
	t.Helper()
	dir := t.TempDir()
	k := newTestKeys(t)
	f := &configFixture{dir: dir, path: filepath.Join(dir, "config.yaml"), keys: k}
	p := func(n string) string { return filepath.Join(dir, n) }
	writePEM(t, p("ca.pem"))
	writeCreds(t, p("user.creds"), k.account)
	writeCreds(t, p("system.creds"), k.account)
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.WriteFile(p("account-public-key"), []byte(k.accountPub+"\n"), 0o600))
	must(os.WriteFile(p("account.jwt"), []byte(k.initialAccount), 0o600))
	must(os.WriteFile(p("account-signing-key.nk"), seed(t, k.accountSigning), 0o600))
	must(os.WriteFile(p("operator-signing-key.nk"), seed(t, k.operatorSigning), 0o600))
	f.doc = map[string]any{
		"http": map[string]any{"listen": ":8080"},
		"db": map[string]any{"host": "postgres", "port": 5432, "user": "executor_registry", "password": "s3cret", "database": "flowai",
			"sslmode": "verify-full", "ca_file": p("ca.pem")},
		"nats": map[string]any{"url": "tls://nats:4222", "ca_file": p("ca.pem"), "credentials_file": p("user.creds"), "inbox_prefix": "_INBOX.executor-registry"},
		"nats_keys": map[string]any{
			"account_public_key_file": p("account-public-key"), "account_jwt_file": p("account.jwt"),
			"account_signing_key_file": p("account-signing-key.nk"), "operator_signing_key_file": p("operator-signing-key.nk"),
			"system_credentials_file": p("system.creds"),
		},
		"cursor_keys": []any{map[string]any{"id": 1, "key_hex": hex.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))}},
	}
	f.write(t)
	return f
}

func (f *configFixture) write(t *testing.T) {
	t.Helper()
	data, err := yaml.Marshal(f.doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// contractFixturePath resolves the pinned inputs staged outside the checkout.
func contractFixturePath(t *testing.T, path string) string {
	t.Helper()
	root := os.Getenv("FLOWAI_CONTRACT_FIXTURE_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("FLOWAI_CONTRACT_FIXTURE_DIR must name an absolute fixture directory")
	}
	return filepath.Join(root, path)
}

// changedOwners applies the standalone service and schema ownership declarations.
func changedOwners(t *testing.T, file string) string {
	t.Helper()
	const prefix = "svc/executor-registry/"
	if !strings.HasPrefix(file, prefix) {
		t.Fatalf("file outside executor-registry: %s", file)
	}
	if !strings.HasPrefix(file, prefix+"schemas/") || !strings.HasSuffix(file, ".schema.json") {
		return `["executor-registry"]`
	}
	raw, err := os.ReadFile(contractFixturePath(t, "qa-e2e/scripts/shared-test-inputs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var declarations struct {
		Schemas map[string][]string `json:"schemas"`
	}
	if err := json.Unmarshal(raw, &declarations); err != nil {
		t.Fatal(err)
	}
	owners := declarations.Schemas[file]
	if len(owners) == 0 || owners[0] != "executor-registry" {
		t.Fatalf("missing publisher declaration for %s: %v", file, owners)
	}
	encoded, err := json.Marshal(owners)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

// productionGoFiles lists the service's non-test Go files.
func productionGoFiles(t *testing.T) []string {
	t.Helper()
	var out []string
	err := filepath.Walk("..", func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && (info.Name() == "test" || info.Name() == "mocks") {
			return filepath.SkipDir
		}
		if strings.HasSuffix(p, ".go") && !strings.HasSuffix(p, "_test.go") {
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
