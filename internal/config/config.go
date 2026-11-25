// Package config loads executor-registry's YAML configuration
// (svc/executor-registry/config.example.yaml lists every key).
package config

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultSchema is the service's default PostgreSQL schema.
const DefaultSchema = "executor_registry_schema"

var (
	defaultExecutorTypes = []string{"executor_docker_openhands", "executor_k8s_openhands"}
	identifier           = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
)

type HTTP struct {
	Listen string `yaml:"listen"`
}

type DB struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	Schema   string `yaml:"schema"`
	SSLMode  string `yaml:"sslmode"`
	CAFile   string `yaml:"ca_file"`
}

type NATS struct {
	URL             string        `yaml:"url"`
	CAFile          string        `yaml:"ca_file"`
	CredentialsFile string        `yaml:"credentials_file"`
	InboxPrefix     string        `yaml:"inbox_prefix"`
	RequestTimeout  time.Duration `yaml:"request_timeout"`
}

type NATSKeys struct {
	AccountPublicKeyFile   string `yaml:"account_public_key_file"`
	AccountJWTFile         string `yaml:"account_jwt_file"`
	AccountSigningKeyFile  string `yaml:"account_signing_key_file"`
	OperatorSigningKeyFile string `yaml:"operator_signing_key_file"`
	SystemCredentialsFile  string `yaml:"system_credentials_file"`
}

type CursorKey struct {
	ID     int64  `yaml:"id"`
	KeyHex string `yaml:"key_hex"`
}

// Config is the whole file.
type Config struct {
	HTTP          HTTP        `yaml:"http"`
	DB            DB          `yaml:"db"`
	NATS          NATS        `yaml:"nats"`
	NATSKeys      NATSKeys    `yaml:"nats_keys"`
	CursorKeys    []CursorKey `yaml:"cursor_keys"`
	ExecutorTypes []string    `yaml:"executor_types"`
}

// Load reads, defaults and validates the file at path.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: read: %w", err)
	}
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("config: parse: %w", err)
	}
	if c.HTTP.Listen == "" {
		c.HTTP.Listen = ":8080"
	}
	if c.DB.Port == 0 {
		c.DB.Port = 5432
	}
	if c.DB.Schema == "" {
		c.DB.Schema = DefaultSchema
	}
	if c.NATS.InboxPrefix == "" {
		c.NATS.InboxPrefix = "_INBOX.executor-registry"
	}
	if c.NATS.RequestTimeout == 0 {
		c.NATS.RequestTimeout = 5 * time.Second
	}
	if len(c.ExecutorTypes) == 0 {
		c.ExecutorTypes = append([]string(nil), defaultExecutorTypes...)
	}
	return c, c.validate()
}

func readable(key, path string) error {
	if path == "" {
		return fmt.Errorf("config: %s is required", key)
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("config: %s is not readable", key)
	}
	return f.Close()
}

func (c Config) validate() error {
	var errs []error
	req := func(key, v string) {
		if strings.TrimSpace(v) == "" {
			errs = append(errs, fmt.Errorf("config: %s is required", key))
		}
	}
	file := func(key, path string) {
		if err := readable(key, path); err != nil {
			errs = append(errs, err)
		}
	}
	req("http.listen", c.HTTP.Listen)
	req("db.host", c.DB.Host)
	req("db.user", c.DB.User)
	req("db.password", c.DB.Password)
	req("db.database", c.DB.Database)
	if c.DB.Port < 1 || c.DB.Port > 65535 {
		errs = append(errs, errors.New("config: db.port is out of range"))
	}
	if !identifier.MatchString(c.DB.Schema) {
		errs = append(errs, errors.New("config: db.schema is not a lowercase identifier"))
	}
	if c.DB.SSLMode != "verify-full" {
		errs = append(errs, errors.New("config: db.sslmode must be verify-full"))
	}
	file("db.ca_file", c.DB.CAFile)
	if !strings.HasPrefix(c.NATS.URL, "tls://") {
		errs = append(errs, errors.New("config: nats.url must use tls://"))
	}
	file("nats.ca_file", c.NATS.CAFile)
	file("nats.credentials_file", c.NATS.CredentialsFile)
	if c.NATS.RequestTimeout < 0 || c.NATS.RequestTimeout > time.Minute {
		errs = append(errs, errors.New("config: nats.request_timeout is out of range"))
	}
	file("nats_keys.account_public_key_file", c.NATSKeys.AccountPublicKeyFile)
	file("nats_keys.account_jwt_file", c.NATSKeys.AccountJWTFile)
	file("nats_keys.account_signing_key_file", c.NATSKeys.AccountSigningKeyFile)
	file("nats_keys.operator_signing_key_file", c.NATSKeys.OperatorSigningKeyFile)
	file("nats_keys.system_credentials_file", c.NATSKeys.SystemCredentialsFile)
	if len(c.CursorKeys) == 0 {
		errs = append(errs, errors.New("config: cursor_keys is required"))
	}
	seen := map[int64]bool{}
	for i, k := range c.CursorKeys {
		b, err := hex.DecodeString(k.KeyHex)
		if k.ID < 1 || seen[k.ID] || err != nil || len(b) < 32 {
			errs = append(errs, fmt.Errorf("config: cursor_keys[%d] needs a unique positive id and at least 32 bytes of hex", i))
		}
		seen[k.ID] = true
	}
	for _, t := range c.ExecutorTypes {
		ok := false
		for _, d := range defaultExecutorTypes {
			ok = ok || t == d
		}
		if !ok {
			errs = append(errs, fmt.Errorf("config: executor_types: unsupported %q", t))
		}
	}
	return errors.Join(errs...)
}

// CursorSecrets returns the decoded cursor keys in configuration order.
func (c Config) CursorSecrets() ([]int64, [][]byte) {
	var ids []int64
	var keys [][]byte
	for _, k := range c.CursorKeys {
		b, _ := hex.DecodeString(k.KeyHex)
		ids = append(ids, k.ID)
		keys = append(keys, b)
	}
	return ids, keys
}

func dsnValue(v string) string {
	if v != "" && !strings.ContainsAny(v, " '\\") {
		return v
	}
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
}

// DSN returns the pgx connection string with search_path set to the schema.
func (c DB) DSN() string {
	parts := []string{
		"host=" + dsnValue(c.Host),
		"port=" + strconv.Itoa(c.Port),
		"user=" + dsnValue(c.User),
		"password=" + dsnValue(c.Password),
		"dbname=" + dsnValue(c.Database),
		"search_path=" + dsnValue(c.Schema),
		"sslmode=" + dsnValue(c.SSLMode),
		"sslrootcert=" + dsnValue(c.CAFile),
	}
	return strings.Join(parts, " ")
}
