package common

import (
	"fmt"
	"os"
)

// TLSCredentialsFromParams fills the TLS credentials that are not already set (e.g. from a Kubernetes secret)
// from PEM files referenced by the target params "tlsCaFile", "tlsCertFile" and "tlsKeyFile".
// This lets statically configured targets (e.g. in a Docker Compose deployment) use a private CA or client certificates.
func TLSCredentialsFromParams(creds TLSCredentials, params map[string]string) (TLSCredentials, error) {
	read := func(dst *string, key string) error {
		path := params[key]
		if path == "" || *dst != "" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", key, err)
		}
		*dst = string(data)
		return nil
	}
	if err := read(&creds.CA, "tlsCaFile"); err != nil {
		return creds, err
	}
	if err := read(&creds.Cert, "tlsCertFile"); err != nil {
		return creds, err
	}
	if err := read(&creds.Key, "tlsKeyFile"); err != nil {
		return creds, err
	}
	if (creds.Cert != "") != (creds.Key != "") {
		return creds, fmt.Errorf("tlsCertFile and tlsKeyFile must be set together")
	}
	return creds, nil
}

// TLSEnabled reports whether a target should connect over TLS given its "tls" param
// ("true", "skip-verify" or empty/"false") and the TLS credentials available.
func TLSEnabled(creds TLSCredentials, tlsParam string) bool {
	if tlsParam == "false" {
		return false
	}
	return tlsParam == "true" || tlsParam == "skip-verify" || creds.CA != "" || (creds.Cert != "" && creds.Key != "")
}
