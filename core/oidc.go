package core

// OIDC token signing and the OpenID discovery endpoints.
//
// Cloud Tasks attaches a Google-signed OIDC ID token to a task's HTTP request
// when the task carries an OidcToken. The emulator cannot mint Google-signed
// tokens, so it signs with its own RSA key and publishes the matching public
// key. A target can then verify the token end-to-end against the emulator as
// the issuer, instead of having to skip verification entirely.

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"sync"
	"time"
)

// defaultOIDCIssuer is the `iss` claim used when no issuer is configured; it
// matches what production Cloud Tasks puts in the token.
const defaultOIDCIssuer = "https://accounts.google.com"

// Paths served by the discovery handler, matching the endpoints Google
// publishes (and the ones aertje/cloud-tasks-emulator exposes).
const (
	discoveryPath = "/.well-known/openid-configuration"
	jwksPath      = "/jwks"
	certsPath     = "/certs"
)

// defaultSigningKey is the process-wide RSA key used to sign OIDC tokens. It is
// generated once, lazily: generation costs ~100ms and every engine in a test
// binary can share it. A nil key (generation failed) degrades to unsigned
// tokens rather than failing startup.
var defaultSigningKey = sync.OnceValue(func() *rsa.PrivateKey {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	return key
})

// Signer mints OIDC ID tokens for dispatched tasks and serves the OpenID
// discovery documents that let a target verify them.
type Signer struct {
	issuer    string
	key       *rsa.PrivateKey
	kid       string
	jwks      []byte
	certs     []byte
	discovery []byte
}

// NewSigner builds a Signer for the given issuer URL. An empty issuer falls
// back to the production issuer. A nil key produces unsigned (`alg=none`)
// tokens and empty key sets, which is the best the emulator can do if key
// generation is unavailable.
func NewSigner(issuer string, key *rsa.PrivateKey) *Signer {
	if issuer == "" {
		issuer = defaultOIDCIssuer
	}
	s := &Signer{issuer: issuer, key: key}
	s.kid = keyID(key)
	s.jwks = buildJWKS(key, s.kid)
	s.certs = buildCerts(key, s.kid, issuer)
	s.discovery, _ = json.Marshal(map[string]any{
		"issuer":                                issuer,
		"jwks_uri":                              issuer + jwksPath,
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"response_types_supported":              []string{"id_token"},
		"subject_types_supported":               []string{"public"},
		"claims_supported": []string{
			"aud", "email", "email_verified", "exp", "iat", "iss", "sub",
		},
	})
	return s
}

// Issuer returns the `iss` claim the signer stamps on its tokens.
func (s *Signer) Issuer() string { return s.issuer }

// Token builds an OIDC ID token for the service account and audience, signed
// with RS256 when a key is available.
func (s *Signer) Token(email, audience string) string {
	now := time.Now()
	claims, _ := json.Marshal(map[string]any{
		"iss":            s.issuer,
		"aud":            audience,
		"azp":            email,
		"email":          email,
		"email_verified": true,
		"sub":            email,
		"iat":            now.Unix(),
		"exp":            now.Add(time.Hour).Unix(),
	})

	if s.key == nil {
		return b64url([]byte(`{"alg":"none","typ":"JWT"}`)) + "." + b64url(claims) + "."
	}

	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": s.kid})
	signingInput := b64url(header) + "." + b64url(claims)
	digest := sha256.Sum256([]byte(signingInput))
	sig, _ := rsa.SignPKCS1v15(rand.Reader, s.key, crypto.SHA256, digest[:])
	return signingInput + "." + b64url(sig)
}

// Handler serves the OpenID discovery document and the public keys in both the
// JWKS and the PEM-certificate shapes Google publishes.
func (s *Signer) Handler() http.Handler {
	mux := http.NewServeMux()
	serve := func(body []byte) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		}
	}
	mux.HandleFunc(discoveryPath, serve(s.discovery))
	mux.HandleFunc(jwksPath, serve(s.jwks))
	mux.HandleFunc(certsPath, serve(s.certs))
	return mux
}

// keyID derives a stable key id from the public key, the way JWKS consumers
// expect one to be stable for the lifetime of the key.
func keyID(key *rsa.PrivateKey) string {
	if key == nil {
		return ""
	}
	der, _ := x509.MarshalPKIXPublicKey(&key.PublicKey)
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:10])
}

// buildJWKS renders the public key as a JWK set.
func buildJWKS(key *rsa.PrivateKey, kid string) []byte {
	keys := []any{}
	if key != nil {
		keys = append(keys, map[string]string{
			"kty": "RSA",
			"alg": "RS256",
			"use": "sig",
			"kid": kid,
			"n":   b64url(key.N.Bytes()),
			"e":   b64url(big.NewInt(int64(key.E)).Bytes()),
		})
	}
	out, _ := json.Marshal(map[string]any{"keys": keys})
	return out
}

// buildCerts renders the public key as the `{kid: PEM certificate}` map served
// by Google's /oauth2/v1/certs endpoint, wrapping the key in a self-signed
// certificate so cert-based verifiers work too.
func buildCerts(key *rsa.PrivateKey, kid, issuer string) []byte {
	certs := map[string]string{}
	if key != nil {
		now := time.Now()
		tmpl := &x509.Certificate{
			SerialNumber:          big.NewInt(1),
			Subject:               pkix.Name{CommonName: issuer},
			NotBefore:             now.Add(-time.Hour),
			NotAfter:              now.AddDate(10, 0, 0),
			KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
			IsCA:                  true,
			BasicConstraintsValid: true,
		}
		der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		certs[kid] = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	}
	out, _ := json.Marshal(certs)
	return out
}

func b64url(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}
