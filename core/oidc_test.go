package core

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// jwtParts splits a compact JWT into its decoded header, decoded claims and raw
// signature.
func jwtParts(t *testing.T, token string) (header, claims map[string]any, signingInput string, sig []byte) {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token is not a compact JWT: %q", token)
	}
	decode := func(s string) map[string]any {
		raw, err := base64.RawURLEncoding.DecodeString(s)
		if err != nil {
			t.Fatalf("decode %q: %v", s, err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("unmarshal %q: %v", raw, err)
		}
		return m
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	return decode(parts[0]), decode(parts[1]), parts[0] + "." + parts[1], sig
}

// get performs a request against the signer's discovery handler.
func get(t *testing.T, h http.Handler, path string) []byte {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s = %d", path, rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("GET %s content-type = %q", path, ct)
	}
	return rec.Body.Bytes()
}

// TestSignedTokenVerifiesAgainstPublishedJWKS is the end-to-end proof that a
// target can verify a dispatched OIDC token using only what the emulator
// publishes: fetch the JWKS, rebuild the public key, check the signature.
func TestSignedTokenVerifiesAgainstPublishedJWKS(t *testing.T) {
	s := NewSigner("http://localhost:8980", defaultSigningKey())
	token := s.Token("svc@example.iam.gserviceaccount.com", "https://target.example/handler")

	header, claims, signingInput, sig := jwtParts(t, token)
	if header["alg"] != "RS256" {
		t.Errorf("alg = %v, want RS256", header["alg"])
	}
	if claims["iss"] != "http://localhost:8980" {
		t.Errorf("iss = %v", claims["iss"])
	}
	if claims["aud"] != "https://target.example/handler" {
		t.Errorf("aud = %v", claims["aud"])
	}
	if claims["email"] != "svc@example.iam.gserviceaccount.com" || claims["email_verified"] != true {
		t.Errorf("email claims = %v", claims)
	}

	var jwks struct {
		Keys []struct {
			Kty, Alg, Use, Kid, N, E string
		}
	}
	if err := json.Unmarshal(get(t, s.Handler(), jwksPath), &jwks); err != nil {
		t.Fatalf("jwks: %v", err)
	}
	if len(jwks.Keys) != 1 {
		t.Fatalf("jwks has %d keys, want 1", len(jwks.Keys))
	}
	jwk := jwks.Keys[0]
	if jwk.Kty != "RSA" || jwk.Alg != "RS256" || jwk.Use != "sig" {
		t.Errorf("jwk = %+v", jwk)
	}
	if jwk.Kid != header["kid"] {
		t.Errorf("jwk kid %q != token kid %v", jwk.Kid, header["kid"])
	}

	nBytes, err := base64.RawURLEncoding.DecodeString(jwk.N)
	if err != nil {
		t.Fatalf("decode n: %v", err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(jwk.E)
	if err != nil {
		t.Fatalf("decode e: %v", err)
	}
	pub := &rsa.PublicKey{
		N: new(big.Int).SetBytes(nBytes),
		E: int(new(big.Int).SetBytes(eBytes).Int64()),
	}
	digest := sha256.Sum256([]byte(signingInput))
	if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig); err != nil {
		t.Fatalf("signature does not verify against the published JWKS: %v", err)
	}
}

// TestCertsEndpointServesVerifyingCertificate checks the PEM-certificate shape
// Google publishes at /oauth2/v1/certs, which cert-based verifiers consume.
func TestCertsEndpointServesVerifyingCertificate(t *testing.T) {
	s := NewSigner("http://localhost:8980", defaultSigningKey())
	token := s.Token("svc@example.com", "aud")
	header, _, signingInput, sig := jwtParts(t, token)

	var certs map[string]string
	if err := json.Unmarshal(get(t, s.Handler(), certsPath), &certs); err != nil {
		t.Fatalf("certs: %v", err)
	}
	pemCert, ok := certs[header["kid"].(string)]
	if !ok {
		t.Fatalf("no certificate for kid %v in %v", header["kid"], certs)
	}
	block, _ := pem.Decode([]byte(pemCert))
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatalf("not a PEM certificate: %q", pemCert)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	digest := sha256.Sum256([]byte(signingInput))
	if err := rsa.VerifyPKCS1v15(cert.PublicKey.(*rsa.PublicKey), crypto.SHA256, digest[:], sig); err != nil {
		t.Fatalf("signature does not verify against the published certificate: %v", err)
	}
}

func TestDiscoveryDocument(t *testing.T) {
	s := NewSigner("http://localhost:8980", defaultSigningKey())
	var doc struct {
		Issuer   string   `json:"issuer"`
		JWKSURI  string   `json:"jwks_uri"`
		Algs     []string `json:"id_token_signing_alg_values_supported"`
		Subjects []string `json:"subject_types_supported"`
	}
	if err := json.Unmarshal(get(t, s.Handler(), discoveryPath), &doc); err != nil {
		t.Fatalf("discovery: %v", err)
	}
	if doc.Issuer != "http://localhost:8980" || doc.JWKSURI != "http://localhost:8980/jwks" {
		t.Errorf("discovery = %+v", doc)
	}
	if len(doc.Algs) != 1 || doc.Algs[0] != "RS256" || len(doc.Subjects) == 0 {
		t.Errorf("discovery algs/subjects = %+v", doc)
	}
	if s.Issuer() != "http://localhost:8980" {
		t.Errorf("Issuer() = %q", s.Issuer())
	}
}

// TestSignerDefaultsAndUnsignedFallback covers the production-issuer default
// and the degraded path taken when no signing key is available.
func TestSignerDefaultsAndUnsignedFallback(t *testing.T) {
	if got := NewSigner("", defaultSigningKey()).Issuer(); got != defaultOIDCIssuer {
		t.Errorf("default issuer = %q, want %q", got, defaultOIDCIssuer)
	}

	s := NewSigner("http://localhost:8980", nil)
	token := s.Token("a@b", "aud")
	if !strings.HasSuffix(token, ".") {
		t.Errorf("unsigned token should have an empty signature: %q", token)
	}
	parts := strings.Split(token, ".")
	rawHeader, _ := base64.RawURLEncoding.DecodeString(parts[0])
	if !strings.Contains(string(rawHeader), `"alg":"none"`) {
		t.Errorf("unsigned header = %s", rawHeader)
	}

	var jwks struct{ Keys []any }
	if err := json.Unmarshal(get(t, s.Handler(), jwksPath), &jwks); err != nil {
		t.Fatalf("jwks: %v", err)
	}
	if len(jwks.Keys) != 0 {
		t.Errorf("keyless signer should publish no keys, got %v", jwks.Keys)
	}
	var certs map[string]string
	if err := json.Unmarshal(get(t, s.Handler(), certsPath), &certs); err != nil {
		t.Fatalf("certs: %v", err)
	}
	if len(certs) != 0 {
		t.Errorf("keyless signer should publish no certs, got %v", certs)
	}
	if keyID(nil) != "" {
		t.Error("keyID(nil) should be empty")
	}
}
