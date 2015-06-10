// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package security

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/util"
)

// Utility to generate x509 certificates, both CA and not.
// This is mostly based on http://golang.org/src/crypto/tls/generate_cert.go
// Most fields and settings are hard-coded. TODO(marc): allow customization.

const (
	validFor      = time.Hour * 24 * 365
	maxPathLength = 2
)

// generateKeyPair returns a random 'keySize' bit RSA key pair.
func generateKeyPair(keySize int) (crypto.PrivateKey, crypto.PublicKey, error) {
	private, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, nil, err
	}
	public := private.Public()
	return private, public, err
}

// privateKeyPEMBlock generates a PEM block from a private key.
func privateKeyPEMBlock(key crypto.PrivateKey) (*pem.Block, error) {
	switch k := key.(type) {
	case *rsa.PrivateKey:
		return &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}, nil
	case *ecdsa.PrivateKey:
		bytes, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			return nil, util.Errorf("error marshalling ECDSA key: %s", err)
		}
		return &pem.Block{Type: "EC PRIVATE KEY", Bytes: bytes}, nil
	default:
		return nil, util.Errorf("unknown key type: %v", k)
	}
}

// certificatePEMBlock generates a PEM block from a certificate.
func certificatePEMBlock(cert []byte) (*pem.Block, error) {
	return &pem.Block{Type: "CERTIFICATE", Bytes: cert}, nil
}

// newTemplate returns a partially-filled template.
// It should be further populated based on whether the cert is for a CA or node.
func newTemplate(commonName string) (*x509.Certificate, error) {
	// Generate a random serial number.
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, err
	}

	notBefore := time.Now()

	// TODO(marc): figure out what else we should set. eg: more Subject fields, MaxPathLen, etc...
	cert := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Cockroach"},
			CommonName:   commonName,
		},
		NotBefore: notBefore,
		NotAfter:  notBefore.Add(validFor),

		KeyUsage: x509.KeyUsageKeyEncipherment |
			x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		MaxPathLen:            maxPathLength,
	}

	return cert, nil
}

// GenerateCA generates a CA certificate and returns the cert bytes as
// well as the private key used to generate the certificate.
func GenerateCA(keySize int) ([]byte, crypto.PrivateKey, error) {
	privateKey, publicKey, err := generateKeyPair(keySize)
	if err != nil {
		return nil, nil, err
	}

	template, err := newTemplate("Cockroach CA")
	if err != nil {
		return nil, nil, err
	}

	// Set CA-specific fields.
	template.IsCA = true
	template.KeyUsage |= x509.KeyUsageCertSign
	template.KeyUsage |= x509.KeyUsageContentCommitment

	certBytes, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		return nil, nil, err
	}

	return certBytes, privateKey, nil
}

// GenerateNodeCert generates a node certificate and returns the cert bytes as
// well as the private key used to generate the certificate.
// The CA cert and private key should be passed in.
func GenerateNodeCert(caCert *x509.Certificate, caKey crypto.PrivateKey, keySize int, hosts []string) (
	[]byte, crypto.PrivateKey, error) {
	privateKey, publicKey, err := generateKeyPair(keySize)
	if err != nil {
		return nil, nil, err
	}

	template, err := newTemplate("Cockroach Node")
	if err != nil {
		return nil, nil, err
	}

	// Set node-specific fields.
	// Nodes needs SSL for both server and client authentication.
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	if hosts != nil {
		for _, h := range hosts {
			if ip := net.ParseIP(h); ip != nil {
				template.IPAddresses = append(template.IPAddresses, ip)
			} else {
				template.DNSNames = append(template.DNSNames, h)
			}
		}
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, template, caCert, publicKey, caKey)
	if err != nil {
		return nil, nil, err
	}

	return certBytes, privateKey, nil
}

// GenerateClientCert generates a client certificate and returns the cert bytes as
// well as the private key used to generate the certificate.
// The CA cert and private key should be passed in.
// 'user' is the unique username stored in the Subject.CommonName field.
func GenerateClientCert(caCert *x509.Certificate, caKey crypto.PrivateKey, keySize int, name string) (
	[]byte, crypto.PrivateKey, error) {

	privateKey, publicKey, err := generateKeyPair(keySize)
	if err != nil {
		return nil, nil, err
	}

	// TODO(marc): should we add extra checks?
	if len(name) == 0 {
		return nil, nil, util.Errorf("name cannot be empty")
	}

	template, err := newTemplate(name)
	if err != nil {
		return nil, nil, err
	}

	// Set client-specific fields.
	// Client authentication only.
	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}

	certBytes, err := x509.CreateCertificate(rand.Reader, template, caCert, publicKey, caKey)
	if err != nil {
		return nil, nil, err
	}

	return certBytes, privateKey, nil
}
