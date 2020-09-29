package pki

import (
	"bytes"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

func PemToPrivateKey(caPrivKeyPEM []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(caPrivKeyPEM)
	if block == nil {
		return nil, fmt.Errorf("unabled to decode CA private key")
	}
	return x509.ParsePKCS1PrivateKey(block.Bytes)
}

func PrivateKeyToPEM(privKey *rsa.PrivateKey) ([]byte, error) {
	privKeyBuff := new(bytes.Buffer)
	err := pem.Encode(privKeyBuff, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privKey),
	})
	if err != nil {
		return nil, err
	}
	return privKeyBuff.Bytes(), nil
}

func PEMToCertificate(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, fmt.Errorf("unabled to decode PEM for CA certificate")
	}
	return x509.ParseCertificate(block.Bytes)
}

func CertificateToPEM(certificate x509.Certificate) ([]byte, error) {
	return CertificateASN1ToPEM(certificate.Raw)
}

func CertificateASN1ToPEM(certificateASN []byte) ([]byte, error) {
	caPEMBuff := new(bytes.Buffer)
	err := pem.Encode(caPEMBuff, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certificateASN,
	})
	if err != nil {
		return nil, err
	}
	return caPEMBuff.Bytes(), nil
}

func CSRToPEM(csr *x509.CertificateRequest) ([]byte, error) {
	return CSRASN1ToPEM(csr.Raw)
}

func CSRASN1ToPEM(csrAsn1 []byte) ([]byte, error) {
	pemBuff := new(bytes.Buffer)
	err := pem.Encode(pemBuff, &pem.Block{
		Type:  "CERTIFICATE REQUEST",
		Bytes: csrAsn1.Raw,
	})
	if err != nil {
		return nil, err
	}
	return pemBuff.Bytes(), nil
}

func PEMToCSR(csrPEM []byte) (*x509.CertificateRequest, error) {
	block, _ := pem.Decode(csrPEM)
	if block == nil {
		return nil, fmt.Errorf("unabled to decode PEM for CA certificate")
	}
	return x509.ParseCertificateRequest(block.Bytes)
}
