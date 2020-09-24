//The initial code taken from : @Shaneutt. Source: https://gist.github.com/shaneutt/5e1995295cff6721c89a71d13a71c251
package pki

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"time"
)

type CertificateAuthority struct {
	PrivKey *rsa.PrivateKey
	Cert    *x509.Certificate
}

var maxSerialNumber = new(big.Int).Lsh(big.NewInt(1), 128)

//creates a new selfsigned CA.
func NewCA(subject pkix.Name) (ca *CertificateAuthority, err error) {
	sNum, err := rand.Int(rand.Reader, maxSerialNumber)
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		Subject:               subject,
		SerialNumber:          sNum,
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		MaxPathLenZero:        true,
		Issuer:                subject,
		IsCA:                  true,
	}

	ca = new(CertificateAuthority)
	// create our private and public key
	ca.PrivKey, err = rsa.GenerateKey(rand.Reader, 2024)
	if err != nil {
		return nil, err
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, template, template, ca.PrivKey.Public(), ca.PrivKey)
	if err != nil {
		return nil, err
	}

	ca.Cert, err = x509.ParseCertificate(caBytes)
	if err != nil {
		return nil, err
	}

	return ca, nil
}

func NewCAFromFile(caFile, caKeyFile string) (ca *CertificateAuthority, err error) {
	caPEM, err := ioutil.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("unable to open ca file: %v", err)
	}
	caPrivKeyPEM, err := ioutil.ReadFile(caKeyFile)
	if err != nil {
		return nil, fmt.Errorf("unable to open key file: %v", err)
	}

	ca = new(CertificateAuthority)
	block, _ := pem.Decode(caPrivKeyPEM)
	if block == nil {
		return nil, fmt.Errorf("unabled to decode CA private key")
	}
	ca.PrivKey, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	block, _ = pem.Decode(caPEM)
	if block == nil {
		return nil, fmt.Errorf("unabled to decode PEM for CA certificate")
	}
	ca.Cert, err = x509.ParseCertificate(block.Bytes)

	return ca, nil
}

//generate certificate for a given
func (ca CertificateAuthority) CreateCertificate(csr *x509.CertificateRequest, server bool) ([]byte, error) {
	sNum, err := rand.Int(rand.Reader, maxSerialNumber)
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		Signature:          csr.Signature,
		SignatureAlgorithm: csr.SignatureAlgorithm,
		SerialNumber:       sNum,
		PublicKeyAlgorithm: csr.PublicKeyAlgorithm,
		PublicKey:          csr.PublicKey,
		Subject:            csr.Subject,
		DNSNames:           csr.DNSNames,
		IPAddresses:        csr.IPAddresses,
		EmailAddresses:     csr.EmailAddresses,
		NotBefore:          time.Now(),
		KeyUsage:           x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:        []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	/*
		cert := &x509.Certificate{
			SerialNumber: sNum,
			Subject: pkix.Name{
				Organization:  []string{"BIMERR EU Project"},
				Country:       []string{"DE"},
				Province:      []string{""},
				Locality:      []string{"Sankt Augustin"},
				StreetAddress: []string{"Schloss Birlinghoven 1"},
				PostalCode:    []string{"53757"},
			},
			IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
			DNSNames:     []string{"localhost"},
			NotBefore:    time.Now(),
			NotAfter:     time.Now().AddDate(10, 0, 0),
			SubjectKeyId: []byte{1, 2, 3, 4, 6},
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
			KeyUsage:     x509.KeyUsageDigitalSignature,
		}
	*/
	certBytes, err := x509.CreateCertificate(rand.Reader, template, ca.Cert, csr.PublicKey, ca.PrivKey)
	if err != nil {
		return nil, err
	}
	certPEM := new(bytes.Buffer)
	pem.Encode(certPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})
	return certPEM.Bytes(), nil
}

func (ca CertificateAuthority) GetPEMS() (caPEM, caPrivKeyPEM []byte) {
	// pem encode
	caPEMBuff := new(bytes.Buffer)
	pem.Encode(caPEMBuff, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: ca.Cert.Raw,
	})

	caPrivKeyPEMBuff := new(bytes.Buffer)
	pem.Encode(caPrivKeyPEMBuff, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(ca.PrivKey),
	})
	return caPEMBuff.Bytes(), caPrivKeyPEMBuff.Bytes()
}
