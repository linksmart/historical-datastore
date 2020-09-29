//The initial code taken from : @Shaneutt. Source: https://gist.github.com/shaneutt/5e1995295cff6721c89a71d13a71c251
package pki

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
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
	ca.PrivKey, err = PemToPrivateKey(caPrivKeyPEM)
	if err != nil {
		return nil, err
	}

	ca.Cert, err = PEMToCertificate(caPEM)
	if err != nil {
		return nil, err
	}
	return ca, err
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

	if server {
		template.ExtKeyUsage = append(template.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, template, ca.Cert, csr.PublicKey, ca.PrivKey)
	if err != nil {
		return nil, err
	}

	return CertificateASN1ToPEM(certBytes)
}

func (ca CertificateAuthority) GetPEMS() (caPEM, caPrivKeyPEM []byte, err error) {
	caPEM, err = CertificateToPEM(*ca.Cert)
	if err != nil {
		return nil, nil, err
	}
	caPrivKeyPEM, err = PrivateKeyToPEM(ca.PrivKey)
	if err != nil {
		return nil, nil, err
	}
	return caPEM, caPrivKeyPEM, nil
}
