package grpc

import (
	"crypto/x509"
	"fmt"
	"strings"
)

var generatedGatewaySANs = map[string]struct{}{
	"ms-gym-api-gateway": {},
	"spiffe://gym.cluster.local/ns/default/sa/ms-gym-api-gateway":    {},
	"spiffe://gym.cluster.local/ns/gym-system/sa/ms-gym-api-gateway": {},
}

func VerifyGeneratedGateway(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	if len(rawCerts) == 0 {
		return fmt.Errorf("missing client certificate")
	}
	certificate, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return err
	}
	for _, name := range certificate.DNSNames {
		if _, ok := generatedGatewaySANs[strings.ToLower(name)]; ok {
			return nil
		}
	}
	for _, uri := range certificate.URIs {
		if _, ok := generatedGatewaySANs[uri.String()]; ok {
			return nil
		}
	}
	return fmt.Errorf("client certificate is not generated gateway")
}
