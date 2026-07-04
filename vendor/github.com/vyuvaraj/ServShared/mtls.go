package ServShared

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

// MeshTransport provides an HTTP transport with auto-provisioned mTLS certificates
// from the ServMesh CA. Use this for all inter-service communication in production.
type MeshTransport struct {
	serviceName string
	meshURL     string
	transport   *http.Transport
	mu          sync.RWMutex
	certExpiry  time.Time
}

// SecureTransport creates an http.Transport that uses mTLS certificates
// provisioned from the ServMesh CA. If SERV_MESH_URL is not set, returns
// a plain transport (dev mode).
func SecureTransport(serviceName string) *http.Transport {
	meshURL := os.Getenv("SERV_MESH_URL")
	if meshURL == "" {
		// Dev mode — no mTLS, plain HTTP
		return &http.Transport{}
	}

	mt := &MeshTransport{
		serviceName: serviceName,
		meshURL:     meshURL,
	}

	transport, err := mt.provisionCert()
	if err != nil {
		// Fallback to plain transport on error
		fmt.Printf("[mtls] Warning: failed to provision cert from %s: %v. Using plain HTTP.\n", meshURL, err)
		return &http.Transport{}
	}

	// Background cert rotation (every 24h)
	go mt.rotationLoop()

	return transport
}

// SecureClient creates an http.Client with mTLS transport.
func SecureClient(serviceName string) *http.Client {
	return &http.Client{
		Transport: SecureTransport(serviceName),
		Timeout:   10 * time.Second,
	}
}

func (mt *MeshTransport) provisionCert() (*http.Transport, error) {
	// Generate a new ECDSA private key
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}

	// Create a CSR
	csrTemplate := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   mt.serviceName + ".servverse",
			Organization: []string{"Servverse"},
		},
		DNSNames: []string{mt.serviceName, mt.serviceName + ".servverse", "localhost"},
	}

	csrDER, err := x509.CreateCertificateRequest(rand.Reader, csrTemplate, privateKey)
	if err != nil {
		return nil, fmt.Errorf("create CSR: %w", err)
	}

	csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})

	// Send CSR to ServMesh CA
	reqBody, _ := json.Marshal(map[string]string{
		"service": mt.serviceName,
		"csr":     string(csrPEM),
	})

	resp, err := http.Post(mt.meshURL+"/api/csr", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("request CA: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("CA returned status %d", resp.StatusCode)
	}

	var caResp struct {
		Certificate string `json:"certificate"`
		CA          string `json:"ca"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&caResp); err != nil {
		return nil, fmt.Errorf("decode CA response: %w", err)
	}

	// Parse the signed cert
	certBlock, _ := pem.Decode([]byte(caResp.Certificate))
	if certBlock == nil {
		return nil, fmt.Errorf("invalid certificate PEM from CA")
	}

	// Build TLS certificate
	keyDER, _ := x509.MarshalECPrivateKey(privateKey)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	tlsCert, err := tls.X509KeyPair([]byte(caResp.Certificate), keyPEM)
	if err != nil {
		return nil, fmt.Errorf("load key pair: %w", err)
	}

	// Build CA pool
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM([]byte(caResp.CA))

	// Create TLS transport
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			Certificates: []tls.Certificate{tlsCert},
			RootCAs:      caPool,
			MinVersion:   tls.VersionTLS13,
		},
	}

	mt.mu.Lock()
	mt.transport = transport
	mt.certExpiry = time.Now().Add(30 * 24 * time.Hour) // certs valid 30 days
	mt.mu.Unlock()

	return transport, nil
}

func (mt *MeshTransport) rotationLoop() {
	for {
		mt.mu.RLock()
		expiry := mt.certExpiry
		mt.mu.RUnlock()

		// Rotate 24h before expiry
		sleepDuration := time.Until(expiry.Add(-24 * time.Hour))
		if sleepDuration < 1*time.Hour {
			sleepDuration = 1 * time.Hour
		}

		time.Sleep(sleepDuration)

		newTransport, err := mt.provisionCert()
		if err != nil {
			fmt.Printf("[mtls] Cert rotation failed: %v. Will retry in 1h.\n", err)
			continue
		}

		mt.mu.Lock()
		mt.transport = newTransport
		mt.mu.Unlock()
		fmt.Printf("[mtls] Certificate rotated for service %s\n", mt.serviceName)
	}
}
