package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sync"
	"time"

	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type CertReloader struct {
	certPath string
	keyPath  string
	mu       sync.RWMutex
	cert     *tls.Certificate
}

const refreshIntervalSeconds = 60

func NewCertReloader(certPath string, privateKeyPath string) (*CertReloader, error) {
	reloader := &CertReloader{
		certPath: certPath,
		keyPath:  privateKeyPath,
	}

	err := utils.WatchFileChanges([]string{certPath, privateKeyPath}, refreshIntervalSeconds, func() {
		log.Infof("NewCertReloader: Reloading certificate at %v with key at %v", certPath, privateKeyPath)
		err := reloader.reload()
		if err != nil {
			log.Errorf("NewCertReloader: Error reloading certificate at %v with key at %v; err=%v",
				certPath, privateKeyPath, err)
		} else {
			log.Infof("NewCertReloader: Successfully reloaded certificate at %v with key at %v", certPath, privateKeyPath)
		}
	})
	if err != nil {
		log.Errorf("NewCertReloader: Error setting up certificate reloading for certificate at %v with key at %v; err=%v",
			certPath, privateKeyPath, err)
		return nil, err
	}

	if err := reloader.reload(); err != nil {
		log.Errorf("NewCertReloader: Error loading certificate at %v with key at %v; err=%v",
			certPath, privateKeyPath, err)
		return nil, err
	} else {
		log.Infof("NewCertReloader: Successfully loaded certificate at %v with key at %v", certPath, privateKeyPath)
	}

	return reloader, nil
}

func (cr *CertReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	return cr.cert, nil
}

func (cr *CertReloader) reload() error {
	cert, err := tls.LoadX509KeyPair(cr.certPath, cr.keyPath)
	if err != nil {
		log.Errorf("reload: Error loading certificate at %v with key at %v; err=%v",
			cr.certPath, cr.keyPath, err)
		return err
	}

	// Check if the certificate time is valid
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		log.Errorf("reload: Error parsing certificate: %v", err)
		return err
	}

	now := time.Now()
	if now.Before(leaf.NotBefore) {
		return fmt.Errorf("reload: certificate at %v is not valid yet (not before: %v)",
			cr.certPath, leaf.NotBefore)
	}
	if now.After(leaf.NotAfter) {
		return fmt.Errorf("reload: certificate at %v has expired (not after: %v)",
			cr.certPath, leaf.NotAfter)
	}

	cr.mu.Lock()
	cr.cert = &cert
	cr.mu.Unlock()

	return nil
}
