package server

import (
	"crypto/tls"
	"sync"

	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type CertReloader struct {
	certPath string
	keyPath  string
	mu       sync.RWMutex
	cert     *tls.Certificate
}

func NewCertReloader(certPath string, privateKeyPath string) (*CertReloader, error) {
	reloader := &CertReloader{
		certPath: certPath,
		keyPath:  privateKeyPath,
	}

	err := utils.OnFileChange([]string{certPath, privateKeyPath}, func() {
		log.Infof("NewCertReloader: Reloading certificate")
		err := reloader.reload()
		if err != nil {
			log.Errorf("NewCertReloader: Error reloading certificate: %v", err)
		} else {
			log.Infof("NewCertReloader: Successfully reloaded certificate")
		}
	})
	if err != nil {
		log.Errorf("NewCertReloader: Error setting up certificate reloading: %v", err)
		return nil, err
	}

	if err := reloader.reload(); err != nil {
		log.Errorf("NewCertReloader: Error loading certificate: %v", err)
		return reloader, err
		// return nil, err
	}

	return reloader, nil
}

func (cr *CertReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	log.Errorf("andrew inside GetCertificate ===============================")
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	return cr.cert, nil
}

func (cr *CertReloader) reload() error {
	cert, err := tls.LoadX509KeyPair(cr.certPath, cr.keyPath)
	if err != nil {
		log.Errorf("reload: Error loading certificate: %v", err)
		return err
	}

	cr.mu.Lock()
	cr.cert = &cert
	cr.mu.Unlock()

	return nil
}
