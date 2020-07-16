/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package certs

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rjeczalik/notify"
)

// A Certs represents a certificate manager able to watch certificate
// and key pairs for changes.
type Certs struct {
	sync.RWMutex
	// user input params.
	certFile string
	keyFile  string
	loadCert LoadX509KeyPairFunc

	// points to the latest certificate.
	cert *tls.Certificate

	// internal param to track for events, also
	// used to close the watcher.
	e chan notify.EventInfo
}

// LoadX509KeyPairFunc - provides a type for custom cert loader function.
type LoadX509KeyPairFunc func(certFile, keyFile string) (tls.Certificate, error)

// New initializes a new certs monitor.
func New(certFile, keyFile string, loadCert LoadX509KeyPairFunc) (*Certs, error) {
	certFileIsLink, err := checkSymlink(certFile)
	if err != nil {
		return nil, err
	}
	keyFileIsLink, err := checkSymlink(keyFile)
	if err != nil {
		return nil, err
	}
	c := &Certs{
		certFile: certFile,
		keyFile:  keyFile,
		loadCert: loadCert,
		// Make the channel buffered to ensure no event is dropped. Notify will drop
		// an event if the receiver is not able to keep up the sending pace.
		e: make(chan notify.EventInfo, 1),
	}

	if certFileIsLink && keyFileIsLink {
		if err := c.watchSymlinks(); err != nil {
			return nil, err
		}
	} else {
		if err := c.watch(); err != nil {
			return nil, err
		}
	}

	return c, nil
}

func checkSymlink(file string) (bool, error) {
	st, err := os.Lstat(file)
	if err != nil {
		return false, err
	}
	return st.Mode()&os.ModeSymlink == os.ModeSymlink, nil
}

// watchSymlinks reloads symlinked files since fsnotify cannot watch
// on symbolic links.
func (c *Certs) watchSymlinks() (err error) {
	cert, err := c.loadCert(c.certFile, c.keyFile)
	if err != nil {
		return err
	}
	c.Lock()
	c.cert = &cert
	c.Unlock()
	go func() {
		for {
			select {
			case <-c.e:
				// Once stopped exits this routine.
				return
			case <-time.After(24 * time.Hour):
				cert, cerr := c.loadCert(c.certFile, c.keyFile)
				if cerr != nil {
					continue
				}
				c.Lock()
				c.cert = &cert
				c.Unlock()
			}
		}
	}()
	return nil
}

// watch starts watching for changes to the certificate
// and key files. On any change the certificate and key
// are reloaded. If there is an issue the loading will fail
// and the old (if any) certificates and keys will continue
// to be used.
func (c *Certs) watch() (err error) {
	defer func() {
		if err != nil {
			// Stop any watches previously setup after an error.
			notify.Stop(c.e)
		}
	}()

	// Windows doesn't allow for watching file changes but instead allows
	// for directory changes only, while we can still watch for changes
	// on files on other platforms. Watch parent directory on all platforms
	// for simplicity.
	if err = notify.Watch(filepath.Dir(c.certFile), c.e, eventWrite...); err != nil {
		return err
	}
	if err = notify.Watch(filepath.Dir(c.keyFile), c.e, eventWrite...); err != nil {
		return err
	}
	cert, err := c.loadCert(c.certFile, c.keyFile)
	if err != nil {
		return err
	}
	c.Lock()
	c.cert = &cert
	c.Unlock()
	if err != nil {
		return err
	}
	go c.run()
	return nil
}

func (c *Certs) run() {
	for event := range c.e {
		base := filepath.Base(event.Path())
		if isWriteEvent(event.Event()) {
			certChanged := base == filepath.Base(c.certFile)
			keyChanged := base == filepath.Base(c.keyFile)
			if certChanged || keyChanged {
				cert, err := c.loadCert(c.certFile, c.keyFile)
				if err != nil {
					// ignore the error continue to use
					// old certificates.
					continue
				}
				c.Lock()
				c.cert = &cert
				c.Unlock()
			}
		}
	}
}

// GetCertificateFunc provides a GetCertificate type for custom client implementations.
type GetCertificateFunc func(hello *tls.ClientHelloInfo) (*tls.Certificate, error)

// GetCertificate returns the loaded certificate for use by
// the TLSConfig fields GetCertificate field in a http.Server.
func (c *Certs) GetCertificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	c.RLock()
	defer c.RUnlock()
	return c.cert, nil
}

// GetClientCertificate returns the loaded certificate for use by
// the TLSConfig fields GetClientCertificate field in a http.Server.
func (c *Certs) GetClientCertificate(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	c.RLock()
	defer c.RUnlock()
	return c.cert, nil
}

// Stop tells loader to stop watching for changes to the
// certificate and key files.
func (c *Certs) Stop() {
	if c != nil {
		notify.Stop(c.e)
	}
}
