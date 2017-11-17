// Copyright (C) 2017 Minio Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package sio implements the DARE format. It provides an API
// for secure en/decrypting IO operations.
package sio // import "github.com/minio/sio"

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
)

// Version10 specifies version 1.0
const Version10 byte = 0x10

const (
	// AES_256_GCM specifies the cipher suite AES-GCM with 256 bit keys.
	AES_256_GCM byte = iota
	// CHACHA20_POLY1305 specifies the cipher suite ChaCha20Poly1305 with 256 bit keys.
	CHACHA20_POLY1305
)

const (
	headerSize  = 16
	payloadSize = 64 * 1024
	tagSize     = 16
)

var (
	errMissingHeader      = errors.New("sio: incomplete header")
	errPayloadTooShort    = errors.New("sio: payload too short")
	errPackageOutOfOrder  = errors.New("sio: sequence number mismatch")
	errTagMismatch        = errors.New("sio: authentication failed")
	errUnsupportedVersion = errors.New("sio: unsupported version")
	errUnsupportedCipher  = errors.New("sio: unsupported cipher suite")
)

var newAesGcm = func(key []byte) (cipher.AEAD, error) {
	aes256, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(aes256)
}

var supportedCiphers = [...]func([]byte) (cipher.AEAD, error){
	AES_256_GCM:       newAesGcm,
	CHACHA20_POLY1305: chacha20poly1305.New,
}

// Encrypt reads from src until it encounters an io.EOF and encrypts all received
// data. The encrypted data is written to dst. It returns the number of bytes
// encrypted and the first error encountered while encrypting, if any.
//
// Encrypt returns the number of bytes written to dst.
func Encrypt(dst io.Writer, src io.Reader, config Config) (n int64, err error) {
	encReader, err := EncryptReader(src, config)
	if err != nil {
		return
	}
	return io.CopyBuffer(dst, encReader, make([]byte, payloadSize))
}

// Decrypt reads from src until it encounters an io.EOF and decrypts all received
// data. The decrypted data is written to dst. It returns the number of bytes
// decrypted and the first error encountered while decrypting, if any.
//
// Decrypt returns the number of bytes written to dst. Decrypt only writes data to
// dst if the data was decrypted successfully.
func Decrypt(dst io.Writer, src io.Reader, config Config) (n int64, err error) {
	decReader, err := DecryptReader(src, config)
	if err != nil {
		return
	}
	return io.CopyBuffer(dst, decReader, make([]byte, headerSize+payloadSize+tagSize))
}

// EncryptReader wraps the given src and returns an io.Reader which encrypts
// all received data. EncryptReader returns an error if the provided encryption
// configuration is invalid.
func EncryptReader(src io.Reader, config Config) (io.Reader, error) {
	if err := setConfigDefaults(&config); err != nil {
		return nil, err
	}
	cipher, err := supportedCiphers[config.CipherSuites[0]](config.Key)
	if err != nil {
		return nil, err
	}
	nonce, err := config.generateNonce()
	if err != nil {
		return nil, err
	}
	return &encryptedReader{
		src:            src,
		config:         config,
		nonce:          nonce,
		cipher:         cipher,
		sequenceNumber: config.SequenceNumber,
	}, nil
}

// DecryptReader wraps the given src and returns an io.Reader which decrypts
// all received data. DecryptReader returns an error if the provided decryption
// configuration is invalid.
func DecryptReader(src io.Reader, config Config) (io.Reader, error) {
	if err := setConfigDefaults(&config); err != nil {
		return nil, err
	}
	ciphers, err := config.createCiphers()
	if err != nil {
		return nil, err
	}
	return &decryptedReader{
		src:            src,
		config:         config,
		ciphers:        ciphers,
		sequenceNumber: config.SequenceNumber,
	}, nil
}

// EncryptWriter wraps the given dst and returns an io.WriteCloser which
// encrypts all data written to it. EncryptWriter returns an error if the
// provided decryption configuration is invalid.
//
// The returned io.WriteCloser must be closed successfully to finalize the
// encryption process.
func EncryptWriter(dst io.Writer, config Config) (io.WriteCloser, error) {
	if err := setConfigDefaults(&config); err != nil {
		return nil, err
	}
	cipher, err := supportedCiphers[config.CipherSuites[0]](config.Key)
	if err != nil {
		return nil, err
	}
	nonce, err := config.generateNonce()
	if err != nil {
		return nil, err
	}
	return &encryptedWriter{
		dst:            dst,
		config:         config,
		nonce:          nonce,
		cipher:         cipher,
		sequenceNumber: config.SequenceNumber,
	}, nil
}

// DecryptWriter wraps the given dst and returns an io.WriteCloser which
// decrypts all data written to it. DecryptWriter returns an error if the
// provided decryption configuration is invalid.
//
// The returned io.WriteCloser must be closed successfully to finalize the
// decryption process.
func DecryptWriter(dst io.Writer, config Config) (io.WriteCloser, error) {
	if err := setConfigDefaults(&config); err != nil {
		return nil, err
	}
	ciphers, err := config.createCiphers()
	if err != nil {
		return nil, err
	}
	return &decryptedWriter{
		dst:            dst,
		config:         config,
		ciphers:        ciphers,
		sequenceNumber: config.SequenceNumber,
	}, nil
}

func setConfigDefaults(config *Config) error {
	if config.MinVersion > Version10 {
		return errors.New("sio: unknown minimum version")
	}
	if config.MaxVersion > Version10 {
		return errors.New("dare: unknown maximum version")
	}
	if len(config.Key) != 32 {
		return errors.New("sio: invalid key size")
	}
	if len(config.CipherSuites) > 2 {
		return errors.New("sio: too many cipher suites")
	}
	for _, c := range config.CipherSuites {
		if int(c) >= len(supportedCiphers) {
			return errors.New("sio: unknown cipher suite")
		}
	}
	if config.MinVersion < Version10 {
		config.MinVersion = Version10
	}
	if config.MaxVersion < Version10 {
		config.MaxVersion = config.MinVersion
	}
	if len(config.CipherSuites) == 0 {
		config.CipherSuites = []byte{AES_256_GCM, CHACHA20_POLY1305}
	}
	if config.Rand == nil {
		config.Rand = rand.Reader
	}
	return nil
}

// Config contains the format configuration. The only field
// which must always be set manually is the secret key.
type Config struct {
	// The minimal supported version of the format. If
	// not set the default value is used.
	MinVersion byte
	// The highest supported version of the format. If
	// not set the default value is used.
	MaxVersion byte
	// A list of supported cipher suites. If not set the
	// default value is used.
	CipherSuites []byte
	// The secret encryption key. It must be 32 bytes long.
	Key []byte
	// The first expected sequence number. It should only
	// be set manually when appending data to an existing
	// sequence of packages or decrypting a range within
	// a sequence of packages.
	SequenceNumber uint32
	// The RNG used to generate random values. If not set
	// the default value (crypto/rand.Reader) is used.
	Rand io.Reader
}

func (c *Config) verifyHeader(header headerV10) error {
	if version := header.Version(); version < c.MinVersion || version > c.MaxVersion {
		return errUnsupportedVersion
	}
	if !c.isCipherSuiteSupported(header.Cipher()) {
		return errUnsupportedCipher
	}
	return nil
}

func (c *Config) createCiphers() ([]cipher.AEAD, error) {
	ciphers := make([]cipher.AEAD, len(supportedCiphers))
	for _, v := range c.CipherSuites {
		aeadCipher, err := supportedCiphers[v](c.Key)
		if err != nil {
			return nil, err
		}
		ciphers[v] = aeadCipher
	}
	return ciphers, nil
}

func (c *Config) generateNonce() (nonce [8]byte, err error) {
	_, err = io.ReadFull(c.Rand, nonce[:])
	return nonce, err
}

func (c *Config) isCipherSuiteSupported(suite byte) bool {
	for _, c := range c.CipherSuites {
		if suite == c {
			return true
		}
	}
	return false
}
