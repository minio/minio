package httpserver

import "crypto/tls"

func getDefaultTLSConfig() *tls.Config {
	config := &tls.Config{}

	//Use only modern ciphers
	config.CipherSuites = []uint16{
		tls.TLS_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	}

	//Use only TLS v1.2
	config.MinVersion = tls.VersionTLS12

	// Ignore client auth for now
	config.ClientAuth = tls.NoClientCert

	//Don't allow session resumption
	config.SessionTicketsDisabled = true
	return config
}
