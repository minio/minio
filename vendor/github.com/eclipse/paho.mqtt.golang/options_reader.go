/*
 * Copyright (c) 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 */

package mqtt

import (
	"crypto/tls"
	"net/url"
	"time"
)

// ClientOptionsReader provides an interface for reading ClientOptions after the client has been initialized.
type ClientOptionsReader struct {
	options *ClientOptions
}

func (r *ClientOptionsReader) Servers() []*url.URL {
	s := make([]*url.URL, len(r.options.Servers))

	for i, u := range r.options.Servers {
		nu := *u
		s[i] = &nu
	}

	return s
}

func (r *ClientOptionsReader) ClientID() string {
	s := r.options.ClientID
	return s
}

func (r *ClientOptionsReader) Username() string {
	s := r.options.Username
	return s
}

func (r *ClientOptionsReader) Password() string {
	s := r.options.Password
	return s
}

func (r *ClientOptionsReader) CleanSession() bool {
	s := r.options.CleanSession
	return s
}

func (r *ClientOptionsReader) Order() bool {
	s := r.options.Order
	return s
}

func (r *ClientOptionsReader) WillEnabled() bool {
	s := r.options.WillEnabled
	return s
}

func (r *ClientOptionsReader) WillTopic() string {
	s := r.options.WillTopic
	return s
}

func (r *ClientOptionsReader) WillPayload() []byte {
	s := r.options.WillPayload
	return s
}

func (r *ClientOptionsReader) WillQos() byte {
	s := r.options.WillQos
	return s
}

func (r *ClientOptionsReader) WillRetained() bool {
	s := r.options.WillRetained
	return s
}

func (r *ClientOptionsReader) ProtocolVersion() uint {
	s := r.options.ProtocolVersion
	return s
}

func (r *ClientOptionsReader) TLSConfig() tls.Config {
	s := r.options.TLSConfig
	return s
}

func (r *ClientOptionsReader) KeepAlive() time.Duration {
	s := r.options.KeepAlive
	return s
}

func (r *ClientOptionsReader) PingTimeout() time.Duration {
	s := r.options.PingTimeout
	return s
}

func (r *ClientOptionsReader) ConnectTimeout() time.Duration {
	s := r.options.ConnectTimeout
	return s
}

func (r *ClientOptionsReader) MaxReconnectInterval() time.Duration {
	s := r.options.MaxReconnectInterval
	return s
}

func (r *ClientOptionsReader) AutoReconnect() bool {
	s := r.options.AutoReconnect
	return s
}

func (r *ClientOptionsReader) WriteTimeout() time.Duration {
	s := r.options.WriteTimeout
	return s
}

func (r *ClientOptionsReader) MessageChannelDepth() uint {
	s := r.options.MessageChannelDepth
	return s
}
