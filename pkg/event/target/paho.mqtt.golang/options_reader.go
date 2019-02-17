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
	"net/http"
	"net/url"
	"time"
)

// ClientOptionsReader provides an interface for reading ClientOptions after the client has been initialized.
type ClientOptionsReader struct {
	options *ClientOptions
}

//Servers returns a slice of the servers defined in the clientoptions
func (r *ClientOptionsReader) Servers() []*url.URL {
	s := make([]*url.URL, len(r.options.Servers))

	for i, u := range r.options.Servers {
		nu := *u
		s[i] = &nu
	}

	return s
}

//ResumeSubs returns true if resuming stored (un)sub is enabled
func (r *ClientOptionsReader) ResumeSubs() bool {
	s := r.options.ResumeSubs
	return s
}

//ClientID returns the set client id
func (r *ClientOptionsReader) ClientID() string {
	s := r.options.ClientID
	return s
}

//Username returns the set username
func (r *ClientOptionsReader) Username() string {
	s := r.options.Username
	return s
}

//Password returns the set password
func (r *ClientOptionsReader) Password() string {
	s := r.options.Password
	return s
}

//CleanSession returns whether Cleansession is set
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

func (r *ClientOptionsReader) TLSConfig() *tls.Config {
	s := r.options.TLSConfig
	return s
}

func (r *ClientOptionsReader) KeepAlive() time.Duration {
	s := time.Duration(r.options.KeepAlive * int64(time.Second))
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

func (r *ClientOptionsReader) HTTPHeaders() http.Header {
	h := r.options.HTTPHeaders
	return h
}
