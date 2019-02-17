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
	"crypto/x509"
	"testing"
	"time"
)

func Test_NewClientOptions_default(t *testing.T) {
	o := NewClientOptions()

	if o.ClientID != "" {
		t.Fatalf("bad default client id")
	}

	if o.Username != "" {
		t.Fatalf("bad default username")
	}

	if o.Password != "" {
		t.Fatalf("bad default password")
	}

	if o.KeepAlive != 30 {
		t.Fatalf("bad default timeout")
	}
}

func Test_NewClientOptions_mix(t *testing.T) {
	o := NewClientOptions()
	o.AddBroker("tcp://192.168.1.2:9999")
	o.SetClientID("myclientid")
	o.SetUsername("myuser")
	o.SetPassword("mypassword")
	o.SetKeepAlive(88 * time.Second)

	if o.Servers[0].Scheme != "tcp" {
		t.Fatalf("bad scheme")
	}

	if o.Servers[0].Host != "192.168.1.2:9999" {
		t.Fatalf("bad host")
	}

	if o.ClientID != "myclientid" {
		t.Fatalf("bad set clientid")
	}

	if o.Username != "myuser" {
		t.Fatalf("bad set username")
	}

	if o.Password != "mypassword" {
		t.Fatalf("bad set password")
	}

	if o.KeepAlive != 88 {
		t.Fatalf("bad set timeout: %d", o.KeepAlive)
	}
}

func Test_ModifyOptions(t *testing.T) {
	o := NewClientOptions()
	o.AddBroker("tcp://3.3.3.3:12345")
	c := NewClient(o).(*client)
	o.AddBroker("ws://2.2.2.2:9999")
	o.SetOrderMatters(false)

	if c.options.Servers[0].Scheme != "tcp" {
		t.Fatalf("client options.server.Scheme was modified")
	}

	// if c.options.server.Host != "2.2.2.2:9999" {
	// 	t.Fatalf("client options.server.Host was modified")
	// }

	if o.Order != false {
		t.Fatalf("options.order was not modified")
	}
}

func Test_TLSConfig(t *testing.T) {
	o := NewClientOptions().SetTLSConfig(&tls.Config{
		RootCAs:            x509.NewCertPool(),
		ClientAuth:         tls.NoClientCert,
		ClientCAs:          x509.NewCertPool(),
		InsecureSkipVerify: true})

	c := NewClient(o).(*client)

	if c.options.TLSConfig.ClientAuth != tls.NoClientCert {
		t.Fatalf("client options.tlsConfig ClientAuth incorrect")
	}

	if c.options.TLSConfig.InsecureSkipVerify != true {
		t.Fatalf("client options.tlsConfig InsecureSkipVerify incorrect")
	}
}

func Test_OnConnectionLost(t *testing.T) {
	onconnlost := func(client Client, err error) {
		panic(err)
	}
	o := NewClientOptions().SetConnectionLostHandler(onconnlost)

	c := NewClient(o).(*client)

	if c.options.OnConnectionLost == nil {
		t.Fatalf("client options.onconnlost was nil")
	}
}
