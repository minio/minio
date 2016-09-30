# NATS - Go Client
A [Go](http://golang.org) client for the [NATS messaging system](https://nats.io).

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)
[![Go Report Card](https://goreportcard.com/badge/github.com/nats-io/nats)](https://goreportcard.com/report/github.com/nats-io/nats) [![Build Status](https://travis-ci.org/nats-io/nats.svg?branch=master)](http://travis-ci.org/nats-io/nats) [![GoDoc](http://godoc.org/github.com/nats-io/nats?status.png)](http://godoc.org/github.com/nats-io/nats) [![Coverage Status](https://coveralls.io/repos/nats-io/nats/badge.svg?branch=master)](https://coveralls.io/r/nats-io/nats?branch=master)

## Installation

```bash
# Go client
go get github.com/nats-io/nats

# Server
go get github.com/nats-io/gnatsd
```

## Basic Usage

```go

nc, _ := nats.Connect(nats.DefaultURL)

// Simple Publisher
nc.Publish("foo", []byte("Hello World"))

// Simple Async Subscriber
nc.Subscribe("foo", func(m *nats.Msg) {
    fmt.Printf("Received a message: %s\n", string(m.Data))
})

// Simple Sync Subscriber
sub, err := nc.SubscribeSync("foo")
m, err := sub.NextMsg(timeout)

// Channel Subscriber
ch := make(chan *nats.Msg, 64)
sub, err := nc.ChanSubscribe("foo", ch)
msg <- ch

// Unsubscribe
sub.Unsubscribe()

// Requests
msg, err := nc.Request("help", []byte("help me"), 10*time.Millisecond)

// Replies
nc.Subscribe("help", func(m *Msg) {
    nc.Publish(m.Reply, []byte("I can help!"))
})

// Close connection
nc := nats.Connect("nats://localhost:4222")
nc.Close();
```

## Encoded Connections

```go

nc, _ := nats.Connect(nats.DefaultURL)
c, _ := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
defer c.Close()

// Simple Publisher
c.Publish("foo", "Hello World")

// Simple Async Subscriber
c.Subscribe("foo", func(s string) {
    fmt.Printf("Received a message: %s\n", s)
})

// EncodedConn can Publish any raw Go type using the registered Encoder
type person struct {
     Name     string
     Address  string
     Age      int
}

// Go type Subscriber
c.Subscribe("hello", func(p *person) {
    fmt.Printf("Received a person: %+v\n", p)
})

me := &person{Name: "derek", Age: 22, Address: "140 New Montgomery Street, San Francisco, CA"}

// Go type Publisher
c.Publish("hello", me)

// Unsubscribe
sub, err := c.Subscribe("foo", nil)
...
sub.Unsubscribe()

// Requests
var response string
err := c.Request("help", "help me", &response, 10*time.Millisecond)
if err != nil {
    fmt.Printf("Request failed: %v\n", err)
}

// Replying
c.Subscribe("help", func(subj, reply string, msg string) {
    c.Publish(reply, "I can help!")
})

// Close connection
c.Close();
```

## TLS

```go
// tls as a scheme will enable secure connections by default. This will also verify the server name.
nc, err := nats.Connect("tls://nats.demo.io:4443")

// If you are using a self-signed certificate, you need to have a tls.Config with RootCAs setup.
// We provide a helper method to make this case easier.
nc, err = nats.Connect("tls://localhost:4443", nats.RootCAs("./configs/certs/ca.pem"))

// If the server requires client certificate, there is an helper function for that too:
cert := nats.ClientCert("./configs/certs/client-cert.pem", "./configs/certs/client-key.pem")
nc, err = nats.Connect("tls://localhost:4443", cert)

// You can also supply a complete tls.Config

certFile := "./configs/certs/client-cert.pem"
keyFile := "./configs/certs/client-key.pem"
cert, err := tls.LoadX509KeyPair(certFile, keyFile)
if err != nil {
    t.Fatalf("error parsing X509 certificate/key pair: %v", err)
}

config := &tls.Config{
    ServerName: 	opts.Host,
    Certificates: 	[]tls.Certificate{cert},
    RootCAs:    	pool,
    MinVersion: 	tls.VersionTLS12,
}

nc, err = nats.Connect("nats://localhost:4443", nats.Secure(config))
if err != nil {
	t.Fatalf("Got an error on Connect with Secure Options: %+v\n", err)
}

```

## Using Go Channels (netchan)

```go
nc, _ := nats.Connect(nats.DefaultURL)
ec, _ := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
defer ec.Close()

type person struct {
     Name     string
     Address  string
     Age      int
}

recvCh := make(chan *person)
ec.BindRecvChan("hello", recvCh)

sendCh := make(chan *person)
ec.BindSendChan("hello", sendCh)

me := &person{Name: "derek", Age: 22, Address: "140 New Montgomery Street"}

// Send via Go channels
sendCh <- me

// Receive via Go channels
who := <- recvCh
```

## Wildcard Subscriptions

```go

// "*" matches any token, at any level of the subject.
nc.Subscribe("foo.*.baz", func(m *Msg) {
    fmt.Printf("Msg received on [%s] : %s\n", m.Subject, string(m.Data));
})

nc.Subscribe("foo.bar.*", func(m *Msg) {
    fmt.Printf("Msg received on [%s] : %s\n", m.Subject, string(m.Data));
})

// ">" matches any length of the tail of a subject, and can only be the last token
// E.g. 'foo.>' will match 'foo.bar', 'foo.bar.baz', 'foo.foo.bar.bax.22'
nc.Subscribe("foo.>", func(m *Msg) {
    fmt.Printf("Msg received on [%s] : %s\n", m.Subject, string(m.Data));
})

// Matches all of the above
nc.Publish("foo.bar.baz", []byte("Hello World"))

```

## Queue Groups

```go
// All subscriptions with the same queue name will form a queue group.
// Each message will be delivered to only one subscriber per queue group,
// using queuing semantics. You can have as many queue groups as you wish.
// Normal subscribers will continue to work as expected.

nc.QueueSubscribe("foo", "job_workers", func(_ *Msg) {
  received += 1;
})

```

## Advanced Usage

```go

// Flush connection to server, returns when all messages have been processed.
nc.Flush()
fmt.Println("All clear!")

// FlushTimeout specifies a timeout value as well.
err := nc.FlushTimeout(1*time.Second)
if err != nil {
    fmt.Println("All clear!")
} else {
    fmt.Println("Flushed timed out!")
}

// Auto-unsubscribe after MAX_WANTED messages received
const MAX_WANTED = 10
sub, err := nc.Subscribe("foo")
sub.AutoUnsubscribe(MAX_WANTED)

// Multiple connections
nc1 := nats.Connect("nats://host1:4222")
nc2 := nats.Connect("nats://host2:4222")

nc1.Subscribe("foo", func(m *Msg) {
    fmt.Printf("Received a message: %s\n", string(m.Data))
})

nc2.Publish("foo", []byte("Hello World!"));

```

## Clustered Usage

```go

var servers = "nats://localhost:1222, nats://localhost:1223, nats://localhost:1224"

nc, err := nats.Connect(servers)

// Optionally set ReconnectWait and MaxReconnect attempts.
// This example means 10 seconds total per backend.
nc, err = nats.Connect(servers, nats.MaxReconnects(5), nats.ReconnectWait(2 * time.Second))

// Optionally disable randomization of the server pool
nc, err = nats.Connect(servers, nats.DontRandomize())

// Setup callbacks to be notified on disconnects, reconnects and connection closed.
nc, err = nats.Connect(servers,
	nats.DisconnectHandler(func(nc *nats.Conn) {
		fmt.Printf("Got disconnected!\n")
	}),
	nats.ReconnectHandler(func(_ *nats.Conn) {
		fmt.Printf("Got reconnected to %v!\n", nc.ConnectedUrl())
	}),
	nats.ClosedHandler(func(nc *nats.Conn) {
		fmt.Printf("Connection closed. Reason: %q\n", nc.LastError())
	})
)

// When connecting to a mesh of servers with auto-discovery capabilities,
// you may need to provide a username/password or token in order to connect
// to any server in that mesh when authentication is required.
// Instead of providing the credentials in the initial URL, you will use
// new option setters:
nc, err = nats.Connect("nats://localhost:4222", nats.UserInfo("foo", "bar"))

// For token based authentication:
nc, err = nats.Connect("nats://localhost:4222", nats.Token("S3cretT0ken"))

// You can even pass the two at the same time in case one of the server
// in the mesh requires token instead of user name and password.
nc, err = nats.Connect("nats://localhost:4222",
    nats.UserInfo("foo", "bar"),
    nats.Token("S3cretT0ken"))

// Note that if credentials are specified in the initial URLs, they take
// precedence on the credentials specfied through the options.
// For instance, in the connect call below, the client library will use
// the user "my" and password "pwd" to connect to locahost:4222, however,
// it will use username "foo" and password "bar" when (re)connecting to
// a different server URL that it got as part of the auto-discovery.
nc, err = nats.Connect("nats://my:pwd@localhost:4222", nats.UserInfo("foo", "bar"))

```

## License

(The MIT License)

Copyright (c) 2012-2016 Apcera Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
