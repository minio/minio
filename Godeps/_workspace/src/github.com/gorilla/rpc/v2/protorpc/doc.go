// Copyright 2009 The Go Authors. All rights reserved.
// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package gorilla/rpc/protorpc provides a codec for ProtoRPC over HTTP services.

To register the codec in a RPC server:

	import (
		"http"
		"github.com/gorilla/rpc/v2"
		"github.com/gorilla/rpc/v2/protorpc"
	)

	func init() {
		s := rpc.NewServer()
		s.RegisterCodec(protorpc.NewCodec(), "application/json")
		// [...]
		http.Handle("/rpc", s)
	}

A codec is tied to a content type. In the example above, the server
will use the ProtoRPC codec for requests with "application/json" as
the value for the "Content-Type" header.

This package implement ProtoRPC, based on the JSON-RPC transport, it
differs in that it uses HTTP as its envelope.

Example:
POST /Service.Method
Request:
{
  "requestField1": "value1",
  "requestField2": "value2",
}
Response:
{
  "responseField1": "value1",
  "responseField2": "value2",
}

Check the gorilla/rpc documentation for more details:

	http://gorilla-web.appspot.com/pkg/rpc
*/
package protorpc
