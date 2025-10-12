# MinIO Grid

The MinIO Grid is a package that provides two-way communication between servers.
It uses a single two-way connection to send and receive messages between servers.

It includes built in muxing of concurrent requests as well as congestion handling for streams.

Requests can be "Single Payload" or "Streamed".

Use the MinIO Grid for:

* Small, frequent requests with low latency requirements.
* Long-running requests with small/medium payloads.

Do *not* use the MinIO Grid for:

* Large payloads.

Only a single connection is ever made between two servers.
Likely this means that this connection will not be able to saturate network bandwidth.
Therefore, using this for large payloads will likely be slower than using a separate connection,
and other connections will be blocked while the large payload is being sent.

## Handlers & Routes

Handlers have a predefined Handler ID.
In addition, there can be several *static* subroutes used to differentiate between different handlers of the same ID.
A subroute on a client must match a subroute on the server. So routes cannot be used for dynamic routing, unlike HTTP.

Handlers should remain backwards compatible. If a breaking API change is required, a new handler ID should be created.

## Setup & Configuration

A **Manager** is used to manage all incoming and outgoing connections to a server.

On startup all remote servers must be specified.
From that individual connections will be spawned to each remote server,
or incoming requests will be hooked up to the appropriate connection.

To get a connection to a specific server, use `Manager.Connection(host)` to get a connection to the specified host.
From this connection individual requests can be made.

Each handler, with optional subroutes can be registered with the manager using
`Manager.RegisterXHandler(handlerID, handler, subroutes...)`.

A `Handler()` function provides an HTTP handler, which should be hooked up to the appropriate route on the server.

On startup, the manager will start connecting to remotes and also starts listening for incoming connections.
Until a connection is established, all outgoing requests will return `ErrDisconnected`.

# Usage

## Single Payload Requests

Single payload requests are requests and responses that are sent in a single message.
In essence, they are `[]byte` -> `[]byte, error` functions.

It is not possible to return *both* an error and a response.

Handlers are registered on the manager using `(*Manager).RegisterSingleHandler(id HandlerID, h SingleHandlerFn, subroute ...string)`.

The server handler function has this signature: `type SingleHandlerFn func(payload []byte) ([]byte, *RemoteErr)`.

Sample handler:
```go
    handler :=  func(payload []byte) ([]byte, *grid.RemoteErr) {
        // Do something with payload
        return []byte("response"), nil
    }

    err := manager.RegisterSingleHandler(grid.HandlerDiskInfo, handler)
```

Sample call:
```go
    // Get a connection to the remote host
    conn := manager.Connection(host)

    payload := []byte("request")
    response, err := conn.SingleRequest(ctx, grid.HandlerDiskInfo, payload)
```

If the error type is `*RemoteErr`, then the error was returned by the remote server. Otherwise it is a local error.

Context timeouts are propagated, and a default timeout of 1 minute is added if none is specified.

There is no cancellation propagation for single payload requests.
When the context is canceled, the request will return at once with an appropriate error.
However, the remote call will not see the cancellation - as can be seen from the 'missing' context on the handler.
The result will be discarded.

### Typed handlers

Typed handlers are handlers that have a specific type for the request and response payloads.
These must provide `msgp` serialization and deserialization.

In the examples we use a `MSS` type, which is a `map[string]string` that is `msgp` serializable.

```go
    handler := func(request *grid.MSS) (*grid.MSS, *grid.RemoteErr) {
        fmt.Println("Got request with field", request["myfield"])
        // Do something with payload
        return NewMSSWith(map[string]string{"result": "ok"}), nil
    }

    // Create a typed handler.
    // Due to current generics limitations, a constructor of the empty type must be provided.
    instance := grid.NewSingleHandler[*grid.MSS, *grid.MSS](h, grid.NewMSS, grid.NewMSS)

    // Register the handler on the manager
    instance.Register(manager, handler)

    // The typed instance is also used for calls
    conn := manager.Connection("host")
    resp, err := instance.Call(ctx, conn, grid.NewMSSWith(map[string]string{"myfield": "myvalue"}))
    if err == nil {
        fmt.Println("Got response with field", resp["result"])
    }
```

The wrapper will handle all serialization and de-serialization of the request and response,
and furthermore provides reuse of the structs used for the request and response.

Note that Responses sent for serialization are automatically reused for similar requests.
If the response contains shared data it will cause issues, since each unique response is reused.
To disable this behavior, use `(SingleHandler).WithSharedResponse()` to disable it.

## Streaming Requests

Streams consists of an initial request with payload and allows for full two-way communication between the client and server.

The handler function has this signature.

Sample handler:
```go
    handler :=  func(ctx context.Context, payload []byte, in <-chan []byte, out chan<- []byte) *RemoteErr {
        fmt.Println("Got request with initial payload", p, "from", GetCaller(ctx context.Context))
        fmt.Println("Subroute:", GetSubroute(ctx))
        for {
            select {
            case <-ctx.Done():
                return nil
            case req, ok := <-in:
                if !ok {
                    break
                }
                // Do something with payload
                out <- []byte("response")

                // Return the request for reuse
                grid.PutByteBuffer(req)
            }
        }
        // out is closed by the caller and should never be closed by the handler.
        return nil
    }

    err := manager.RegisterStreamingHandler(grid.HandlerDiskInfo, StreamHandler{
        Handle: handler,
        Subroute: "asubroute",
        OutCapacity: 1,
        InCapacity: 1,
    })
```

Sample call:
```go
    // Get a connection to the remote host
    conn := manager.Connection(host).Subroute("asubroute")

    payload := []byte("request")
    stream, err := conn.NewStream(ctx, grid.HandlerDiskInfo, payload)
	if err != nil {
        return err
    }
    // Read results from the stream
    err = stream.Results(func(result []byte) error {
        fmt.Println("Got result", string(result))

        // Return the response for reuse
        grid.PutByteBuffer(result)
        return nil
    })
```

Context cancellation and timeouts are propagated to the handler.
The client does not wait for the remote handler to finish before returning.
Returning any error will also cancel the stream remotely.

CAREFUL: When utilizing two-way communication, it is important to ensure that the remote handler is not blocked on a send.
If the remote handler is blocked on a send, and the client is trying to send without the remote receiving,
the operation would become deadlocked if the channels are full.

### Typed handlers

Typed handlers are handlers that have a specific type for the request and response payloads.

```go
    // Create a typed handler.
    handler := func(ctx context.Context, p *Payload, in <-chan *Req, out chan<- *Resp) *RemoteErr {
        fmt.Println("Got request with initial payload", p, "from", GetCaller(ctx context.Context))
		fmt.Println("Subroute:", GetSubroute(ctx))
        for {
            select {
            case <-ctx.Done():
                return nil
            case req, ok := <-in:
                if !ok {
                    break
                }
                fmt.Println("Got request", in)
                // Do something with payload
                out <- Resp{"response"}
            }
            // out is closed by the caller and should never be closed by the handler.
            return nil
    }

    // Create a typed handler.
    // Due to current generics limitations, a constructor of the empty type must be provided.
    instance := grid.NewStream[*Payload, *Req, *Resp](h, newPayload, newReq, newResp)

    // Tweakable options
    instance.WithPayload = true // default true when newPayload != nil
    instance.OutCapacity = 1    // default
    instance.InCapacity = 1     // default true when newReq != nil

    // Register the handler on the manager
    instance.Register(manager, handler, "asubroute")

    // The typed instance is also used for calls
    conn := manager.Connection("host").Subroute("asubroute")
    stream, err := instance.Call(ctx, conn, &Payload{"request payload"})
    if err != nil { ... }

    // Read results from the stream
    err = stream.Results(func(resp *Resp) error {
        fmt.Println("Got result", resp)
        // Return the response for reuse
		instance.PutResponse(resp)
        return nil
    })
```

There are handlers for requests with:
 * No input stream: `RegisterNoInput`.
 * No initial payload: `RegisterNoPayload`.

Note that Responses sent for serialization are automatically reused for similar requests.
If the response contains shared data it will cause issues, since each unique response is reused.
To disable this behavior, use `(StreamTypeHandler).WithSharedResponse()` to disable it.
