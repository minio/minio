// Package sasl is an implementation detail of the mgo package.
//
// This package is not meant to be used by itself.
//

// +build !windows

package sasl

// #cgo LDFLAGS: -lsasl2
//
// struct sasl_conn {};
//
// #include <stdlib.h>
// #include <sasl/sasl.h>
//
// sasl_callback_t *mgo_sasl_callbacks(const char *username, const char *password);
//
import "C"

import (
	"fmt"
	"strings"
	"sync"
	"unsafe"
)

type saslStepper interface {
	Step(serverData []byte) (clientData []byte, done bool, err error)
	Close()
}

type saslSession struct {
	conn *C.sasl_conn_t
	step int
	mech string

	cstrings  []*C.char
	callbacks *C.sasl_callback_t
}

var initError error
var initOnce sync.Once

func initSASL() {
	rc := C.sasl_client_init(nil)
	if rc != C.SASL_OK {
		initError = saslError(rc, nil, "cannot initialize SASL library")
	}
}

func New(username, password, mechanism, service, host string) (saslStepper, error) {
	initOnce.Do(initSASL)
	if initError != nil {
		return nil, initError
	}

	ss := &saslSession{mech: mechanism}
	if service == "" {
		service = "mongodb"
	}
	if i := strings.Index(host, ":"); i >= 0 {
		host = host[:i]
	}
	ss.callbacks = C.mgo_sasl_callbacks(ss.cstr(username), ss.cstr(password))
	rc := C.sasl_client_new(ss.cstr(service), ss.cstr(host), nil, nil, ss.callbacks, 0, &ss.conn)
	if rc != C.SASL_OK {
		ss.Close()
		return nil, saslError(rc, nil, "cannot create new SASL client")
	}
	return ss, nil
}

func (ss *saslSession) cstr(s string) *C.char {
	cstr := C.CString(s)
	ss.cstrings = append(ss.cstrings, cstr)
	return cstr
}

func (ss *saslSession) Close() {
	for _, cstr := range ss.cstrings {
		C.free(unsafe.Pointer(cstr))
	}
	ss.cstrings = nil

	if ss.callbacks != nil {
		C.free(unsafe.Pointer(ss.callbacks))
	}

	// The documentation of SASL dispose makes it clear that this should only
	// be done when the connection is done, not when the authentication phase
	// is done, because an encryption layer may have been negotiated.
	// Even then, we'll do this for now, because it's simpler and prevents
	// keeping track of this state for every socket. If it breaks, we'll fix it.
	C.sasl_dispose(&ss.conn)
}

func (ss *saslSession) Step(serverData []byte) (clientData []byte, done bool, err error) {
	ss.step++
	if ss.step > 10 {
		return nil, false, fmt.Errorf("too many SASL steps without authentication")
	}
	var cclientData *C.char
	var cclientDataLen C.uint
	var rc C.int
	if ss.step == 1 {
		var mechanism *C.char // ignored - must match cred
		rc = C.sasl_client_start(ss.conn, ss.cstr(ss.mech), nil, &cclientData, &cclientDataLen, &mechanism)
	} else {
		var cserverData *C.char
		var cserverDataLen C.uint
		if len(serverData) > 0 {
			cserverData = (*C.char)(unsafe.Pointer(&serverData[0]))
			cserverDataLen = C.uint(len(serverData))
		}
		rc = C.sasl_client_step(ss.conn, cserverData, cserverDataLen, nil, &cclientData, &cclientDataLen)
	}
	if cclientData != nil && cclientDataLen > 0 {
		clientData = C.GoBytes(unsafe.Pointer(cclientData), C.int(cclientDataLen))
	}
	if rc == C.SASL_OK {
		return clientData, true, nil
	}
	if rc == C.SASL_CONTINUE {
		return clientData, false, nil
	}
	return nil, false, saslError(rc, ss.conn, "cannot establish SASL session")
}

func saslError(rc C.int, conn *C.sasl_conn_t, msg string) error {
	var detail string
	if conn == nil {
		detail = C.GoString(C.sasl_errstring(rc, nil, nil))
	} else {
		detail = C.GoString(C.sasl_errdetail(conn))
	}
	return fmt.Errorf(msg + ": " + detail)
}
